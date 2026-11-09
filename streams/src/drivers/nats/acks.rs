//! JetStream publishes with their PubAcks collected on one subscription.
//!
//! async-nats' [`send_publish`](async_nats::jetstream::Context::send_publish)
//! makes each publish a request of its own: a fresh inbox from a global id
//! generator behind a mutex, a permit from one semaphore every publisher on
//! the connection shares, and two timers on the runtime's timer wheel. With
//! many publishers on a multi-threaded runtime those three locks, not the
//! network, set the rate. This does what nats.go's `PublishAsync` does
//! instead: one wildcard reply subscription per provider, a counter for the
//! reply tokens, and a single task that routes each PubAck to its waiter and
//! fails the ones that wait too long.
//!
//! On the wire nothing changes: the same subject, the same `Nats-Msg-Id`
//! header beside the message's own, the same frame, and a reply subject under the client's inbox
//! prefix, so the user permissions a JetStream publisher already needs
//! (`_INBOX.>`) still cover it.

use std::{
    collections::HashMap,
    pin::pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::Poll,
    time::{Duration, Instant},
};

use async_nats::{Client, HeaderMap, StatusCode, Subject, header::NATS_MESSAGE_ID};
use bytes::Bytes;
use futures::StreamExt;
use serde::{Deserialize, de::IgnoredAny};
use tokio::{
    sync::{Semaphore, oneshot},
    task::AbortHandle,
};
use tracing::warn;

/// How long a publish waits for its PubAck: async-nats' JetStream default.
pub(super) const ACK_TIMEOUT: Duration = Duration::from_secs(5);

/// Shards of the pending map, so publishers on different threads rarely
/// meet on one lock.
const SHARDS: usize = 16;

/// How many publishes may await their PubAcks through one [`Acks`] at once,
/// nats.go's `PublishAsyncMaxPending` default. Above roughly ten thousand a
/// stream's inbound queue overflows and the server answers 429.
///
/// Like nats.go's, the limit belongs to the publishing context (here, one
/// provider), not to the connection: the server sets no per-connection
/// limit, and its 429 is per stream, so two contexts on one connection are
/// no worse off than two on two connections.
pub(super) const MAX_PENDING: usize = 4000;

/// How many permits under [`MAX_PENDING`] a batch takes or gives back at a
/// time, so that publishers on different threads meet on the semaphore once
/// per chunk rather than once per message.
pub(super) const CHUNK: usize = 64;

/// How often waits past [`ACK_TIMEOUT`] are failed.
const SWEEP: Duration = Duration::from_millis(250);

/// Why a publish was not stored.
#[derive(Debug)]
pub(super) enum AckError {
    /// It could not be handed to the connection.
    Send(String),
    /// The server answered, with an error.
    Refused(String),
    /// Nothing answered in time, or the connection's reply subscription ended.
    TimedOut,
    /// Nothing listens on the subject: no stream captures it.
    NoStream,
}

impl std::fmt::Display for AckError {
    /// Keeps the server's or client's wording, so a caller can match on what
    /// NATS itself would have said.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AckError::Send(e) => write!(f, "{e}"),
            AckError::Refused(e) => write!(f, "{e}"),
            AckError::TimedOut => write!(f, "timed out: didn't receive ack in time"),
            AckError::NoStream => write!(f, "no stream found for given subject"),
        }
    }
}

type Outcome = Result<(), AckError>;

/// A publish's wait for its PubAck.
pub(super) type Pending = oneshot::Receiver<Outcome>;

struct Waiter {
    tx: oneshot::Sender<Outcome>,
    sent: Instant,
}

struct Inner {
    client: Client,
    /// `<inbox>.`; a reply subject is this plus a token.
    prefix: String,
    next: AtomicU64,
    waiting: [Mutex<HashMap<u64, Waiter>>; SHARDS],
    /// Set once the reply subscription has ended. Nothing routes or expires a
    /// wait after that, so a send that registers one too late would wait
    /// forever; `send` checks this after registering.
    closed: AtomicBool,
}

impl Inner {
    /// The shard holding `token`'s wait. Splitting the map keeps publishers and
    /// the reply router from serializing on one lock.
    fn shard(&self, token: u64) -> &Mutex<HashMap<u64, Waiter>> {
        &self.waiting[token as usize % SHARDS]
    }

    /// Hands `outcome` to whoever waits on `token`. A token with no wait (it
    /// already timed out, or its sender gave up) is dropped silently.
    fn settle(&self, token: u64, outcome: Outcome) {
        let waiter = self.shard(token).lock().expect("ack map").remove(&token);
        if let Some(waiter) = waiter {
            let _ = waiter.tx.send(outcome);
        }
    }

    /// Fails every wait older than `ACK_TIMEOUT`, or every wait at all.
    fn expire(&self, all: bool) {
        let now = Instant::now();
        for shard in &self.waiting {
            let expired: Vec<Waiter> = {
                let mut map = shard.lock().expect("ack map");
                if map.is_empty() {
                    continue;
                }
                let tokens: Vec<u64> = map
                    .iter()
                    .filter(|(_, w)| all || now.duration_since(w.sent) >= ACK_TIMEOUT)
                    .map(|(t, _)| *t)
                    .collect();
                tokens.iter().filter_map(|t| map.remove(t)).collect()
            };
            for waiter in expired {
                let _ = waiter.tx.send(Err(AckError::TimedOut));
            }
        }
    }
}

/// The publishing half of one connection's JetStream traffic.
pub(super) struct Acks {
    inner: Arc<Inner>,
    task: AbortHandle,
    /// Permits for [`MAX_PENDING`]. They are taken and returned by count
    /// through [`Credit`], never held as guard objects. Shared so that the
    /// permits of an abandoned [`Credit`] can come back after it is gone.
    budget: Arc<Semaphore>,
}

impl Drop for Acks {
    /// Stops routing replies and fails every outstanding wait at once, so no
    /// publish is left waiting on a reply subscription that no longer exists.
    fn drop(&mut self) {
        self.task.abort();
        self.inner.closed.store(true, Ordering::SeqCst);
        self.inner.expire(true);
    }
}

impl Acks {
    /// Subscribes to a fresh inbox on `client` and starts routing its replies.
    pub(super) async fn start(client: Client) -> Result<Self, async_nats::SubscribeError> {
        let inbox = client.new_inbox();
        let mut replies = client.subscribe(format!("{inbox}.*")).await?;
        let inner = Arc::new(Inner {
            prefix: format!("{inbox}."),
            client,
            next: AtomicU64::new(1),
            waiting: std::array::from_fn(|_| Mutex::new(HashMap::new())),
            closed: AtomicBool::new(false),
        });
        let routing = inner.clone();
        let task = tokio::spawn(async move {
            let mut sweep = tokio::time::interval(SWEEP);
            sweep.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    reply = replies.next() => match reply {
                        Some(reply) => route(&routing, reply),
                        None => break,
                    },
                    _ = sweep.tick() => routing.expire(false),
                }
            }
            // The subscription is gone for good; nothing else will answer.
            warn!("the JetStream ack subscription ended");
            // Closed first, then the stragglers: a send that registers after
            // this sweep sees the flag, and one that registered before it is
            // failed by the sweep, so no wait is left with nobody to end it.
            routing.closed.store(true, Ordering::SeqCst);
            routing.expire(true);
        })
        .abort_handle();
        Ok(Self {
            inner,
            task,
            budget: Arc::new(Semaphore::new(MAX_PENDING)),
        })
    }

    /// Returns `permits` once every wait registered by now has ended: the
    /// sweep fails a wait `ACK_TIMEOUT` after it was sent, checking every
    /// `SWEEP`, so none outlives that by more.
    fn release_later(&self, permits: usize) {
        let budget = self.budget.clone();
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move {
                    tokio::time::sleep(ACK_TIMEOUT + SWEEP).await;
                    budget.add_permits(permits);
                });
            }
            // No runtime is left to run the sweep either, so nothing is
            // waiting any more.
            Err(_) => budget.add_permits(permits),
        }
    }

    /// Sends `payload` to `subject` under `id`, with `headers` beside
    /// `Nats-Msg-Id`, returning once it is on its way. The receiver resolves
    /// when the server has stored it, or refused, or `ACK_TIMEOUT` has passed.
    ///
    /// Handing the frame to the connection waits only while the client's
    /// outbound queue is full, as it is while the connection is down. That
    /// wait counts against the same `ACK_TIMEOUT`: past it the send gives up
    /// with [`AckError::TimedOut`] rather than holding its permit for as long
    /// as the outage lasts. Giving up drops the client's enqueue, which is
    /// cancel safe, so the frame is either queued whole or not at all. The
    /// timer is armed only once the queue turns out to be full, so the usual
    /// send, which enqueues at once, costs none.
    pub(super) async fn send(
        &self,
        subject: Subject,
        id: &str,
        headers: Option<&HeaderMap>,
        payload: Bytes,
    ) -> Result<Pending, AckError> {
        let token = self.inner.next.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        let sent = Instant::now();
        self.inner
            .shard(token)
            .lock()
            .expect("ack map")
            .insert(token, Waiter { tx, sent });
        if self.inner.closed.load(Ordering::SeqCst) {
            self.inner
                .shard(token)
                .lock()
                .expect("ack map")
                .remove(&token);
            return Err(AckError::Send(
                "the connection's JetStream ack subscription has ended".into(),
            ));
        }
        let mut headers = headers.cloned().unwrap_or_default();
        headers.insert(NATS_MESSAGE_ID, id);
        let reply = format!("{}{token}", self.inner.prefix);
        let mut publish = pin!(
            self.inner
                .client
                .publish_with_reply_and_headers(subject, reply, headers, payload)
        );
        let outcome = match futures::poll!(publish.as_mut()) {
            Poll::Ready(sent) => sent.map_err(|error| AckError::Send(error.to_string())),
            Poll::Pending => {
                let deadline = tokio::time::Instant::from_std(sent + ACK_TIMEOUT);
                match tokio::time::timeout_at(deadline, publish).await {
                    Ok(sent) => sent.map_err(|error| AckError::Send(error.to_string())),
                    Err(_) => Err(AckError::TimedOut),
                }
            }
        };
        if let Err(error) = outcome {
            self.inner
                .shard(token)
                .lock()
                .expect("ack map")
                .remove(&token);
            return Err(error);
        }
        Ok(rx)
    }
}

/// Permits under [`MAX_PENDING`] one caller holds, returned when it drops.
///
/// `spare` are held but not yet spent on a send; the rest are spent on sends
/// still awaiting acks. Finished sends are returned in chunks. A caller calls
/// [`finish`](Self::finish) for every send it spent a permit on once that
/// send's wait is over, so that a credit dropped with spent permits left
/// unfinished is one whose caller was cancelled mid-wait: those permits stay
/// taken until the waits they cover can no longer be open, so cancelling
/// never lets more than [`MAX_PENDING`] waits stand at once.
pub(super) struct Credit<'a> {
    acks: &'a Acks,
    held: usize,
    spare: usize,
    finished: usize,
}

impl<'a> Credit<'a> {
    /// A credit holding no permits; the first send takes some.
    pub(super) fn new(acks: &'a Acks) -> Self {
        Self {
            acks,
            held: 0,
            spare: 0,
            finished: 0,
        }
    }

    /// Takes up to `want` permits without waiting.
    pub(super) fn try_take(&mut self, want: usize) -> bool {
        let want = want.clamp(1, CHUNK);
        match self.acks.budget.try_acquire_many(want as u32) {
            Ok(permits) => {
                permits.forget();
                self.held += want;
                self.spare += want;
                true
            }
            Err(_) => false,
        }
    }

    /// Takes up to `want` permits, waiting for them. Whatever finished is
    /// returned first, so a caller never waits on permits it holds itself.
    pub(super) async fn take(&mut self, want: usize) {
        self.flush();
        let want = want.clamp(1, CHUNK);
        if let Ok(permits) = self.acks.budget.acquire_many(want as u32).await {
            permits.forget();
            self.held += want;
            self.spare += want;
        }
    }

    /// Whether a send may go out now.
    pub(super) fn has_spare(&self) -> bool {
        self.spare > 0
    }

    /// Spends one spare permit on a send.
    pub(super) fn spend(&mut self) {
        self.spare -= 1;
    }

    /// A send's wait is over, or it never went out: its permit is returned.
    pub(super) fn finish(&mut self) {
        self.finished += 1;
        if self.finished >= CHUNK {
            self.flush();
        }
    }

    /// Returns the permits of finished sends to the shared budget in one call.
    fn flush(&mut self) {
        if self.finished > 0 {
            self.held -= self.finished;
            self.acks.budget.add_permits(self.finished);
            self.finished = 0;
        }
    }
}

impl Drop for Credit<'_> {
    /// Returns what is free now; permits still covering an open wait are
    /// released only once that wait can no longer be outstanding.
    fn drop(&mut self) {
        let free = self.spare + self.finished;
        if free > 0 {
            self.acks.budget.add_permits(free);
        }
        let waiting = self.held - free;
        if waiting > 0 {
            self.acks.release_later(waiting);
        }
    }
}

/// Resolves a pending publish's receiver into its outcome.
pub(super) async fn stored(pending: Pending) -> Outcome {
    pending.await.unwrap_or(Err(AckError::TimedOut))
}

/// What a PubAck says, as far as the publisher cares.
#[derive(Deserialize)]
struct PubAck {
    error: Option<async_nats::jetstream::Error>,
    stream: Option<IgnoredAny>,
    seq: Option<IgnoredAny>,
}

/// Settles the wait a reply belongs to, found by the token after the inbox
/// prefix. A reply that carries no token of ours is ignored.
fn route(inner: &Inner, reply: async_nats::Message) {
    let Some(token) = reply
        .subject
        .as_str()
        .strip_prefix(inner.prefix.as_str())
        .and_then(|t| t.parse::<u64>().ok())
    else {
        return;
    };
    inner.settle(token, outcome(&reply));
}

/// Reads a PubAck reply. "No responders" means no stream captures the
/// subject; a reply with neither an error nor a stream and sequence is not a
/// PubAck, and is treated as a refusal rather than as stored.
fn outcome(reply: &async_nats::Message) -> Outcome {
    if reply.status == Some(StatusCode::NO_RESPONDERS) {
        return Err(AckError::NoStream);
    }
    match serde_json::from_slice::<PubAck>(&reply.payload) {
        Ok(PubAck { error: Some(e), .. }) => Err(AckError::Refused(e.to_string())),
        Ok(PubAck {
            stream: Some(_),
            seq: Some(_),
            ..
        }) => Ok(()),
        Ok(_) => Err(AckError::Refused("the reply is not a PubAck".into())),
        Err(e) => Err(AckError::Refused(format!("unreadable PubAck: {e}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A reply on a token-bearing inbox subject, as the server would send.
    fn reply(payload: &str, status: Option<StatusCode>) -> async_nats::Message {
        async_nats::Message {
            subject: "_INBOX.x.1".into(),
            reply: None,
            payload: Bytes::copy_from_slice(payload.as_bytes()),
            headers: None,
            status,
            description: None,
            length: 0,
        }
    }

    #[test]
    fn reads_a_pub_ack() {
        assert!(outcome(&reply(r#"{"stream":"S","seq":4}"#, None)).is_ok());
        assert!(outcome(&reply(r#"{"stream":"S","seq":4,"duplicate":true}"#, None)).is_ok());
        let refused = outcome(&reply(
            r#"{"error":{"code":503,"err_code":10077,"description":"maximum messages exceeded"}}"#,
            None,
        ));
        assert!(
            matches!(&refused, Err(AckError::Refused(e)) if e.contains("maximum messages")),
            "{refused:?}"
        );
        assert!(matches!(
            outcome(&reply("", Some(StatusCode::NO_RESPONDERS))),
            Err(AckError::NoStream)
        ));
        assert!(outcome(&reply("hello", None)).is_err());
        assert!(outcome(&reply("{}", None)).is_err());
    }

    /// A client whose connection never comes up, so nothing drains its
    /// outbound queue of `capacity` commands.
    async fn unconnected(capacity: usize) -> Client {
        async_nats::ConnectOptions::new()
            .retry_on_initial_connect()
            .client_capacity(capacity)
            .connect("127.0.0.1:1")
            .await
            .unwrap()
    }

    /// How many publishes are waiting on an ack, across every shard.
    fn waits(acks: &Acks) -> usize {
        acks.inner
            .waiting
            .iter()
            .map(|shard| shard.lock().unwrap().len())
            .sum()
    }

    #[tokio::test(start_paused = true)]
    async fn a_send_stuck_on_a_full_queue_gives_up_at_the_ack_deadline() {
        // The reply subscription takes the queue's only slot.
        let acks = Acks::start(unconnected(1).await).await.unwrap();
        let start = tokio::time::Instant::now();
        let error = acks
            .send("s".into(), "id", None, Bytes::from_static(b"x"))
            .await
            .expect_err("the queue is full");
        assert!(matches!(error, AckError::TimedOut), "{error}");
        assert!(start.elapsed() >= ACK_TIMEOUT);
        assert_eq!(waits(&acks), 0, "the wait is cleaned up");
    }

    #[tokio::test(start_paused = true)]
    async fn an_abandoned_credit_keeps_its_open_waits_until_they_expire() {
        let acks = Acks::start(unconnected(64).await).await.unwrap();
        let mut credit = Credit::new(&acks);
        credit.take(4).await;
        credit.spend();
        credit.spend();
        credit.finish();
        drop(credit);
        assert_eq!(
            acks.budget.available_permits(),
            MAX_PENDING - 1,
            "the spare and finished permits come back at once, the open wait's does not"
        );
        tokio::time::sleep(ACK_TIMEOUT).await;
        assert_eq!(acks.budget.available_permits(), MAX_PENDING - 1);
        tokio::time::sleep(SWEEP + Duration::from_millis(1)).await;
        assert_eq!(acks.budget.available_permits(), MAX_PENDING);

        let mut credit = Credit::new(&acks);
        credit.take(2).await;
        credit.spend();
        credit.finish();
        drop(credit);
        assert_eq!(acks.budget.available_permits(), MAX_PENDING);
    }
}
