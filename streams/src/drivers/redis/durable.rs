//! Durable delivery over Redis Streams: a log per subject, a consumer group per
//! consumer name, and a pending list that holds each message until it is
//! acknowledged.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::StreamExt;
use redis::{
    aio::MultiplexedConnection,
    streams::{
        StreamAutoClaimReply, StreamId, StreamPendingCountReply, StreamPendingReply,
        StreamRangeReply, StreamReadReply,
    },
};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    Acker, Batch, Deliveries, Delivery, Durable, Headers, Manager, Message, Options, Payload,
    Position, Positioned, Publisher, Registry, Result, Stream, StreamError, Subscriber,
    Subscription,
    provider::{batch_result, check_batch, new_id, outgoing, pack, prefetch, publish_id, unpack},
};

use super::{
    Inner, Replay, fail,
    keys::{HEADER_FIELD_PREFIX, PAYLOAD_FIELD},
    pubsub::{capacity, refuse_group},
    refuse_unhonoured,
};

/// How many entries one read asks for: one round trip per batch rather than
/// per message. A durable consumer asks for its prefetch instead, within
/// [`MAX_READ_BATCH`].
const READ_BATCH: usize = 64;

/// The most entries one group read or reclaim asks for, however large the
/// prefetch: a reply this size is already a few hundred kilobytes, and a
/// bigger one only delays the first message of the batch.
const MAX_READ_BATCH: usize = 1024;

/// The most entry ids one XACK carries when acknowledgements arrive faster
/// than they can be sent one by one.
const MAX_ACK_BATCH: usize = 1024;

/// The longest a consumer goes between looks for abandoned messages while new
/// ones keep arriving. Checking before every read cost a round trip per batch
/// on the reader's connection, the one that bounds a consumer's throughput.
const RECLAIM_EVERY_UP_TO: Duration = Duration::from_secs(1);

/// How often a consumer records its pending count.
const MEASURE_LAG_EVERY: Duration = Duration::from_secs(1);

/// How long a read waits at the server before coming back empty. It bounds how
/// long a loop goes without checking for work to reclaim, so it is short
/// enough to feel responsive and long enough not to poll in a hot loop.
pub(super) const BLOCK_FOR: Duration = Duration::from_secs(2);
/// How long a consumer waits after a read that came back nearly empty, so the
/// next read collects a batch. Without it, every blocked reader is woken for
/// nearly every entry at moderate rates, and Redis spends its single thread
/// answering one-entry reads: on TLS, two consumers per subject capped a paced
/// 50k/s run at about 42k/s, against 50k/s with the pause. It costs at most
/// this much latency, and only when traffic is light enough to afford it.
const COALESCE_FOR: Duration = Duration::from_millis(1);

/// How long a stopping consumer may spend removing itself from its group.
const CLEANUP_FOR: Duration = Duration::from_secs(5);

/// The first and the longest wait between attempts to reopen a consumer's
/// connection after the server went away.
const RECONNECT_FROM: Duration = Duration::from_millis(100);
const RECONNECT_UP_TO: Duration = Duration::from_secs(2);

/// Publishes to and consumes from Redis streams, one per subject.
///
/// Where [`PubSubManager`](super::PubSubManager) hands a message to whoever is
/// listening at that moment, this appends it to a log. The log is what makes
/// the rest possible: a consumer group's position lives on the server, so it
/// outlives the process reading it, and a delivered message stays pending
/// until it is acknowledged. It offers [`Durable`], [`Positioned`], and
/// [`Batch`].
pub struct DurableManager {
    inner: Arc<Inner>,
    stream: Stream,
}

impl DurableManager {
    /// A manager over `stream`; nothing is read or created until it is used.
    pub(super) fn new(inner: Arc<Inner>, stream: Stream) -> Self {
        Self { inner, stream }
    }

    /// The Redis stream key that holds `subject`'s log.
    fn key(&self, subject: &str) -> String {
        self.inner.keys.subject(&self.stream.id, subject)
    }

    /// A Redis stream delivers when it is read, not on a timer. Saying so
    /// beats appending now and letting the caller believe it was scheduled.
    fn refuse_ttl(opts: &Options) -> Result<()> {
        if opts.delay().is_some() {
            return Err(StreamError::Unsupported(
                "a durable stream delivers when it is read, not on a timer; \
                 use RedisProvider::scheduled for a TTL"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Queues one XADD of `frame` and `headers` onto `pipe`, trimmed when a
    /// cap is set.
    fn queue_append(
        &self,
        pipe: &mut redis::Pipeline,
        key: &str,
        frame: Vec<u8>,
        headers: &Headers,
    ) {
        let cmd = pipe.cmd("XADD").arg(key);
        if let Some(max) = self.inner.max_len {
            // Approximate: Redis trims on node boundaries, which is what lets
            // it trim without walking the stream.
            cmd.arg("MAXLEN").arg("~").arg(max);
        }
        cmd.arg("*").arg(PAYLOAD_FIELD).arg(frame);
        for (name, value) in headers.iter() {
            cmd.arg(header_field(name)).arg(value);
        }
        cmd.ignore();
    }
}

#[async_trait::async_trait]
impl Publisher for DurableManager {
    /// An XADD that is never retried, because a retry would append a second
    /// copy under a new entry id. Refuses a TTL: the log has no timer.
    async fn publish(&self, subject: &str, payload: Payload, opts: &Options) -> Result<String> {
        self.inner.declares(&self.stream, subject)?;
        Self::refuse_ttl(opts)?;
        refuse_unhonoured(opts)?;
        let headers = outgoing(&self.stream, opts)?;
        let id = publish_id(opts);
        let mut pipe = redis::pipe();
        self.queue_append(&mut pipe, &self.key(subject), pack(&id, &payload), &headers);
        // Once: XADD * mints a new entry id each time, so a repeat appends the
        // message twice.
        self.inner
            .query_pipe::<()>(&pipe, Replay::Once)
            .await
            .map_err(fail(format!("redis: cannot append to {subject:?}")))?;
        tracing::debug!(subject, id, "appended");
        Ok(id)
    }
}

/// Appends the whole batch in one pipeline, so a thousand payloads are one
/// round trip, and the ids come back in order because they are minted first.
#[async_trait::async_trait]
impl Batch for DurableManager {
    /// One pipeline for every XADD. A failure is reported against every index,
    /// since Redis does not say how many entries landed before it.
    async fn publish_batch(
        &self,
        subject: &str,
        payloads: Vec<Payload>,
        opts: &Options,
    ) -> Result<Vec<String>> {
        check_batch(opts)?;
        self.inner.declares(&self.stream, subject)?;
        Self::refuse_ttl(opts)?;
        refuse_unhonoured(opts)?;
        let headers = outgoing(&self.stream, opts)?;
        let ids: Vec<String> = payloads.iter().map(|_| new_id()).collect();
        if ids.is_empty() {
            return Ok(ids);
        }
        let key = self.key(subject);
        let mut pipe = redis::pipe();
        for (id, payload) in ids.iter().zip(&payloads) {
            self.queue_append(&mut pipe, &key, pack(id, payload), &headers);
        }
        let sent = self
            .inner
            .query_pipe::<()>(&pipe, Replay::Once)
            .await
            .map_err(fail(format!("redis: cannot append a batch to {subject:?}")));
        // One failure for the lot; some entries may still have landed.
        let failures = match sent {
            Ok(()) => Vec::new(),
            Err(error) => (0..ids.len()).map(|index| (index, error.clone())).collect(),
        };
        batch_result(subject, ids, failures)
    }
}

/// The undurable half of the contract, which on this provider is a tail of the
/// same log the durable half reads: nothing is acknowledged, nothing is
/// remembered, and a message read from the stream is gone. Reach for
/// [`Durable::consume`] when that matters.
#[async_trait::async_trait]
impl Subscriber for DurableManager {
    /// Reads with XREAD from the newest entry at the time of the call. Refuses
    /// a group, since every subscriber here sees every message.
    async fn subscribe(&self, subject: &str, opts: &Options) -> Result<Subscription> {
        self.inner.declares(&self.stream, subject)?;
        refuse_group(opts)?;
        let key = self.key(subject);

        // Fix the starting position before returning, so a payload published
        // after this returns is delivered rather than raced against the task
        // getting as far as its first read.
        let mut last = last_id(&self.inner, &key).await?;
        let mut conn = self.inner.blocking_conn().await?;
        tracing::info!(subject, key, from = %last, "subscribed");

        let (tx, rx) = mpsc::channel(capacity(opts));
        let registry = self.inner.registry.clone();
        let subject = subject.to_owned();
        tokio::spawn(async move {
            let mut delivered = 0u64;
            'read: loop {
                let mut command = redis::cmd("XREAD");
                command
                    .arg("COUNT")
                    .arg(READ_BATCH)
                    .arg("BLOCK")
                    .arg(BLOCK_FOR.as_millis() as u64)
                    .arg("STREAMS")
                    .arg(&key)
                    .arg(&last);
                let read = command.query_async::<Option<StreamReadReply>>(&mut conn);
                let reply = tokio::select! {
                    _ = tx.closed() => break,
                    reply = read => reply,
                };
                let reply = match reply {
                    // None is the block expiring with nothing to report.
                    Ok(None) => continue,
                    Ok(Some(reply)) => reply,
                    Err(error) => {
                        tracing::error!(subject, %error, "could not read the stream");
                        break;
                    }
                };
                for entry in reply.keys.into_iter().flat_map(|k| k.ids) {
                    last = entry.id.clone();
                    match decode(&registry, &subject, &entry) {
                        Ok(message) => {
                            if tx.send(message).await.is_err() {
                                break 'read;
                            }
                            delivered += 1;
                        }
                        Err(error) => tracing::warn!(subject, entry = %entry.id, %error,
                            "dropping a malformed message"),
                    }
                }
            }
            tracing::info!(subject, delivered, "subscription closed");
        });
        Ok(ReceiverStream::new(rx).boxed())
    }
}

#[async_trait::async_trait]
impl Durable for DurableManager {
    /// Joins the group at `$`: a new consumer sees only what is published after
    /// it first attaches, and an existing one resumes where it left off.
    async fn consume(&self, subject: &str, consumer: &str, opts: &Options) -> Result<Deliveries> {
        self.consume_from(subject, consumer, Position::New, opts)
            .await
    }
}

/// The consumer name is a Redis consumer group: two processes under one name
/// share its position and split the work, and a process that comes back
/// resumes where the name left off. Each process joins the group under an
/// identity of its own, which is what lets Redis tell whose message is
/// outstanding when one of them stops answering.
///
/// The position applies when the group is created and not after. A group that
/// already exists keeps its position; resetting it on every attach would
/// replay the log every time a process restarted.
#[async_trait::async_trait]
impl Positioned for DurableManager {
    /// Creates the group at `at` if it does not exist, then spawns its delivery
    /// loop under an identity unique to this call.
    async fn consume_from(
        &self,
        subject: &str,
        consumer: &str,
        at: Position,
        opts: &Options,
    ) -> Result<Deliveries> {
        self.inner.declares(&self.stream, subject)?;
        if consumer.is_empty() {
            return Err(StreamError::Unsupported(
                "a durable consumer needs a name, because the name is the position that \
                 survives a restart"
                    .into(),
            ));
        }
        if opts.group.is_some() {
            // Group and consumer would be two names for one thing here, and
            // honouring one silently would make the other a no-op.
            return Err(StreamError::Unsupported(format!(
                "the consumer name {consumer:?} is already the group on this provider; \
                 drop the group option"
            )));
        }

        let key = self.key(subject);
        let start = match at {
            Position::New => "$",
            Position::Earliest => "0",
        };
        // MKSTREAM so a consumer may attach before anything is published;
        // BUSYGROUP is the answer we want from a create run on every attach,
        // which is also what makes repeating it harmless.
        let created = self
            .inner
            .query::<()>(
                redis::cmd("XGROUP")
                    .arg("CREATE")
                    .arg(&key)
                    .arg(consumer)
                    .arg(start)
                    .arg("MKSTREAM"),
                Replay::Idempotent,
            )
            .await;
        if let Err(error) = created
            && error.code() != Some("BUSYGROUP")
        {
            return Err(StreamError::internal(
                &format!("redis: cannot create consumer {consumer:?} on {subject:?}"),
                error,
            ));
        }

        let reader = self.inner.blocking_conn().await?;
        let name = consumer_name();
        tracing::info!(subject, consumer, identity = %name, from = start, "consuming");

        let (tx, rx) = mpsc::channel(capacity(opts));
        let target = Arc::new(AckTarget {
            inner: Arc::clone(&self.inner),
            key: key.clone(),
            group: consumer.to_owned(),
            name: name.clone(),
            subject: subject.to_owned(),
        });
        let loop_ = ConsumeLoop {
            inner: Arc::clone(&self.inner),
            key,
            subject: subject.to_owned(),
            group: consumer.to_owned(),
            name,
            batch: prefetch(opts).clamp(1, MAX_READ_BATCH),
            acks: Acks::start(target),
            tx,
        };
        tokio::spawn(loop_.run(reader));
        Ok(ReceiverStream::new(rx).boxed())
    }
}

impl Manager for DurableManager {
    /// Some: consumer groups keep position and pending entries on the server.
    fn durable(&self) -> Option<&dyn Durable> {
        Some(self)
    }

    /// Some, though the position only applies when the group is first created.
    fn positioned(&self) -> Option<&dyn Positioned> {
        Some(self)
    }

    /// Some: a batch is one pipeline of XADDs.
    fn batch(&self) -> Option<&dyn Batch> {
        Some(self)
    }
}

/// One consumer's delivery loop: reclaim what a dead consumer abandoned, then
/// read what is new, until the reader drops its end.
struct ConsumeLoop {
    inner: Arc<Inner>,
    key: String,
    subject: String,
    group: String,
    name: String,
    /// How many entries one read or reclaim asks for: the prefetch.
    batch: usize,
    /// Where this consumer's deliveries send their acknowledgements.
    acks: Acks,
    tx: mpsc::Sender<Delivery>,
}

/// Why a step of the loop could not go on.
enum Stop {
    /// The reader dropped its end.
    Closed,
    /// Redis refused a read; it is logged and the stream ends.
    Failed,
    /// The connection went away: a restart, a failover, a network blip. The
    /// group, its position, and its pending list live on the server, so the
    /// loop reopens its connection and carries on rather than ending.
    Disconnected,
}

/// Whether `error` means the connection is gone rather than that Redis
/// refused the command.
fn disconnected(error: &redis::RedisError) -> bool {
    error.is_io_error()
        || error.is_connection_dropped()
        || error.is_connection_refusal()
        || error.is_unrecoverable_error()
        || error.code() == Some("LOADING")
}

impl ConsumeLoop {
    /// Delivers until the reader drops its end or Redis refuses a read or a
    /// reclaim, reconnecting across dropped connections, then leaves the group.
    async fn run(self, mut reader: MultiplexedConnection) {
        let mut delivered = 0u64;
        // Often enough that an abandoned message waits little past its reclaim
        // interval, rarely enough not to cost a round trip on every read.
        let reclaim_every = (self.inner.reclaim_after / 2).min(RECLAIM_EVERY_UP_TO);
        let mut reclaim_due = Instant::now();
        let mut lag_due = Instant::now();
        loop {
            // Messages a consumer took and never acknowledged are invisible to
            // a group read forever. Reclaiming them is what turns "we will
            // redeliver" from a promise into a behaviour.
            if Instant::now() >= reclaim_due {
                match self.reclaim(&mut reader).await {
                    Ok(0) => reclaim_due = Instant::now() + reclaim_every,
                    // There may be more: look again straight away.
                    Ok(sent) => {
                        delivered += sent;
                        continue;
                    }
                    Err(_) => break,
                }
            }
            if Instant::now() >= lag_due {
                self.measure_lag(&mut reader).await;
                lag_due = Instant::now() + MEASURE_LAG_EVERY;
            }
            match self.read(&mut reader).await {
                // A read that came back empty waited out its block: the
                // consumer is idle, so it may as well look for abandoned work.
                Ok(0) => reclaim_due = Instant::now(),
                Ok(sent) => {
                    delivered += sent;
                    if (sent as usize) < (self.batch / 8).max(2) {
                        tokio::time::sleep(COALESCE_FOR).await;
                    }
                }
                Err(Stop::Disconnected) => match self.reconnect().await {
                    Some(fresh) => reader = fresh,
                    None => break,
                },
                Err(_) => break,
            }
        }
        tracing::info!(subject = %self.subject, consumer = %self.group, delivered, "consumer stopped");
        self.leave().await;
    }

    /// Reopens the reader's connection, backing off between attempts, until it
    /// succeeds or the reader drops its end.
    ///
    /// Ending the stream instead would turn a broker restart into a consumer
    /// that silently stops, when nothing it depends on was lost: the group's
    /// position and pending list are on the server, and whatever this consumer
    /// held unacknowledged is reclaimed once the loop is back.
    async fn reconnect(&self) -> Option<MultiplexedConnection> {
        let mut wait = RECONNECT_FROM;
        loop {
            tokio::select! {
                _ = self.tx.closed() => return None,
                _ = tokio::time::sleep(wait) => {}
            }
            match self.inner.blocking_conn().await {
                Ok(conn) => {
                    tracing::info!(subject = %self.subject, consumer = %self.group,
                        "consumer reconnected");
                    return Some(conn);
                }
                Err(error) => {
                    tracing::debug!(subject = %self.subject, consumer = %self.group, %error,
                        "consumer still cannot reconnect");
                    wait = (wait * 2).min(RECONNECT_UP_TO);
                }
            }
        }
    }

    /// Runs `command` on the reader's connection unless the reader goes away
    /// first, so a dropped stream does not wait out a two-second block.
    async fn query<T: redis::FromRedisValue>(
        &self,
        conn: &mut MultiplexedConnection,
        command: &redis::Cmd,
    ) -> std::result::Result<redis::RedisResult<T>, Stop> {
        tokio::select! {
            _ = self.tx.closed() => Err(Stop::Closed),
            reply = command.query_async::<T>(conn) => Ok(reply),
        }
    }

    /// Takes over messages idle past the reclaim interval and delivers them,
    /// returning how many it sent.
    async fn reclaim(&self, conn: &mut MultiplexedConnection) -> std::result::Result<u64, Stop> {
        if self.inner.reclaim_after.is_zero() {
            return Ok(0);
        }
        let mut command = redis::cmd("XAUTOCLAIM");
        command
            .arg(&self.key)
            .arg(&self.group)
            .arg(&self.name)
            .arg(self.inner.reclaim_after.as_millis() as u64)
            .arg("0")
            .arg("COUNT")
            .arg(self.batch);
        let reply = match self.query::<StreamAutoClaimReply>(conn, &command).await? {
            Ok(reply) => reply,
            Err(error) => {
                // A failed reclaim is not a reason to stop consuming new work.
                tracing::warn!(subject = %self.subject, consumer = %self.group, %error,
                    "could not reclaim abandoned messages");
                return Ok(0);
            }
        };
        if reply.claimed.is_empty() {
            return Ok(0);
        }

        let attempts = self.attempts(conn).await?;
        let mut sent = 0;
        for entry in reply.claimed {
            // Claimed but no longer listed as pending: it has been delivered at
            // least twice by definition, so say so rather than claim it is new.
            let attempt = attempts.get(&entry.id).copied().unwrap_or(2);
            tracing::debug!(subject = %self.subject, entry = %entry.id, attempt,
                "reclaimed an abandoned message");
            self.deliver(entry, attempt).await?;
            sent += 1;
        }
        Ok(sent)
    }

    /// How many times each message outstanding with this consumer has been
    /// delivered: one call for the batch, because a round trip per message
    /// would undo the batching.
    async fn attempts(
        &self,
        conn: &mut MultiplexedConnection,
    ) -> std::result::Result<HashMap<String, u32>, Stop> {
        let mut command = redis::cmd("XPENDING");
        command
            .arg(&self.key)
            .arg(&self.group)
            .arg("-")
            .arg("+")
            .arg(self.batch)
            .arg(&self.name);
        let reply = self
            .query::<StreamPendingCountReply>(conn, &command)
            .await?;
        Ok(reply
            .map(|reply| {
                reply
                    .ids
                    .into_iter()
                    .map(|p| (p.id, u32::try_from(p.times_delivered).unwrap_or(u32::MAX)))
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Records the group's pending count, which is what "behind" means on
    /// Redis: messages handed out and not yet acknowledged.
    async fn measure_lag(&self, conn: &mut MultiplexedConnection) {
        let mut command = redis::cmd("XPENDING");
        command.arg(&self.key).arg(&self.group);
        if let Ok(Ok(reply)) = self.query::<StreamPendingReply>(conn, &command).await {
            let count = match reply {
                StreamPendingReply::Data(data) => data.count,
                _ => 0,
            };
            self.inner.metrics.lag(
                &self.subject,
                &self.group,
                i64::try_from(count).unwrap_or(i64::MAX),
            );
        }
    }

    /// One group read of new messages, returning how many it delivered.
    async fn read(&self, conn: &mut MultiplexedConnection) -> std::result::Result<u64, Stop> {
        let mut command = redis::cmd("XREADGROUP");
        command
            .arg("GROUP")
            .arg(&self.group)
            .arg(&self.name)
            .arg("COUNT")
            .arg(self.batch)
            .arg("BLOCK")
            .arg(BLOCK_FOR.as_millis() as u64)
            .arg("STREAMS")
            .arg(&self.key)
            .arg(">");
        let reply = match self
            .query::<Option<StreamReadReply>>(conn, &command)
            .await?
        {
            Ok(Some(reply)) => reply,
            // None is the block expiring with nothing to report.
            Ok(None) => return Ok(0),
            Err(error) if disconnected(&error) => {
                tracing::warn!(subject = %self.subject, consumer = %self.group, %error,
                    "lost the connection; reconnecting");
                return Err(Stop::Disconnected);
            }
            Err(error) => {
                tracing::error!(subject = %self.subject, consumer = %self.group, %error,
                    "could not read as the consumer group");
                return Err(Stop::Failed);
            }
        };
        let mut sent = 0;
        for entry in reply.keys.into_iter().flat_map(|k| k.ids) {
            // Fresh from the log, so this is its first delivery.
            self.deliver(entry, 1).await?;
            sent += 1;
        }
        Ok(sent)
    }

    /// Decodes an entry and lends it to the reader.
    async fn deliver(&self, entry: StreamId, attempt: u32) -> std::result::Result<(), Stop> {
        let message = match decode(&self.inner.registry, &self.subject, &entry) {
            Ok(message) => message,
            Err(error) => {
                // Nobody can decode it, so no handler will ever acknowledge it
                // and it would be reclaimed forever. Acknowledge it and say so.
                tracing::warn!(subject = %self.subject, entry = %entry.id, %error,
                    "acknowledging a malformed message so it does not redeliver forever");
                let _ = xack(&self.inner, &self.key, &self.group, &entry.id).await;
                return Ok(());
            }
        };
        let acker = RedisAck {
            acks: self.acks.clone(),
            entry: entry.id,
        };
        self.inner.metrics.delivered(&self.subject, &self.group);
        self.tx
            .send(Delivery::new(message, attempt, Box::new(acker)))
            .await
            .map_err(|_| Stop::Closed)
    }

    /// Removes this identity from the group once it holds nothing.
    ///
    /// Best effort and bounded: a consumer left behind holds no messages once
    /// its pending list is empty, but it does stay in the group's roster. One
    /// still holding messages stays, so they can be reclaimed from it.
    async fn leave(&self) {
        let cleanup = async {
            let pending: StreamPendingCountReply = self
                .inner
                .query(
                    redis::cmd("XPENDING")
                        .arg(&self.key)
                        .arg(&self.group)
                        .arg("-")
                        .arg("+")
                        .arg(1)
                        .arg(&self.name),
                    Replay::Idempotent,
                )
                .await
                .ok()?;
            if pending.ids.is_empty() {
                self.inner
                    .query::<()>(
                        redis::cmd("XGROUP")
                            .arg("DELCONSUMER")
                            .arg(&self.key)
                            .arg(&self.group)
                            .arg(&self.name),
                        Replay::Idempotent,
                    )
                    .await
                    .ok()?;
            }
            Some(())
        };
        let _ = tokio::time::timeout(CLEANUP_FOR, cleanup).await;
    }
}

/// What every delivery of one consumer acknowledges against.
struct AckTarget {
    inner: Arc<Inner>,
    key: String,
    group: String,
    /// This consumer's identity in the group, which a delayed nak claims the
    /// entry back to.
    name: String,
    subject: String,
}

/// One acknowledgement waiting to be sent, and who to tell when it has been:
/// nobody, when [`RedisConfig::confirm_acks`](super::RedisConfig::confirm_acks)
/// is off.
type PendingAck = (
    String,
    Option<oneshot::Sender<std::result::Result<(), String>>>,
);

/// A consumer's acknowledgements, coalesced into multi-id XACKs.
///
/// XACK takes any number of ids, so acknowledgements that arrive while one
/// XACK is in flight go out together in the next, rather than as one command
/// each. Nothing waits to fill a batch: an ack on an idle consumer is sent at
/// once, alone. Each [`Acker::ack`] still returns only after the XACK carrying
/// its id has been answered, so a successful ack means what it always meant
/// and at-least-once is untouched, unless the provider was told not to wait
/// (see [`RedisConfig::confirm_acks`](super::RedisConfig::confirm_acks)).
#[derive(Clone)]
struct Acks {
    target: Arc<AckTarget>,
    tx: mpsc::UnboundedSender<PendingAck>,
}

impl Acks {
    /// Starts the sender task, which ends when the consumer loop and every
    /// delivery it lent have been dropped.
    fn start(target: Arc<AckTarget>) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<PendingAck>();
        let sender = Arc::clone(&target);
        tokio::spawn(async move {
            let mut batch = Vec::new();
            while rx.recv_many(&mut batch, MAX_ACK_BATCH).await > 0 {
                let mut command = redis::cmd("XACK");
                command.arg(&sender.key).arg(&sender.group);
                for (entry, _) in &batch {
                    command.arg(entry);
                }
                // Acknowledging an acknowledged entry is a no-op, so the whole
                // XACK is safe to repeat.
                let sent = sender
                    .inner
                    .query::<()>(&command, Replay::Idempotent)
                    .await
                    .map_err(|error| error.to_string());
                if let Err(error) = &sent {
                    tracing::warn!(subject = %sender.subject, consumer = %sender.group,
                        count = batch.len(), %error,
                        "could not acknowledge; the entries stay pending and are redelivered");
                }
                for (_, done) in batch.drain(..) {
                    // The delivery may have been dropped mid-ack, or nobody is
                    // waiting; either way there is nobody to tell.
                    if let Some(done) = done {
                        let _ = done.send(sent.clone());
                    }
                }
            }
        });
        Self { target, tx }
    }
}

/// Settles one delivery against its consumer group.
struct RedisAck {
    acks: Acks,
    entry: String,
}

#[async_trait::async_trait]
impl Acker for RedisAck {
    /// Queues an XACK for the consumer's ack task, which batches them. With
    /// `confirm_acks` it waits for Redis to answer; without, it returns once queued.
    async fn ack(self: Box<Self>) -> Result<()> {
        let target = &self.acks.target;
        let failed = |error: String| {
            StreamError::Internal(format!(
                "redis: cannot acknowledge {} on {:?}: {error}",
                self.entry, target.subject
            ))
        };
        let confirm = target.inner.confirm_acks;
        let (done, answer) = match confirm {
            true => {
                let (done, answer) = oneshot::channel();
                (Some(done), Some(answer))
            }
            false => (None, None),
        };
        self.acks
            .tx
            .send((self.entry.clone(), done))
            .map_err(|_| failed("the consumer's acknowledgement task has stopped".into()))?;
        if let Some(answer) = answer {
            answer
                .await
                .map_err(|_| failed("the acknowledgement was dropped unanswered".into()))?
                .map_err(failed)?;
        }
        target
            .inner
            .metrics
            .settled(&target.subject, &target.group, "ack");
        Ok(())
    }

    /// Leaves the message pending: it stays outstanding and is reclaimed once
    /// it has been idle for the reclaim interval, by whichever consumer gets
    /// there first.
    ///
    /// Appending a copy to the tail would redeliver it sooner, but with a new
    /// id and an attempt count of one, and the attempt is the only signal a
    /// consumer has for noticing a redelivery loop it cannot escape.
    async fn nak(self: Box<Self>) -> Result<()> {
        let target = &self.acks.target;
        target
            .inner
            .metrics
            .settled(&target.subject, &target.group, "nak");
        Ok(())
    }

    /// Leaves the message pending, as [`RedisAck::nak`] does, with its idle
    /// time set back so the group's reclaim reaches it `delay` from now rather
    /// than after the whole reclaim interval.
    ///
    /// The reclaim takes entries idle for at least
    /// [`RedisConfig::reclaim_after`](super::RedisConfig::reclaim_after), so
    /// an entry made `reclaim_after - delay` idle becomes eligible once
    /// `delay` has passed, and is redelivered on the next reclaim after that:
    /// never sooner, at most a reclaim pass later. XCLAIM with `JUSTID` sets
    /// the idle time without counting a delivery, so the attempt number the
    /// redelivery carries is still the true one. The server holds the timing,
    /// so it survives a restart and needs no clock of ours.
    ///
    /// Redis refuses an idle time in the future, so a delay longer than
    /// `reclaim_after` cannot be honoured and is refused; the entry then
    /// redelivers after `reclaim_after`, as a plain nak's does.
    async fn nak_after(self: Box<Self>, delay: Duration) -> Result<()> {
        let target = &self.acks.target;
        let idle = idle_for_delay(target.inner.reclaim_after, delay)?;
        // Safe to repeat: a second XCLAIM sets the same idle time again.
        let claimed: Vec<String> = target
            .inner
            .query(
                redis::cmd("XCLAIM")
                    .arg(&target.key)
                    .arg(&target.group)
                    .arg(&target.name)
                    .arg(0)
                    .arg(&self.entry)
                    .arg("IDLE")
                    .arg(idle)
                    .arg("JUSTID"),
                Replay::Idempotent,
            )
            .await
            .map_err(fail(format!(
                "redis: cannot delay the redelivery of {} on {:?}",
                self.entry, target.subject
            )))?;
        if claimed.is_empty() {
            // Acknowledged already, or trimmed off the log: nothing is
            // pending to redeliver.
            tracing::warn!(subject = %target.subject, entry = %self.entry,
                "a delayed nak found the entry no longer pending");
        }
        target
            .inner
            .metrics
            .settled(&target.subject, &target.group, "nak");
        Ok(())
    }

    /// Whenever reclaiming is on, since reclaiming is what redelivers.
    fn delays_nak(&self) -> bool {
        !self.acks.target.inner.reclaim_after.is_zero()
    }

    /// Redis redelivers an entry once it has been idle for the reclaim
    /// interval and cannot set an idle time in the future, so that interval
    /// is the longest a delayed nak can wait.
    fn max_nak_delay(&self) -> Option<Duration> {
        Some(self.acks.target.inner.reclaim_after)
    }
}

/// The idle time, in milliseconds, that makes an entry eligible for a reclaim
/// of `reclaim_after` once `delay` has passed, or why there is none.
///
/// Rounded so the delay is never shortened: the delay up, the result down.
fn idle_for_delay(reclaim_after: Duration, delay: Duration) -> Result<u64> {
    if reclaim_after.is_zero() {
        return Err(StreamError::Unsupported(
            "redis: reclaiming is off (RedisConfig::reclaim_after is zero), so nothing \
             would ever redeliver a delayed nak"
                .into(),
        ));
    }
    if delay > reclaim_after {
        return Err(StreamError::Unsupported(format!(
            "redis: a delayed nak waits at most RedisConfig::reclaim_after ({reclaim_after:?}), \
             because Redis redelivers an entry once it has been idle that long and cannot set \
             an idle time in the future; asked for {delay:?}. The entry stays pending and \
             redelivers after reclaim_after"
        )));
    }
    let reclaim = reclaim_after.as_millis() as u64;
    let delay = delay.as_nanos().div_ceil(1_000_000) as u64;
    Ok(reclaim.saturating_sub(delay))
}

/// XACK one entry outside the batching ack task, for an entry that is
/// settled by the loop itself rather than by a handler.
async fn xack(inner: &Inner, key: &str, group: &str, entry: &str) -> Result<()> {
    // Acknowledging an acknowledged entry is a no-op.
    inner
        .query::<()>(
            redis::cmd("XACK").arg(key).arg(group).arg(entry),
            Replay::Idempotent,
        )
        .await
        .map_err(fail("redis: XACK failed"))
}

/// The id of the newest entry in `key`, or the beginning when it is empty.
async fn last_id(inner: &Inner, key: &str) -> Result<String> {
    let reply: StreamRangeReply = inner
        .query(
            redis::cmd("XREVRANGE")
                .arg(key)
                .arg("+")
                .arg("-")
                .arg("COUNT")
                .arg(1),
            Replay::Idempotent,
        )
        .await
        .map_err(fail(format!("redis: cannot read the end of {key:?}")))?;
    Ok(reply
        .ids
        .into_iter()
        .next()
        .map_or_else(|| "0".to_owned(), |entry| entry.id))
}

/// The entry field header `name` is stored under.
fn header_field(name: &str) -> String {
    format!("{HEADER_FIELD_PREFIX}{name}")
}

/// Turns a stream entry back into a message, with the headers its `h:` fields
/// carry.
fn decode(registry: &Registry, subject: &str, entry: &StreamId) -> Result<Message> {
    let raw: Vec<u8> = entry.get(PAYLOAD_FIELD).ok_or_else(|| StreamError::Frame {
        subject: subject.to_owned(),
        reason: format!("entry {} carries no {PAYLOAD_FIELD:?} field", entry.id),
    })?;
    let message = unpack(registry, subject, raw)?;
    let mut headers = Headers::new();
    for field in entry.map.keys() {
        let Some(name) = field.strip_prefix(HEADER_FIELD_PREFIX) else {
            continue;
        };
        // A field some other writer added that is not text is not a header
        // this contract could have written; it is left out, not fatal.
        if let Some(value) = entry.get::<String>(field) {
            headers.insert(name, value);
        }
    }
    Ok(message.with_headers(headers))
}

/// This process's identity within a consumer group.
///
/// Redis tracks outstanding messages per consumer, so two processes sharing a
/// group need different names or each would see the other's work as its own.
/// Host and pid make it recognisable in XINFO CONSUMERS; the id keeps two
/// consumers in one process apart.
fn consumer_name() -> String {
    let host = std::env::var("HOSTNAME")
        .ok()
        .filter(|host| !host.is_empty())
        .unwrap_or_else(|| "unknown".into());
    format!("{host}-{}-{}", std::process::id(), new_id())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumer_names_are_distinct_within_a_process() {
        assert_ne!(consumer_name(), consumer_name());
        assert!(consumer_name().contains(&std::process::id().to_string()));
    }

    #[test]
    fn decode_reads_the_go_field() {
        use crate::provider::pack;
        let frame = pack("m1", &Payload::json(&"x".to_owned()).unwrap());
        let entry = StreamId {
            id: "1-0".into(),
            map: HashMap::from([(PAYLOAD_FIELD.to_owned(), redis::Value::BulkString(frame))]),
            ..Default::default()
        };
        let message = decode(&Registry::new(), "s", &entry).unwrap();
        assert_eq!(message.id(), "m1");

        let empty = StreamId {
            id: "2-0".into(),
            ..Default::default()
        };
        assert!(matches!(
            decode(&Registry::new(), "s", &empty),
            Err(StreamError::Frame { .. })
        ));
    }

    #[test]
    fn decode_reads_header_fields_and_nothing_else() {
        let frame = pack("m1", &Payload::json(&"x".to_owned()).unwrap());
        let bulk = |s: &str| redis::Value::BulkString(s.as_bytes().to_vec());
        let entry = StreamId {
            id: "1-0".into(),
            map: HashMap::from([
                (PAYLOAD_FIELD.to_owned(), redis::Value::BulkString(frame)),
                (header_field("lang"), bulk("rust")),
                (header_field("msg"), bulk("a header called msg")),
                ("other".to_owned(), bulk("not a header")),
            ]),
            ..Default::default()
        };
        let message = decode(&Registry::new(), "s", &entry).unwrap();
        let want: Headers = [("lang", "rust"), ("msg", "a header called msg")]
            .into_iter()
            .collect();
        assert_eq!(message.headers(), &want);
        assert_eq!(message.json::<String>().unwrap(), "x");
    }

    #[test]
    fn header_fields_cannot_collide_with_the_driver_fields() {
        // The contract refuses ':' in a key, so no header maps onto `msg`,
        // `meta`, or another header's field.
        assert!(header_field("lang").starts_with(HEADER_FIELD_PREFIX));
        for own in [PAYLOAD_FIELD, super::super::keys::METADATA_FIELD] {
            assert!(!own.starts_with(HEADER_FIELD_PREFIX));
        }
        let bad: Headers = [("h:x", "1")].into_iter().collect();
        assert!(matches!(bad.validate(), Err(StreamError::InvalidHeader(_))));
    }

    #[test]
    fn a_delayed_nak_sets_back_the_idle_time() {
        let reclaim = Duration::from_secs(30);
        assert_eq!(idle_for_delay(reclaim, Duration::ZERO).unwrap(), 30_000);
        assert_eq!(
            idle_for_delay(reclaim, Duration::from_secs(5)).unwrap(),
            25_000
        );
        // Rounded so the delay is never shortened.
        assert_eq!(
            idle_for_delay(reclaim, Duration::from_micros(1_500)).unwrap(),
            29_998
        );
        assert_eq!(idle_for_delay(reclaim, reclaim).unwrap(), 0);
        assert!(matches!(
            idle_for_delay(reclaim, reclaim + Duration::from_millis(1)),
            Err(StreamError::Unsupported(_))
        ));
        assert!(matches!(
            idle_for_delay(Duration::ZERO, Duration::from_secs(1)),
            Err(StreamError::Unsupported(_))
        ));
    }
}
