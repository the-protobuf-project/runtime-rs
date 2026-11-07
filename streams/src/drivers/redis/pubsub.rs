//! Immediate delivery over pub/sub, and scheduled delivery over keyspace
//! expiry events: the two modes that hand a message over and forget it.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    Batch, Manager, Message, Options, Payload, Publisher, Result, Stream, StreamError, Subscriber,
    Subscription,
    provider::{batch_result, check_batch, new_id, pack, prefetch, publish_id, unpack},
};

use super::{Inner, Replay, fail, keys::Kind};

/// Publishes to and subscribes from one immediate or scheduled stream.
///
/// It holds the metadata read at bind time, so subjects are validated without
/// a round trip on every call. It offers [`Batch`] and nothing else optional:
/// pub/sub remembers nothing, so there is no position to resume or replay.
pub struct PubSubManager {
    inner: Arc<Inner>,
    stream: Stream,
}

impl PubSubManager {
    pub(super) fn new(inner: Arc<Inner>, stream: Stream) -> Self {
        Self { inner, stream }
    }

    fn scheduled(&self) -> bool {
        self.inner.kind == Kind::Notify
    }

    /// The TTL a scheduled publish fires after, or the refusal an immediate
    /// stream gives a TTL and a scheduled one gives its absence.
    fn delay(&self, opts: &Options) -> Result<Option<Duration>> {
        match (self.scheduled(), opts.delay()) {
            (true, Some(ttl)) => Ok(Some(ttl)),
            // A zero TTL never expires, so the notification could never fire.
            // Accepting it silently would strand the subscriber.
            (true, None) => Err(StreamError::Unsupported(format!(
                "a scheduled message needs a positive TTL, got {:?}",
                opts.ttl.unwrap_or_default()
            ))),
            (false, None) => Ok(None),
            // Saying so beats publishing now and letting the caller believe it
            // was scheduled.
            (false, Some(_)) => Err(StreamError::Unsupported(format!(
                "stream {} delivers immediately; use RedisProvider::scheduled for a TTL",
                self.stream.id
            ))),
        }
    }

    /// Queues the two writes a scheduled message needs onto `pipe`.
    ///
    /// The payload key holds the frame and does not expire, because the
    /// pending key's value is gone by the time its expiry fires. It is queued
    /// first so the pending key can never expire before its body exists.
    fn queue_schedule(
        &self,
        pipe: &mut redis::Pipeline,
        subject: &str,
        id: &str,
        frame: Vec<u8>,
        ttl: Duration,
    ) {
        // PX rather than EX so a sub-second delay is honoured, and at least
        // one millisecond because Redis refuses a zero expiry.
        let millis = ttl.as_millis().clamp(1, u128::from(u64::MAX)) as u64;
        pipe.cmd("SET")
            .arg(self.inner.keys.payload(id))
            .arg(frame)
            .ignore()
            .cmd("SET")
            .arg(self.inner.keys.pending(&self.stream.id, subject, id))
            .arg(id)
            .arg("PX")
            .arg(millis)
            .ignore();
    }

    /// Subscribes to the expiry events of this stream's subject.
    ///
    /// Redis publishes one event per expired key across the whole database, so
    /// this filters by key prefix down to the stream and subject asked for.
    async fn subscribe_scheduled(&self, subject: &str, capacity: usize) -> Result<Subscription> {
        let channel = format!("__keyevent@{}__:expired", self.inner.database);
        let messages = open(&self.inner, &channel).await?;
        let want = self.inner.keys.pending_prefix(&self.stream.id, subject);
        tracing::info!(subject, channel, prefix = %want, "subscribed to scheduled deliveries");

        let inner = Arc::clone(&self.inner);
        let subject = subject.to_owned();
        Ok(feed(capacity, messages, move |event| {
            let inner = Arc::clone(&inner);
            let subject = subject.clone();
            let want = want.clone();
            async move {
                let key = String::from_utf8_lossy(event.get_payload_bytes());
                let id = key.trim().strip_prefix(want.as_str())?.to_owned();
                match claim(&inner, &subject, &id).await {
                    Ok(Some(message)) => Some(message),
                    // Another subscriber got there first; one expiry delivers
                    // one message.
                    Ok(None) => None,
                    Err(error) => {
                        tracing::warn!(subject, id, %error, "could not claim a scheduled payload");
                        None
                    }
                }
            }
        }))
    }
}

/// Reads a scheduled frame and removes it in one command, so one expiry
/// delivers one message even with several subscribers racing for it.
async fn claim(inner: &Inner, subject: &str, id: &str) -> Result<Option<Message>> {
    // Safe to repeat: GETDEL hands a payload to one caller at most, so a
    // repeat after a lost reply finds it already claimed and delivers nothing,
    // which is what losing the reply had already cost.
    let raw: Option<Vec<u8>> = inner
        .query(
            redis::cmd("GETDEL").arg(inner.keys.payload(id)),
            Replay::Idempotent,
        )
        .await
        .map_err(fail(format!("redis: cannot claim scheduled message {id}")))?;
    raw.map(|raw| unpack(&inner.registry, subject, raw))
        .transpose()
}

/// Opens a dedicated pub/sub connection subscribed to `channel`.
///
/// The subscribe is confirmed by the server before this returns, so a payload
/// published afterwards is delivered rather than raced.
async fn open(inner: &Inner, channel: &str) -> Result<redis::aio::PubSubStream> {
    let mut pubsub = inner
        .client
        .get_async_pubsub()
        .await
        .map_err(fail("redis: cannot open a subscription connection"))?;
    pubsub
        .subscribe(channel)
        .await
        .map_err(fail(format!("redis: cannot subscribe to {channel}")))?;
    Ok(pubsub.into_on_message())
}

/// Forwards what `handle` makes of each pub/sub message through a channel of
/// `capacity`, so a slow reader backs up into the buffer the caller asked for.
///
/// The task ends when the reader drops its end, and dropping the pub/sub
/// connection with it is what unsubscribes on the server; a reader that stops
/// reading cannot strand the task, because a closed channel fails the send.
fn feed<F, Fut>(capacity: usize, mut messages: redis::aio::PubSubStream, handle: F) -> Subscription
where
    F: Fn(redis::Msg) -> Fut + Send + 'static,
    Fut: Future<Output = Option<Message>> + Send,
{
    let (tx, rx) = mpsc::channel(capacity);
    tokio::spawn(async move {
        let mut delivered = 0u64;
        loop {
            let raw = tokio::select! {
                _ = tx.closed() => break,
                raw = messages.next() => match raw {
                    Some(raw) => raw,
                    None => break,
                },
            };
            let Some(message) = handle(raw).await else {
                continue;
            };
            if tx.send(message).await.is_err() {
                break;
            }
            delivered += 1;
        }
        tracing::info!(delivered, "subscription closed");
    });
    ReceiverStream::new(rx).boxed()
}

/// Refuses [`Options::group`] where the provider cannot share a subject.
///
/// A group changes what a subscription means: honouring it by fanning out
/// anyway would double the work the caller asked to split.
pub(super) fn refuse_group(opts: &Options) -> Result<()> {
    match &opts.group {
        Some(group) => Err(StreamError::Unsupported(format!(
            "Redis cannot share a subscription among a group ({group:?}); \
             every subscriber receives every message, so drop the group option \
             or use a durable consumer"
        ))),
        None => Ok(()),
    }
}

/// The channel capacity for `opts`. A prefetch of zero still needs one slot,
/// which is the closest a bounded channel comes to a synchronous hand-off.
pub(super) fn capacity(opts: &Options) -> usize {
    prefetch(opts).max(1)
}

#[async_trait::async_trait]
impl Publisher for PubSubManager {
    async fn publish(&self, subject: &str, payload: Payload, opts: &Options) -> Result<String> {
        self.inner.declares(&self.stream, subject)?;
        let delay = self.delay(opts)?;
        let id = publish_id(opts);
        let frame = pack(&id, &payload);

        if let Some(ttl) = delay {
            let mut pipe = redis::pipe();
            pipe.atomic();
            self.queue_schedule(&mut pipe, subject, &id, frame, ttl);
            // Once: a repeat after the first had landed restarts the timer,
            // and one that lands after a short timer already fired schedules
            // the message a second time.
            self.inner
                .query_pipe::<()>(&pipe, Replay::Once)
                .await
                .map_err(fail(format!("redis: cannot schedule on {subject:?}")))?;
            tracing::debug!(subject, id, ?ttl, "scheduled");
            return Ok(id);
        }

        // Exactly once. Subscribe confirms its subscription before returning,
        // so there is no window a second send would cover; it would only
        // deliver the message twice.
        self.inner
            .query::<()>(
                redis::cmd("PUBLISH")
                    .arg(self.inner.keys.channel(&self.stream.id, subject))
                    .arg(frame),
                Replay::Once,
            )
            .await
            .map_err(fail(format!("redis: cannot publish on {subject:?}")))?;
        tracing::debug!(subject, id, "published");
        Ok(id)
    }
}

#[async_trait::async_trait]
impl Subscriber for PubSubManager {
    async fn subscribe(&self, subject: &str, opts: &Options) -> Result<Subscription> {
        self.inner.declares(&self.stream, subject)?;
        refuse_group(opts)?;
        let capacity = capacity(opts);
        if self.scheduled() {
            return self.subscribe_scheduled(subject, capacity).await;
        }

        let channel = self.inner.keys.channel(&self.stream.id, subject);
        let messages = open(&self.inner, &channel).await?;
        tracing::info!(subject, channel, "subscribed");

        let registry = self.inner.registry.clone();
        let subject = subject.to_owned();
        Ok(feed(capacity, messages, move |raw| {
            let message = unpack(
                &registry,
                &subject,
                Bytes::copy_from_slice(raw.get_payload_bytes()),
            );
            async move {
                match message {
                    Ok(message) => Some(message),
                    // One bad message is not a reason to tear down a healthy
                    // subscription.
                    Err(error) => {
                        tracing::warn!(%error, "dropping a malformed message");
                        None
                    }
                }
            }
        }))
    }
}

/// One pipeline for the whole batch: PUBLISH on an immediate stream, the
/// SET pair on a scheduled one, inside one transaction so a batch is
/// scheduled whole or not at all.
#[async_trait::async_trait]
impl Batch for PubSubManager {
    async fn publish_batch(
        &self,
        subject: &str,
        payloads: Vec<Payload>,
        opts: &Options,
    ) -> Result<Vec<String>> {
        check_batch(opts)?;
        self.inner.declares(&self.stream, subject)?;
        let delay = self.delay(opts)?;
        let ids: Vec<String> = payloads.iter().map(|_| new_id()).collect();
        if ids.is_empty() {
            return Ok(ids);
        }

        let mut pipe = redis::pipe();
        let channel = self.inner.keys.channel(&self.stream.id, subject);
        if delay.is_some() {
            pipe.atomic();
        }
        for (id, payload) in ids.iter().zip(&payloads) {
            let frame = pack(id, payload);
            match delay {
                Some(ttl) => self.queue_schedule(&mut pipe, subject, id, frame, ttl),
                None => {
                    pipe.cmd("PUBLISH").arg(&channel).arg(frame).ignore();
                }
            }
        }

        let sent = self
            .inner
            .query_pipe::<()>(&pipe, Replay::Once)
            .await
            .map_err(fail(format!(
                "redis: cannot publish a batch on {subject:?}"
            )));
        // A pipeline reports one failure for the lot, so every entry carries
        // it; some may still have landed, which is what at-least-once allows.
        let failures = match sent {
            Ok(()) => Vec::new(),
            Err(error) => (0..ids.len()).map(|index| (index, error.clone())).collect(),
        };
        batch_result(subject, ids, failures)
    }
}

impl Manager for PubSubManager {
    fn batch(&self) -> Option<&dyn Batch> {
        Some(self)
    }
}
