//! Durable delivery over Redis Streams: a log per subject, a consumer group per
//! consumer name, and a pending list that holds each message until it is
//! acknowledged.

use std::{collections::HashMap, sync::Arc, time::Duration};

use futures::StreamExt;
use redis::{
    aio::MultiplexedConnection,
    streams::{
        StreamAutoClaimReply, StreamId, StreamPendingCountReply, StreamPendingReply,
        StreamRangeReply, StreamReadReply,
    },
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    Acker, Batch, Deliveries, Delivery, Durable, Manager, Message, Options, Payload, Position,
    Positioned, Publisher, Registry, Result, Stream, StreamError, Subscriber, Subscription,
    provider::{batch_result, check_batch, new_id, pack, publish_id, unpack},
};

use super::{
    Inner, Replay, fail,
    keys::PAYLOAD_FIELD,
    pubsub::{capacity, refuse_group},
};

/// How many entries one read asks for: one round trip per batch rather than
/// per message.
const READ_BATCH: usize = 64;

/// How long a read waits at the server before coming back empty. It bounds how
/// long a loop goes without checking for work to reclaim, so it is short
/// enough to feel responsive and long enough not to poll in a hot loop.
pub(super) const BLOCK_FOR: Duration = Duration::from_secs(2);

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
    pub(super) fn new(inner: Arc<Inner>, stream: Stream) -> Self {
        Self { inner, stream }
    }

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

    /// Queues one XADD of `frame` onto `pipe`, trimmed when a cap is set.
    fn queue_append(&self, pipe: &mut redis::Pipeline, key: &str, frame: Vec<u8>) {
        let cmd = pipe.cmd("XADD").arg(key);
        if let Some(max) = self.inner.max_len {
            // Approximate: Redis trims on node boundaries, which is what lets
            // it trim without walking the stream.
            cmd.arg("MAXLEN").arg("~").arg(max);
        }
        cmd.arg("*").arg(PAYLOAD_FIELD).arg(frame).ignore();
    }
}

#[async_trait::async_trait]
impl Publisher for DurableManager {
    async fn publish(&self, subject: &str, payload: Payload, opts: &Options) -> Result<String> {
        self.inner.declares(&self.stream, subject)?;
        Self::refuse_ttl(opts)?;
        let id = publish_id(opts);
        let mut pipe = redis::pipe();
        self.queue_append(&mut pipe, &self.key(subject), pack(&id, &payload));
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
    async fn publish_batch(
        &self,
        subject: &str,
        payloads: Vec<Payload>,
        opts: &Options,
    ) -> Result<Vec<String>> {
        check_batch(opts)?;
        self.inner.declares(&self.stream, subject)?;
        Self::refuse_ttl(opts)?;
        let ids: Vec<String> = payloads.iter().map(|_| new_id()).collect();
        if ids.is_empty() {
            return Ok(ids);
        }
        let key = self.key(subject);
        let mut pipe = redis::pipe();
        for (id, payload) in ids.iter().zip(&payloads) {
            self.queue_append(&mut pipe, &key, pack(id, payload));
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
        let loop_ = ConsumeLoop {
            inner: Arc::clone(&self.inner),
            key,
            subject: subject.to_owned(),
            group: consumer.to_owned(),
            name,
            tx,
        };
        tokio::spawn(loop_.run(reader));
        Ok(ReceiverStream::new(rx).boxed())
    }
}

impl Manager for DurableManager {
    fn durable(&self) -> Option<&dyn Durable> {
        Some(self)
    }

    fn positioned(&self) -> Option<&dyn Positioned> {
        Some(self)
    }

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
    async fn run(self, mut reader: MultiplexedConnection) {
        let mut delivered = 0u64;
        loop {
            // Messages a consumer took and never acknowledged are invisible to
            // a group read forever. Reclaiming them is what turns "we will
            // redeliver" from a promise into a behaviour.
            match self.reclaim(&mut reader).await {
                Ok(0) => {}
                Ok(sent) => {
                    delivered += sent;
                    continue;
                }
                Err(_) => break,
            }
            self.measure_lag(&mut reader).await;
            match self.read(&mut reader).await {
                Ok(sent) => delivered += sent,
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
            .arg(READ_BATCH);
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
            .arg(READ_BATCH)
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
            .arg(READ_BATCH)
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
            inner: Arc::clone(&self.inner),
            key: self.key.clone(),
            group: self.group.clone(),
            entry: entry.id,
            subject: self.subject.clone(),
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

/// Settles one delivery against its consumer group.
struct RedisAck {
    inner: Arc<Inner>,
    key: String,
    group: String,
    entry: String,
    subject: String,
}

#[async_trait::async_trait]
impl Acker for RedisAck {
    async fn ack(self: Box<Self>) -> Result<()> {
        xack(&self.inner, &self.key, &self.group, &self.entry)
            .await
            .map_err(|error| {
                StreamError::Internal(format!(
                    "redis: cannot acknowledge {} on {:?}: {error}",
                    self.entry, self.subject
                ))
            })?;
        self.inner
            .metrics
            .settled(&self.subject, &self.group, "ack");
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
        self.inner
            .metrics
            .settled(&self.subject, &self.group, "nak");
        Ok(())
    }
}

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

/// Turns a stream entry back into a message.
fn decode(registry: &Registry, subject: &str, entry: &StreamId) -> Result<Message> {
    let raw: Vec<u8> = entry.get(PAYLOAD_FIELD).ok_or_else(|| StreamError::Frame {
        subject: subject.to_owned(),
        reason: format!("entry {} carries no {PAYLOAD_FIELD:?} field", entry.id),
    })?;
    unpack(registry, subject, raw)
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
}
