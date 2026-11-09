//! Publishing to and consuming from one bound stream.

use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use bytes::Bytes;
use futures::{StreamExt, future::join_all};
use rdkafka::{
    ClientContext, Message as _, Offset, TopicPartitionList,
    consumer::{
        BaseConsumer, CommitMode, Consumer, ConsumerContext, RebalanceProtocol, StreamConsumer,
    },
    error::KafkaError,
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    types::{RDKafkaErrorCode, RDKafkaRespErr},
};
use tokio::sync::{Notify, mpsc};

use crate::{
    Acker, Batch, Deliveries, Delivery, Durable, Headers, Manager, Message, Options, Payload,
    Position, Positioned, Publisher, Result, Stream, StreamError, Subscriber, Subscription,
    provider,
};

use super::{DIAL_TIMEOUT, Inner, blocking, reader_group, safe_name};

/// How long a publish waits for room in librdkafka's queue before failing.
/// Delivery itself is bounded by the client's own `delivery.timeout.ms`.
const QUEUE_TIMEOUT: Duration = Duration::from_secs(5);

/// Pause after a consumer error before reading again. librdkafka reconnects
/// on its own; this only keeps a broker outage from becoming a log flood.
const ERROR_BACKOFF: Duration = Duration::from_millis(250);

/// A [`Manager`] bound to one Kafka-backed stream.
///
/// Obtained from [`Streams::bind`](crate::Streams::bind). It holds no
/// connection of its own beyond the provider's shared producer; every
/// subscription builds, and on drop closes, its own consumer.
pub struct KafkaManager {
    inner: Arc<Inner>,
    stream: Stream,
    /// Each declared subject's topic name, worked out once rather than on
    /// every publish.
    topics: HashMap<String, String>,
}

/// Which consumer a subscription needs.
enum Reader {
    /// No group, assigned at the current end of every partition.
    Tail,
    /// A member of a group: shared with other members, offsets committed
    /// automatically, because a plain subscription promises no redelivery.
    Shared(String),
    /// A member of a group whose offsets move only on ack.
    Durable(String, Position),
}

impl KafkaManager {
    /// Works out every declared subject's topic name up front, so a publish
    /// looks its topic up rather than formatting it each time.
    pub(super) fn new(inner: Arc<Inner>, stream: Stream) -> Self {
        let topics = stream
            .subjects
            .iter()
            .map(|subject| (subject.clone(), inner.topic(&stream.id, subject)))
            .collect();
        Self {
            inner,
            stream,
            topics,
        }
    }

    /// The topic carrying `subject`, which [`Self::check_subject`] has passed.
    fn topic(&self, subject: &str) -> std::borrow::Cow<'_, str> {
        match self.topics.get(subject) {
            Some(topic) => topic.as_str().into(),
            None => self.inner.topic(&self.stream.id, subject).into(),
        }
    }

    /// Rejects a subject the stream does not declare.
    ///
    /// A Kafka topic is a literal name, so a declared `orders.*` is a subject
    /// called that and not a pattern: matched by name, as in Go.
    fn check_subject(&self, subject: &str) -> Result<()> {
        if provider::declares(&self.stream.subjects, subject) {
            return Ok(());
        }
        tracing::error!(subject, stream = %self.stream.id, declared = ?self.stream.subjects,
            "subject is not declared by this stream");
        Err(provider::unknown_subject(
            &self.stream.id,
            subject,
            &self.stream.subjects,
        ))
    }

    /// Refuses what Kafka cannot do per message: deliver later, expire, or
    /// keep a last value.
    fn check_options(opts: &Options) -> Result<()> {
        if opts.delay().is_some() {
            return Err(StreamError::Unsupported(
                "Kafka delivers when it is read, not on a timer".into(),
            ));
        }
        if opts.expiry().is_some() {
            return Err(StreamError::Unsupported(
                "Kafka has no per-message expiry: a record stays readable until the topic's \
                 retention removes it, whoever has or has not read it"
                    .into(),
            ));
        }
        if opts.retain {
            return Err(StreamError::Unsupported(
                "Kafka has no per-message retain: the nearest thing, a compacted topic, keeps \
                 only the newest record per key for the whole subject, which would change \
                 what every reader of the stream sees"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Appends one framed payload to `topic` through `producer`, keyed by the
    /// partition key, carrying `headers` as its record headers.
    async fn send(
        producer: &FutureProducer,
        topic: &str,
        frame: &[u8],
        key: Option<&str>,
        headers: Option<OwnedHeaders>,
    ) -> Result<()> {
        let mut record = FutureRecord::<str, [u8]>::to(topic).payload(frame);
        if let Some(key) = key {
            record = record.key(key);
        }
        if let Some(headers) = headers {
            record = record.headers(headers);
        }
        producer
            .send(record, QUEUE_TIMEOUT)
            .await
            .map(drop)
            .map_err(|(error, _)| {
                StreamError::internal(&format!("kafka: cannot publish on {topic:?}"), error)
            })
    }

    /// Builds the consumer a subscription reads with, positioned and ready.
    async fn consumer(&self, subject: &str, reader: &Reader) -> Result<Arc<Kafka>> {
        let topic = self.topic(subject).into_owned();
        let fail = |error: KafkaError| {
            StreamError::internal(&format!("kafka: cannot consume {subject:?}"), error)
        };
        let mut client = self.inner.client.clone();
        match reader {
            Reader::Tail => {
                client
                    .set("group.id", reader_group())
                    .set("enable.auto.commit", "false");
            }
            Reader::Shared(group) => {
                client
                    .set("group.id", group)
                    .set("enable.auto.commit", "true")
                    .set("auto.offset.reset", "latest");
            }
            Reader::Durable(group, at) => {
                // Offsets are committed when a delivery is acknowledged and
                // not before, so a message taken by a consumer that dies is
                // read again by whoever takes over the partition. The reset
                // applies only to a group with no committed offset yet.
                let reset = match at {
                    Position::New => "latest",
                    Position::Earliest => "earliest",
                };
                client
                    .set("group.id", group)
                    .set("enable.auto.commit", "false")
                    .set("auto.offset.reset", reset);
            }
        }
        let consumer = Arc::new(
            client
                .create_with_context::<_, Kafka>(Start::default())
                .map_err(fail)?,
        );

        if let Reader::Shared(_) = reader {
            // A group member is assigned its partitions whenever the group
            // settles, after this returns, and a partition the group has never
            // committed would then start at whatever the end is by that time,
            // skipping what was published in between. Note the ends now, so
            // such a partition starts where it stood when subscribe was called.
            let (noted, topic) = (Arc::clone(&consumer), topic.clone());
            blocking(move || -> std::result::Result<(), KafkaError> {
                let ends = watermarks(&noted, &topic)?;
                *noted
                    .context()
                    .ends
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner) = ends;
                Ok(())
            })
            .await?
            .map_err(fail)?;
        }
        if !matches!(reader, Reader::Tail) {
            consumer.subscribe(&[&topic]).map_err(fail)?;
            return Ok(consumer);
        }

        // A tail pins every partition at the end it has right now, rather
        // than leaving "latest" to be resolved whenever the first fetch lands,
        // so a publish made after subscribe returns is delivered, not raced.
        let pinned = Arc::clone(&consumer);
        blocking(move || -> std::result::Result<(), KafkaError> {
            let mut assignment = TopicPartitionList::new();
            for (partition, high) in watermarks(&pinned, &topic)? {
                assignment.add_partition_offset(&topic, partition, Offset::Offset(high))?;
            }
            pinned.assign(&assignment)
        })
        .await?
        .map_err(fail)?;
        Ok(consumer)
    }

    /// Delivers under a named consumer: the body of `consume` and
    /// `consume_from`.
    async fn deliveries(
        &self,
        subject: &str,
        consumer: &str,
        at: Position,
        opts: &Options,
    ) -> Result<Deliveries> {
        self.check_subject(subject)?;
        if consumer.is_empty() {
            return Err(StreamError::Unsupported(
                "a durable consumer needs a name, because the name is the position that survives a restart"
                    .into(),
            ));
        }
        if opts.group.is_some() {
            return Err(StreamError::Unsupported(format!(
                "the consumer name {consumer:?} is already the group on Kafka; drop the group option"
            )));
        }

        let reader = self
            .consumer(subject, &Reader::Durable(safe_name(consumer), at))
            .await?;
        tracing::info!(subject, consumer, from = ?at, "consuming");

        let (tx, rx) = mpsc::channel(provider::prefetch(opts).max(1));
        let registry = self.inner.config.registry.clone();
        let metrics = self.inner.config.metrics.clone();
        // Shared, so each delivery points at one copy rather than its own.
        let (subject, name): (Arc<str>, Arc<str>) = (subject.into(), consumer.into());
        let topic = self.topic(&subject).into_owned();
        let commits = Arc::new(Commits::new(topic));
        let committer = tokio::spawn(commit_acked(
            Arc::clone(&reader),
            Arc::clone(&commits),
            subject.to_string(),
        ));
        tokio::spawn(async move {
            let mut delivered = 0usize;
            loop {
                let record = tokio::select! {
                    _ = tx.closed() => break,
                    record = reader.recv() => record,
                };
                let record = match record {
                    Ok(record) => record,
                    Err(error) => {
                        tracing::warn!(subject = %subject, consumer = %name, error = %error,
                            "could not read as the consumer group");
                        tokio::time::sleep(ERROR_BACKOFF).await;
                        continue;
                    }
                };
                let (partition, offset) = (record.partition(), record.offset());
                let frame = Bytes::copy_from_slice(record.payload().unwrap_or_default());
                let headers = received(record.headers(), &subject, offset);
                drop(record);
                commits.delivered(partition, offset);
                let settle = Settle {
                    commits: Arc::clone(&commits),
                    partition,
                    offset,
                    metrics: metrics.clone(),
                    subject: Arc::clone(&subject),
                    name: Arc::clone(&name),
                };
                let message = match provider::unpack_shared(&registry, &subject, frame) {
                    Ok(message) => message.with_headers(headers),
                    Err(error) => {
                        // Nothing will ever decode this, so no handler will ever
                        // acknowledge it and the group would stall on it
                        // forever. Settle it here and say so.
                        tracing::warn!(subject = %subject, offset, error = %error,
                            "committing past a malformed message");
                        let _ = commits.ack(partition, offset);
                        continue;
                    }
                };
                delivered += 1;
                metrics.delivered(&subject, &name);
                // Kafka redelivers the same bytes with no record of having
                // done so, so there is nothing honest to count: the contract
                // asks for zero where a provider cannot.
                if tx
                    .send(Delivery::new(message, 0, Box::new(settle)))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            tracing::info!(subject = %subject, consumer = %name, delivered, "consumer stopped");
            // Commit what was acknowledged before leaving the group, so the
            // next member under this name starts after it.
            commits.close();
            let _ = committer.await;
            // Closing leaves the group, which blocks for a round trip.
            let _ = blocking(move || drop(reader)).await;
        });
        Ok(receive(rx))
    }
}

/// The consumers this module builds.
type Kafka = StreamConsumer<Start>;

/// The current end of every partition of `topic`.
///
/// A topic created a moment ago may not have its leaders elected yet, which
/// the broker reports as an error that clears by itself; those are waited out
/// for up to [`DIAL_TIMEOUT`] rather than failing a subscribe made right
/// after `create`.
fn watermarks(consumer: &Kafka, topic: &str) -> std::result::Result<HashMap<i32, i64>, KafkaError> {
    let deadline = std::time::Instant::now() + DIAL_TIMEOUT;
    loop {
        match try_watermarks(consumer, topic) {
            Err(error)
                if std::time::Instant::now() < deadline
                    && matches!(
                        error.rdkafka_error_code(),
                        Some(
                            RDKafkaErrorCode::NotLeaderForPartition
                                | RDKafkaErrorCode::LeaderNotAvailable
                                | RDKafkaErrorCode::UnknownTopicOrPartition
                                | RDKafkaErrorCode::UnknownPartition
                        )
                    ) =>
            {
                std::thread::sleep(Duration::from_millis(100));
            }
            result => return result,
        }
    }
}

/// One attempt at [`watermarks`].
fn try_watermarks(
    consumer: &Kafka,
    topic: &str,
) -> std::result::Result<HashMap<i32, i64>, KafkaError> {
    let metadata = consumer.fetch_metadata(Some(topic), DIAL_TIMEOUT)?;
    let mut ends = HashMap::new();
    for described in metadata.topics() {
        // A topic whose leaders are not elected yet comes back with an error
        // and no partitions rather than failing the fetch. Raise it, so
        // [`watermarks`] waits it out instead of reporting no partitions.
        if let Some(error) = described.error() {
            return Err(KafkaError::MetadataFetch(error.into()));
        }
        for partition in described.partitions() {
            let (_, high) = consumer.fetch_watermarks(topic, partition.id(), DIAL_TIMEOUT)?;
            ends.insert(partition.id(), high);
        }
    }
    // Every topic has at least one partition, so none means the broker has not
    // caught up with it yet: an empty map would pin a tail to nothing, and it
    // would never deliver.
    if ends.is_empty() {
        return Err(KafkaError::MetadataFetch(
            RDKafkaErrorCode::UnknownTopicOrPartition,
        ));
    }
    Ok(ends)
}

/// The context every consumer here runs with: librdkafka's own rebalancing,
/// except that a group member starts a partition its group has never
/// committed at the end it had when the member subscribed, rather than at
/// whatever the end is once the group settles.
#[derive(Default)]
struct Start {
    /// Partition ends noted at subscribe; empty for every other reader.
    ends: Mutex<HashMap<i32, i64>>,
}

impl ClientContext for Start {}

impl ConsumerContext for Start {
    /// Applies librdkafka's assignment itself, eager or cooperative to match
    /// the group's protocol, so uncommitted partitions can be pointed at their
    /// noted ends first. A failed apply is logged, not raised: there is no caller.
    fn rebalance(
        &self,
        consumer: &BaseConsumer<Self>,
        err: RDKafkaRespErr,
        tpl: &mut TopicPartitionList,
    ) {
        let cooperative = matches!(
            consumer.rebalance_protocol(),
            RebalanceProtocol::Cooperative
        );
        let result = match err {
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => {
                self.start_uncommitted(consumer, tpl);
                if cooperative {
                    consumer.incremental_assign(tpl)
                } else {
                    consumer.assign(tpl)
                }
            }
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS => {
                if cooperative {
                    consumer.incremental_unassign(tpl)
                } else {
                    consumer.unassign()
                }
            }
            other => {
                tracing::warn!(error = ?other, "kafka: rebalance failed");
                consumer.unassign()
            }
        };
        if let Err(error) = result {
            tracing::warn!(error = %error, "kafka: could not apply a rebalance");
        }
    }
}

impl Start {
    /// Points each newly assigned partition that has no committed offset at
    /// its noted end. Partitions with one are left alone, to resume there.
    fn start_uncommitted(&self, consumer: &BaseConsumer<Self>, tpl: &mut TopicPartitionList) {
        let ends = self.ends.lock().unwrap_or_else(PoisonError::into_inner);
        if ends.is_empty() {
            return;
        }
        let committed = match consumer.committed_offsets(tpl.clone(), DIAL_TIMEOUT) {
            Ok(committed) => committed,
            Err(error) => {
                // Falls back to the reset policy, which is what a group
                // without this would have done anyway.
                tracing::warn!(error = %error, "kafka: could not read committed offsets");
                return;
            }
        };
        for entry in committed.elements() {
            if entry.offset() != Offset::Invalid {
                continue;
            }
            if let Some(&end) = ends.get(&entry.partition()) {
                let _ =
                    tpl.set_partition_offset(entry.topic(), entry.partition(), Offset::Offset(end));
            }
        }
    }
}

/// `headers` as Kafka record headers, each value its UTF-8 bytes; none when
/// there are none, so a header-less record is exactly what runtime-go writes.
fn record_headers(headers: &Headers) -> Option<OwnedHeaders> {
    if headers.is_empty() {
        return None;
    }
    Some(headers.iter().fold(
        OwnedHeaders::new_with_capacity(headers.len()),
        |out, (key, value)| {
            out.insert(Header {
                key,
                value: Some(value),
            })
        },
    ))
}

/// The headers a record arrived with.
///
/// Kafka headers are bytes and may repeat a key; this contract's are one
/// string per key. A repeated key takes its last value, as Kafka's own
/// `lastHeader` does, and a null value reads as empty. A header that is not
/// UTF-8 can only come from another producer and cannot be represented, so it
/// is skipped with a warning rather than costing the message its payload.
fn received(
    headers: Option<&impl rdkafka::message::Headers>,
    subject: &str,
    offset: i64,
) -> Headers {
    let mut out = Headers::new();
    let Some(headers) = headers else {
        return out;
    };
    for index in 0..headers.count() {
        // rdkafka unwraps a header key's UTF-8 conversion, so a foreign
        // producer's binary key would panic and end the reading task; caught
        // here it costs only that header.
        let header = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            headers.try_get(index).map(|header| {
                (
                    header.key.to_owned(),
                    header.value.map(<[u8]>::to_vec).unwrap_or_default(),
                )
            })
        }));
        match header {
            Ok(Some((key, value))) => match String::from_utf8(value) {
                Ok(value) => {
                    out.insert(key, value);
                }
                Err(_) => tracing::warn!(subject, offset, key = %key,
                    "skipping a header whose value is not UTF-8"),
            },
            Ok(None) => {}
            Err(_) => tracing::warn!(
                subject,
                offset,
                index,
                "skipping a header whose key is not UTF-8"
            ),
        }
    }
    out
}

/// Turns a channel into the stream a caller holds. Dropping the stream drops
/// the receiver, which is what tells the reading task to stop.
fn receive<T: Send + 'static>(rx: mpsc::Receiver<T>) -> futures::stream::BoxStream<'static, T> {
    futures::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    })
    .boxed()
}

#[async_trait::async_trait]
impl Publisher for KafkaManager {
    /// Appends a payload to the subject's topic and waits for the broker to
    /// acknowledge it.
    ///
    /// The partition key becomes the record key, which decides the partition
    /// and so what is ordered relative to what. Headers become record headers.
    async fn publish(&self, subject: &str, payload: Payload, opts: &Options) -> Result<String> {
        self.check_subject(subject)?;
        Self::check_options(opts)?;
        let headers = record_headers(&provider::outgoing(&self.stream, opts)?);
        let id = provider::publish_id(opts);
        let frame = provider::pack(&id, &payload);
        let topic = self.topic(subject);
        let key = opts.partition_key.as_deref();
        if let Err(error) = Self::send(self.inner.producer(key), &topic, &frame, key, headers).await
        {
            tracing::error!(subject, id = %id, error = %error, "could not publish");
            return Err(error);
        }
        tracing::debug!(subject, id = %id, partition_key = ?opts.partition_key,
            bytes = frame.len(), "published");
        Ok(id)
    }
}

#[async_trait::async_trait]
impl Batch for KafkaManager {
    /// Appends several payloads to the subject's topic in one go.
    ///
    /// This is where Kafka is worth reaching for. [`Publisher::publish`] waits
    /// for each acknowledgement, so a thousand payloads one at a time are a
    /// thousand round trips. Here every record is handed to librdkafka before
    /// any is awaited, which is what lets it accumulate them into the batches
    /// the protocol was designed around.
    async fn publish_batch(
        &self,
        subject: &str,
        payloads: Vec<Payload>,
        opts: &Options,
    ) -> Result<Vec<String>> {
        self.check_subject(subject)?;
        provider::check_batch(opts)?;
        Self::check_options(opts)?;
        let headers = record_headers(&provider::outgoing(&self.stream, opts)?);

        let topic = self.topic(subject);
        let ids: Vec<String> = payloads.iter().map(|_| provider::new_id()).collect();
        let frames: Vec<Vec<u8>> = payloads
            .iter()
            .zip(&ids)
            .map(|(payload, id)| provider::pack(id, payload))
            .collect();
        let key = opts.partition_key.as_deref();
        // One producer for the whole batch, so its records keep their order.
        let producer = self.inner.producer(key);
        let results = join_all(
            frames
                .iter()
                // Every record carries the same headers; librdkafka takes
                // ownership of each record's own copy.
                .map(|frame| Self::send(producer, &topic, frame, key, headers.clone())),
        )
        .await;
        let failures = results
            .into_iter()
            .enumerate()
            .filter_map(|(index, result)| result.err().map(|error| (index, error)))
            .collect();

        tracing::debug!(subject, entries = ids.len(), partition_key = ?opts.partition_key,
            "published a batch");
        provider::batch_result(subject, ids, failures)
    }
}

#[async_trait::async_trait]
impl Subscriber for KafkaManager {
    /// Tails a subject from the end of its log.
    ///
    /// Without a group this is a plain reader with no offsets committed:
    /// nothing about what it saw survives the subscription. With
    /// [`Options::group`] it joins that consumer group, so members share the
    /// subject's partitions; group membership is settled by the broker after
    /// this returns, so the first publishes may land before the group starts
    /// reading. Reach for [`Durable::consume`] when what was seen must
    /// survive.
    async fn subscribe(&self, subject: &str, opts: &Options) -> Result<Subscription> {
        self.check_subject(subject)?;
        let reader = match &opts.group {
            Some(group) => Reader::Shared(safe_name(group)),
            None => Reader::Tail,
        };
        let consumer = self.consumer(subject, &reader).await?;
        tracing::info!(subject, group = ?opts.group, "subscribed");

        let (tx, rx) = mpsc::channel(provider::prefetch(opts).max(1));
        let registry = self.inner.config.registry.clone();
        let subject: Arc<str> = subject.into();
        tokio::spawn(async move {
            let mut delivered = 0usize;
            loop {
                let record = tokio::select! {
                    _ = tx.closed() => break,
                    record = consumer.recv() => record,
                };
                let message: Message = match record {
                    Ok(record) => {
                        let offset = record.offset();
                        let frame = Bytes::copy_from_slice(record.payload().unwrap_or_default());
                        let headers = received(record.headers(), &subject, offset);
                        drop(record);
                        match provider::unpack_shared(&registry, &subject, frame) {
                            Ok(message) => message.with_headers(headers),
                            Err(error) => {
                                tracing::warn!(subject = %subject, offset, error = %error,
                                    "dropping a malformed message");
                                continue;
                            }
                        }
                    }
                    Err(error) => {
                        tracing::warn!(subject = %subject, error = %error, "could not read the topic");
                        tokio::time::sleep(ERROR_BACKOFF).await;
                        continue;
                    }
                };
                delivered += 1;
                if tx.send(message).await.is_err() {
                    break;
                }
            }
            tracing::info!(subject = %subject, delivered, "subscription closed");
            let _ = blocking(move || drop(consumer)).await;
        });
        Ok(receive(rx))
    }
}

#[async_trait::async_trait]
impl Durable for KafkaManager {
    /// Delivers under a named consumer, starting at new messages when the name
    /// has no committed position yet.
    ///
    /// See [`Positioned::consume_from`] for what the name means on Kafka.
    async fn consume(&self, subject: &str, consumer: &str, opts: &Options) -> Result<Deliveries> {
        self.deliveries(subject, consumer, Position::New, opts)
            .await
    }
}

#[async_trait::async_trait]
impl Positioned for KafkaManager {
    /// Delivers under a named consumer, starting at `at`.
    ///
    /// The name is a Kafka consumer group: two processes under one name are
    /// assigned different partitions and split the work, and a process that
    /// comes back resumes from the last offset the name committed. The position
    /// applies only when the group has no committed offset yet.
    ///
    /// # Acknowledging
    ///
    /// Kafka keeps one offset per partition, not one per message, so the
    /// committed offset is the lowest one still unsettled in each partition:
    /// a delivery acknowledged out of order is remembered, but nothing is
    /// committed past an earlier delivery that has not been acknowledged.
    /// A delivery that is naked, or dropped unsettled, holds its partition's
    /// committed offset where it is, so it and everything after it is read
    /// again when the partition is next assigned.
    ///
    /// An ack is recorded at once and committed in the background, one commit
    /// at a time per consumer covering every ack since the last, rather than
    /// one blocking round trip per message. The commit is made before the
    /// consumer leaves its group, so a clean shutdown loses no
    /// acknowledgement; a process that dies between an ack and its commit has
    /// the message redelivered, which is at-least-once doing what it promised.
    async fn consume_from(
        &self,
        subject: &str,
        consumer: &str,
        at: Position,
        opts: &Options,
    ) -> Result<Deliveries> {
        self.deliveries(subject, consumer, at, opts).await
    }
}

impl Manager for KafkaManager {
    /// Kafka offers every optional capability: committed group offsets make a
    /// consumer durable, and a fresh group can start at either end of the log.
    fn durable(&self) -> Option<&dyn Durable> {
        Some(self)
    }

    /// Some: a new group starts at `Earliest` or at the end noted on subscribe.
    fn positioned(&self) -> Option<&dyn Positioned> {
        Some(self)
    }

    /// Some: a batch goes through one producer, keeping its order.
    fn batch(&self) -> Option<&dyn Batch> {
        Some(self)
    }
}

/// What a durable consumer has delivered and its handlers have settled, per
/// partition, and so what it may commit.
struct Commits {
    topic: String,
    ledger: Mutex<Ledger>,
    /// Woken by every ack and by closing; a wake-up while a commit is in
    /// flight is kept, so the next commit covers every ack made meanwhile.
    wake: Notify,
}

#[derive(Default)]
struct Ledger {
    partitions: HashMap<i32, Partition>,
    /// Set once the consumer stops: an ack after that cannot be committed.
    closed: bool,
}

#[derive(Default)]
struct Partition {
    /// Delivered and not yet acknowledged.
    unacked: BTreeSet<i64>,
    /// One past the highest offset delivered.
    next: i64,
    /// The offset last committed, or being committed.
    committed: i64,
}

impl Partition {
    /// The offset that may be committed: the oldest unacknowledged delivery,
    /// or, when everything delivered is acknowledged, the one after the last.
    fn safe(&self) -> i64 {
        self.unacked.first().copied().unwrap_or(self.next)
    }
}

impl Commits {
    /// An empty ledger for `topic`; partitions are added as deliveries arrive.
    fn new(topic: String) -> Self {
        Self {
            topic,
            ledger: Mutex::default(),
            wake: Notify::new(),
        }
    }

    /// Locks the ledger, carrying on past a poisoned lock: every update to it
    /// is a few integer and set operations that a panic cannot leave half-done.
    fn ledger(&self) -> std::sync::MutexGuard<'_, Ledger> {
        self.ledger.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records a delivery as outstanding.
    fn delivered(&self, partition: i32, offset: i64) {
        let mut ledger = self.ledger();
        let part = ledger.partitions.entry(partition).or_default();
        if offset < part.next {
            // The partition was read again from its committed offset, after a
            // rebalance or a reset: what came before it is handled, and what
            // was outstanding from here on is being delivered anew.
            part.unacked.clear();
            part.committed = offset;
        } else if part.unacked.is_empty() && part.next == 0 {
            // First sight of this partition: nothing below this is ours.
            part.committed = offset;
        }
        part.unacked.insert(offset);
        part.next = offset + 1;
    }

    /// Marks a delivery handled, so a commit may pass it. Fails once the
    /// consumer has stopped, since nothing will commit it then.
    fn ack(&self, partition: i32, offset: i64) -> bool {
        let mut ledger = self.ledger();
        if ledger.closed {
            return false;
        }
        if let Some(part) = ledger.partitions.get_mut(&partition) {
            part.unacked.remove(&offset);
        }
        drop(ledger);
        self.wake.notify_one();
        true
    }

    /// Stops taking acks and asks for a last commit.
    fn close(&self) {
        self.ledger().closed = true;
        self.wake.notify_one();
    }

    /// What has become committable since the last commit, marked as
    /// committed, or `None` if nothing has. `owned` limits it to the
    /// partitions this member is still assigned; the rest are forgotten.
    fn take(&self, owned: Option<&TopicPartitionList>) -> (Option<TopicPartitionList>, bool) {
        let mut ledger = self.ledger();
        if let Some(owned) = owned {
            ledger
                .partitions
                .retain(|partition, _| owned.find_partition(&self.topic, *partition).is_some());
        }
        let mut list = TopicPartitionList::new();
        for (partition, part) in &mut ledger.partitions {
            let safe = part.safe();
            if safe > part.committed {
                // Kafka's committed offset is the next one to read.
                let _ = list.add_partition_offset(&self.topic, *partition, Offset::Offset(safe));
                part.committed = safe;
            }
        }
        let list = (list.count() > 0).then_some(list);
        (list, ledger.closed)
    }

    /// After a failed commit, makes the next one try those partitions again.
    fn retry(&self, list: &TopicPartitionList) {
        let mut ledger = self.ledger();
        for entry in list.elements() {
            if let (Some(part), Offset::Offset(offset)) = (
                ledger.partitions.get_mut(&entry.partition()),
                entry.offset(),
            ) && part.committed == offset
            {
                part.committed = -1;
            }
        }
    }
}

/// Commits acknowledged offsets in the background until the consumer stops,
/// then once more.
///
/// One synchronous commit at a time, off the async workers: under load each
/// one carries every ack made while the last was in flight, so commits cost a
/// round trip per batch of acks rather than per message, and they reach the
/// broker in order.
async fn commit_acked(consumer: Arc<Kafka>, commits: Arc<Commits>, subject: String) {
    loop {
        commits.wake.notified().await;
        let (consumer, ledger) = (Arc::clone(&consumer), Arc::clone(&commits));
        let round = blocking(move || {
            // Asking librdkafka for the assignment waits on its own thread.
            let owned = consumer.assignment().ok();
            let (list, closed) = ledger.take(owned.as_ref());
            let failed = list.and_then(|list| {
                consumer
                    .commit(&list, CommitMode::Sync)
                    .err()
                    .map(|error| (list, error))
            });
            (failed, closed)
        })
        .await;
        let Ok((failed, closed)) = round else { break };
        if let Some((list, error)) = failed {
            tracing::warn!(subject = %subject, error = %error,
                "could not commit acknowledged offsets; retrying");
            commits.retry(&list);
            if !closed {
                tokio::time::sleep(ERROR_BACKOFF).await;
                commits.wake.notify_one();
            }
        }
        if closed {
            break;
        }
    }
}

/// Settles one delivery by recording it in its consumer's ledger, from which
/// the committer commits.
///
/// Once the subscription is dropped the consumer leaves the group and its
/// partitions belong to someone else, so an ack then fails rather than
/// committing on behalf of a member that no longer exists, and the message is
/// redelivered as the contract promises.
struct Settle {
    commits: Arc<Commits>,
    partition: i32,
    offset: i64,
    metrics: provider::Metrics,
    subject: Arc<str>,
    name: Arc<str>,
}

#[async_trait::async_trait]
impl Acker for Settle {
    /// Marks the offset settled for the committer to pick up; it does not wait
    /// for the commit. Fails once the consumer has been dropped, since its
    /// partitions may already belong to another member.
    async fn ack(self: Box<Self>) -> Result<()> {
        if !self.commits.ack(self.partition, self.offset) {
            return Err(StreamError::Internal(format!(
                "kafka: cannot acknowledge offset {} on {:?}: its consumer was dropped, \
                 so the message will be redelivered",
                self.offset, self.subject
            )));
        }
        self.metrics.settled(&self.subject, &self.name, "ack");
        Ok(())
    }

    /// Leaves the offset uncommitted, so the group reads this record again when
    /// the partition is next assigned. Kafka has no per-message redelivery, so
    /// there is nothing more truthful to do.
    async fn nak(self: Box<Self>) -> Result<()> {
        self.metrics.settled(&self.subject, &self.name, "nak");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rdkafka::{ClientConfig, message::Headers as _};

    use super::*;
    use crate::drivers::kafka::{KafkaConfig, KafkaProvider};

    const SUBJECT: &str = "user.created";

    /// A manager whose clients point nowhere. librdkafka connects lazily, so
    /// every refusal below is decided before any network I/O.
    fn manager() -> KafkaManager {
        let mut client = ClientConfig::new();
        client.set("bootstrap.servers", "127.0.0.1:1");
        let provider = KafkaProvider::build(client, KafkaConfig::default()).unwrap();
        KafkaManager::new(
            provider.inner,
            Stream {
                id: "s-1".into(),
                subjects: vec![SUBJECT.into()],
                ..Stream::default()
            },
        )
    }

    /// A JSON payload whose contents no test cares about.
    fn payload() -> Payload {
        Payload::json(&"x".to_owned()).unwrap()
    }

    #[tokio::test]
    async fn every_capability_is_present() {
        let manager = manager();
        assert!(crate::as_durable(&manager).is_ok());
        assert!(crate::as_positioned(&manager).is_ok());
        assert!(crate::as_batch(&manager).is_ok());
    }

    #[tokio::test]
    async fn publish_refuses_an_undeclared_subject() {
        let error = manager()
            .publish("typo", payload(), &Options::default())
            .await
            .unwrap_err();
        assert!(
            matches!(error, StreamError::UnknownSubject { .. }),
            "{error}"
        );
    }

    #[tokio::test]
    async fn publish_refuses_a_ttl() {
        let opts = Options::default().with_ttl(Duration::from_secs(1));
        let error = manager()
            .publish(SUBJECT, payload(), &opts)
            .await
            .unwrap_err();
        assert!(matches!(error, StreamError::Unsupported(_)), "{error}");
    }

    #[tokio::test]
    async fn batch_refuses_an_id_ttl_and_undeclared_subject() {
        let manager = manager();
        let batch = |opts: Options, subject: &'static str| {
            let manager = &manager;
            async move {
                manager
                    .publish_batch(subject, vec![payload(), payload()], &opts)
                    .await
            }
        };
        assert!(matches!(
            batch(Options::default().with_id("fixed"), SUBJECT).await,
            Err(StreamError::Unsupported(_))
        ));
        assert!(matches!(
            batch(Options::default().with_ttl(Duration::from_secs(1)), SUBJECT).await,
            Err(StreamError::Unsupported(_))
        ));
        assert!(matches!(
            batch(Options::default(), "typo").await,
            Err(StreamError::UnknownSubject { .. })
        ));
    }

    #[test]
    fn expiry_and_retain_are_refused_by_name() {
        let refused = |opts: Options| match KafkaManager::check_options(&opts) {
            Err(StreamError::Unsupported(why)) => why,
            other => panic!("{other:?}"),
        };
        assert!(refused(Options::default().with_expiry(Duration::from_secs(5))).contains("expiry"));
        assert!(refused(Options::default().with_retain()).contains("compacted"));
        // A zero expiry is none, as a zero TTL is.
        assert!(
            KafkaManager::check_options(&Options::default().with_expiry(Duration::ZERO)).is_ok()
        );
    }

    #[tokio::test]
    async fn publish_and_batch_refuse_expiry_retain_and_bad_headers() {
        let manager = manager();
        for opts in [
            Options::default().with_expiry(Duration::from_secs(5)),
            Options::default().with_retain(),
        ] {
            let error = manager
                .publish(SUBJECT, payload(), &opts)
                .await
                .unwrap_err();
            assert!(matches!(error, StreamError::Unsupported(_)), "{error}");
            let error = manager
                .publish_batch(SUBJECT, vec![payload()], &opts)
                .await
                .unwrap_err();
            assert!(matches!(error, StreamError::Unsupported(_)), "{error}");
        }
        let bad = Options::default().header("lang", "two\nlines");
        let error = manager.publish(SUBJECT, payload(), &bad).await.unwrap_err();
        assert!(matches!(error, StreamError::InvalidHeader(_)), "{error}");
        let error = manager
            .publish_batch(SUBJECT, vec![payload()], &bad)
            .await
            .unwrap_err();
        assert!(matches!(error, StreamError::InvalidHeader(_)), "{error}");
    }

    #[test]
    fn headers_map_to_record_headers_and_back() {
        let sent: Headers = [("lang", "rust"), ("traceparent", "00-ab-cd-01")]
            .into_iter()
            .collect();
        let record = record_headers(&sent).unwrap();
        assert_eq!(record.count(), 2);
        let first = record.get(0);
        assert_eq!((first.key, first.value), ("lang", Some(&b"rust"[..])));
        assert_eq!(received(Some(&record), SUBJECT, 0), sent);
        assert!(
            record_headers(&Headers::new()).is_none(),
            "no headers, no header block"
        );
        assert!(received(None::<&OwnedHeaders>, SUBJECT, 0).is_empty());
    }

    #[test]
    fn a_foreign_record_header_is_read_as_kafka_reads_it() {
        let record = OwnedHeaders::new()
            .insert(Header {
                key: "site",
                value: Some("plant-3"),
            })
            .insert(Header {
                key: "site",
                value: Some("plant-4"),
            })
            .insert(Header::<&str> {
                key: "flag",
                value: None,
            })
            .insert(Header {
                key: "binary",
                value: Some(&[0xff_u8, 0xfe][..]),
            });
        let headers = received(Some(&record), SUBJECT, 0);
        assert_eq!(headers.get("site"), Some("plant-4"), "the last value wins");
        assert_eq!(headers.get("flag"), Some(""), "a null value reads as empty");
        assert!(
            !headers.contains_key("binary"),
            "a non-UTF-8 value is skipped"
        );
    }

    #[tokio::test]
    async fn a_delivery_cannot_delay_its_redelivery() {
        let commits = Arc::new(Commits::new("t".into()));
        commits.delivered(0, 1);
        let delivery = Delivery::new(
            Message::new("id", SUBJECT, "json", Bytes::new()),
            0,
            settle(&commits, 0, 1),
        );
        assert!(!delivery.can_delay_redelivery());
        let error = delivery
            .nak_after(Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(matches!(error, StreamError::Unsupported(_)), "{error}");
    }

    #[tokio::test]
    async fn an_empty_batch_is_not_an_error() {
        let ids = manager()
            .publish_batch(SUBJECT, Vec::new(), &Options::default())
            .await
            .unwrap();
        assert!(ids.is_empty());
    }

    #[tokio::test]
    async fn consume_needs_a_name_and_refuses_a_group() {
        let manager = manager();
        let error = manager
            .consume(SUBJECT, "", &Options::default())
            .await
            .err()
            .unwrap();
        assert!(matches!(error, StreamError::Unsupported(_)), "{error}");
        let opts = Options::default().with_group("other");
        let error = manager
            .consume(SUBJECT, "billing", &opts)
            .await
            .err()
            .unwrap();
        assert!(matches!(error, StreamError::Unsupported(_)), "{error}");
        assert!(error.to_string().contains("billing"), "{error}");
    }

    #[tokio::test]
    async fn subscribe_and_consume_refuse_an_undeclared_subject() {
        let manager = manager();
        let error = manager
            .subscribe("typo", &Options::default())
            .await
            .err()
            .unwrap();
        assert!(
            matches!(error, StreamError::UnknownSubject { .. }),
            "{error}"
        );
        let error = manager
            .consume_from("typo", "billing", Position::Earliest, &Options::default())
            .await
            .err()
            .unwrap();
        assert!(
            matches!(error, StreamError::UnknownSubject { .. }),
            "{error}"
        );
    }

    /// An acker for `offset` on `partition`, as a delivery would carry.
    fn settle(commits: &Arc<Commits>, partition: i32, offset: i64) -> Box<Settle> {
        Box::new(Settle {
            commits: Arc::clone(commits),
            partition,
            offset,
            metrics: provider::Metrics::default(),
            subject: SUBJECT.into(),
            name: "billing".into(),
        })
    }

    /// Takes what the ledger would commit now, as sorted (partition, offset)
    /// pairs, so tests can compare it against a literal.
    fn committed(commits: &Commits) -> Vec<(i32, i64)> {
        let (list, _) = commits.take(None);
        let mut out: Vec<(i32, i64)> = list
            .map(|list| {
                list.elements()
                    .iter()
                    .map(|entry| match entry.offset() {
                        Offset::Offset(offset) => (entry.partition(), offset),
                        other => panic!("unexpected offset {other:?}"),
                    })
                    .collect()
            })
            .unwrap_or_default();
        out.sort_unstable();
        out
    }

    #[tokio::test]
    async fn an_ack_commits_the_next_offset() {
        let commits = Arc::new(Commits::new("t".into()));
        commits.delivered(2, 41);
        settle(&commits, 2, 41).ack().await.unwrap();
        assert_eq!(committed(&commits), [(2, 42)]);
        // Nothing new, nothing to commit.
        assert!(committed(&commits).is_empty());
    }

    #[tokio::test]
    async fn nothing_is_committed_past_an_unacked_delivery() {
        let commits = Arc::new(Commits::new("t".into()));
        for offset in 10..13 {
            commits.delivered(0, offset);
        }
        commits.delivered(1, 5);
        // Out of order: 11 and 12 are handled, 10 is not.
        settle(&commits, 0, 12).ack().await.unwrap();
        settle(&commits, 0, 11).ack().await.unwrap();
        settle(&commits, 1, 5).ack().await.unwrap();
        assert_eq!(committed(&commits), [(1, 6)]);
        // A nak holds the partition where it is.
        settle(&commits, 0, 10).nak().await.unwrap();
        assert!(committed(&commits).is_empty());
        // Reassigned, the partition is read again from 10.
        for offset in 10..13 {
            commits.delivered(0, offset);
            settle(&commits, 0, offset).ack().await.unwrap();
        }
        assert_eq!(committed(&commits), [(0, 13)]);
    }

    #[tokio::test]
    async fn a_rewound_partition_starts_over() {
        let commits = Arc::new(Commits::new("t".into()));
        commits.delivered(0, 7);
        commits.delivered(0, 8);
        // Reassigned and read again from 7: the old 7 and 8 are being
        // redelivered, not lost.
        commits.delivered(0, 7);
        settle(&commits, 0, 7).ack().await.unwrap();
        assert_eq!(committed(&commits), [(0, 8)]);
    }

    #[tokio::test]
    async fn a_failed_commit_is_retried() {
        let commits = Arc::new(Commits::new("t".into()));
        commits.delivered(0, 3);
        settle(&commits, 0, 3).ack().await.unwrap();
        let (list, _) = commits.take(None);
        commits.retry(&list.unwrap());
        assert_eq!(committed(&commits), [(0, 4)]);
    }

    #[tokio::test]
    async fn an_ack_after_the_consumer_is_gone_fails_loudly() {
        let commits = Arc::new(Commits::new("t".into()));
        commits.delivered(0, 7);
        commits.close();
        let error = settle(&commits, 0, 7).ack().await.unwrap_err();
        assert!(error.to_string().contains("redelivered"), "{error}");
    }
}
