//! Publishing to and consuming from one bound stream.

use std::{
    sync::{Arc, Weak},
    time::Duration,
};

use bytes::Bytes;
use futures::{StreamExt, future::join_all};
use rdkafka::{
    Message as _, Offset, TopicPartitionList,
    consumer::{CommitMode, Consumer, StreamConsumer},
    error::KafkaError,
    producer::FutureRecord,
};
use tokio::sync::mpsc;

use crate::{
    Acker, Batch, Deliveries, Delivery, Durable, Manager, Message, Options, Payload, Position,
    Positioned, Publisher, Result, Stream, StreamError, Subscriber, Subscription, provider,
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
    pub(super) fn new(inner: Arc<Inner>, stream: Stream) -> Self {
        Self { inner, stream }
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

    /// Refuses a delayed delivery, which Kafka has no way to schedule.
    fn check_ttl(opts: &Options) -> Result<()> {
        if opts.delay().is_some() {
            return Err(StreamError::Unsupported(
                "Kafka delivers when it is read, not on a timer".into(),
            ));
        }
        Ok(())
    }

    /// Appends one framed payload to `topic`, keyed by the partition key.
    async fn send(&self, topic: &str, frame: &[u8], key: Option<&str>) -> Result<()> {
        let mut record = FutureRecord::<str, [u8]>::to(topic).payload(frame);
        if let Some(key) = key {
            record = record.key(key);
        }
        self.inner
            .producer
            .send(record, QUEUE_TIMEOUT)
            .await
            .map(drop)
            .map_err(|(error, _)| {
                StreamError::internal(&format!("kafka: cannot publish on {topic:?}"), error)
            })
    }

    /// Builds the consumer a subscription reads with, positioned and ready.
    async fn consumer(&self, subject: &str, reader: &Reader) -> Result<Arc<StreamConsumer>> {
        let topic = self.inner.topic(&self.stream.id, subject);
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
        let consumer = Arc::new(client.create::<StreamConsumer>().map_err(fail)?);

        if !matches!(reader, Reader::Tail) {
            consumer.subscribe(&[&topic]).map_err(fail)?;
            return Ok(consumer);
        }

        // A tail pins every partition at the end it has right now, rather
        // than leaving "latest" to be resolved whenever the first fetch lands,
        // so a publish made after subscribe returns is delivered, not raced.
        let pinned = Arc::clone(&consumer);
        blocking(move || -> std::result::Result<(), KafkaError> {
            let metadata = pinned.fetch_metadata(Some(&topic), DIAL_TIMEOUT)?;
            let mut assignment = TopicPartitionList::new();
            for described in metadata.topics() {
                for partition in described.partitions() {
                    let (_, high) =
                        pinned.fetch_watermarks(&topic, partition.id(), DIAL_TIMEOUT)?;
                    assignment.add_partition_offset(
                        &topic,
                        partition.id(),
                        Offset::Offset(high),
                    )?;
                }
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
        let (subject, name) = (subject.to_owned(), consumer.to_owned());
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
                let settle = Settle {
                    consumer: Arc::downgrade(&reader),
                    topic: record.topic().to_owned(),
                    partition: record.partition(),
                    offset: record.offset(),
                    metrics: metrics.clone(),
                    subject: subject.clone(),
                    name: name.clone(),
                };
                let frame = Bytes::copy_from_slice(record.payload().unwrap_or_default());
                drop(record);
                let message = match provider::unpack(&registry, &subject, frame) {
                    Ok(message) => message,
                    Err(error) => {
                        // Nothing will ever decode this, so no handler will ever
                        // acknowledge it and the group would stall on it
                        // forever. Commit past it and say so.
                        tracing::warn!(subject = %subject, offset = settle.offset, error = %error,
                            "committing past a malformed message");
                        let _ = reader.commit(&settle.list(), CommitMode::Async);
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
            // Closing leaves the group, which blocks for a round trip.
            let _ = blocking(move || drop(reader)).await;
        });
        Ok(receive(rx))
    }
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
    /// and so what is ordered relative to what.
    async fn publish(&self, subject: &str, payload: Payload, opts: &Options) -> Result<String> {
        self.check_subject(subject)?;
        Self::check_ttl(opts)?;
        let id = provider::publish_id(opts);
        let frame = provider::pack(&id, &payload);
        let topic = self.inner.topic(&self.stream.id, subject);
        if let Err(error) = self
            .send(&topic, &frame, opts.partition_key.as_deref())
            .await
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
        Self::check_ttl(opts)?;

        let topic = self.inner.topic(&self.stream.id, subject);
        let ids: Vec<String> = payloads.iter().map(|_| provider::new_id()).collect();
        let frames: Vec<Vec<u8>> = payloads
            .iter()
            .zip(&ids)
            .map(|(payload, id)| provider::pack(id, payload))
            .collect();
        let key = opts.partition_key.as_deref();
        let results = join_all(frames.iter().map(|frame| self.send(&topic, frame, key))).await;
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
        let subject = subject.to_owned();
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
                        drop(record);
                        match provider::unpack(&registry, &subject, frame) {
                            Ok(message) => message,
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
    /// # Acknowledging is sequential
    ///
    /// Kafka keeps one offset per partition, not one per message, so
    /// acknowledging a delivery marks everything before it in that partition as
    /// handled too. Handlers that acknowledge out of order will mark messages
    /// they never finished; where that matters, process a partition in order,
    /// which is what a partition key is for.
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

/// Settles one delivery by committing its offset.
///
/// It holds the consumer weakly: once the subscription is dropped the consumer
/// has left the group and its partitions belong to someone else, so an ack
/// then fails rather than committing on behalf of a member that no longer
/// exists, and the message is redelivered as the contract promises.
struct Settle {
    consumer: Weak<StreamConsumer>,
    topic: String,
    partition: i32,
    offset: i64,
    metrics: provider::Metrics,
    subject: String,
    name: String,
}

impl Settle {
    /// The commit that marks this record handled: Kafka's committed offset is
    /// the next one to read, not the last one read.
    fn list(&self) -> TopicPartitionList {
        let mut list = TopicPartitionList::new();
        // Only fails for an invalid offset, which a delivered record never has.
        let _ =
            list.add_partition_offset(&self.topic, self.partition, Offset::Offset(self.offset + 1));
        list
    }
}

#[async_trait::async_trait]
impl Acker for Settle {
    async fn ack(self: Box<Self>) -> Result<()> {
        let Some(consumer) = self.consumer.upgrade() else {
            return Err(StreamError::Internal(format!(
                "kafka: cannot acknowledge offset {} on {:?}: its consumer was dropped, \
                 so the message will be redelivered",
                self.offset, self.subject
            )));
        };
        let list = self.list();
        blocking(move || consumer.commit(&list, CommitMode::Sync))
            .await?
            .map_err(|error| {
                StreamError::internal(
                    &format!(
                        "kafka: cannot acknowledge offset {} on {:?}",
                        self.offset, self.subject
                    ),
                    error,
                )
            })?;
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
    use rdkafka::ClientConfig;

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

    #[test]
    fn an_ack_commits_the_next_offset() {
        let settle = Settle {
            consumer: Weak::new(),
            topic: "t".into(),
            partition: 2,
            offset: 41,
            metrics: provider::Metrics::default(),
            subject: SUBJECT.into(),
            name: "billing".into(),
        };
        let list = settle.list();
        let entry = list.find_partition("t", 2).unwrap();
        assert_eq!(entry.offset(), Offset::Offset(42));
    }

    #[tokio::test]
    async fn an_ack_after_the_consumer_is_gone_fails_loudly() {
        let settle = Box::new(Settle {
            consumer: Weak::new(),
            topic: "t".into(),
            partition: 0,
            offset: 7,
            metrics: provider::Metrics::default(),
            subject: SUBJECT.into(),
            name: "billing".into(),
        });
        let error = settle.ack().await.unwrap_err();
        assert!(error.to_string().contains("redelivered"), "{error}");
    }
}
