//! Stream lifecycle: topics per subject, declarations in a compacted topic.

use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::Instant,
};

use rdkafka::{
    Message as _, Offset, TopicPartitionList,
    admin::{AdminOptions, NewTopic, TopicReplication},
    consumer::{BaseConsumer, Consumer},
    error::KafkaError,
    producer::FutureRecord,
    types::RDKafkaErrorCode,
};

use crate::{Manager, Result, Stream, StreamError, Streams, provider};

use super::{DIAL_TIMEOUT, Inner, KafkaManager, KafkaProvider, blocking, reader_group, safe_name};

impl KafkaProvider {
    /// Creates the metadata topic if it is not there.
    ///
    /// It is compacted rather than time-retained: the newest record for a
    /// stream id is its current declaration, and a declaration must not expire
    /// out from under the topics it describes.
    pub(super) async fn ensure_meta(&self) -> Result<()> {
        let name = self.inner.meta_topic();
        let topic = NewTopic::new(
            &name,
            1,
            TopicReplication::Fixed(self.inner.config.replication_factor),
        )
        .set("cleanup.policy", "compact");
        self.create_topics(&[topic], "the metadata topic").await
    }

    /// Creates one topic per declared subject.
    async fn create_subject_topics(&self, stream: &Stream) -> Result<()> {
        if stream.subjects.is_empty() {
            return Ok(());
        }
        let names: Vec<String> = stream
            .subjects
            .iter()
            .map(|subject| self.inner.topic(&stream.id, subject))
            .collect();
        let config = &self.inner.config;
        let topics: Vec<NewTopic<'_>> = names
            .iter()
            .map(|name| {
                NewTopic::new(
                    name,
                    config.partitions,
                    TopicReplication::Fixed(config.replication_factor),
                )
            })
            .collect();
        self.create_topics(&topics, &format!("topics for stream {}", stream.id))
            .await
    }

    /// Creates topics, treating "already exists" as success: it is the answer
    /// we want from a create that may run more than once, or race another
    /// process making the same declaration.
    async fn create_topics(&self, topics: &[NewTopic<'_>], what: &str) -> Result<()> {
        let opts = AdminOptions::new().operation_timeout(Some(DIAL_TIMEOUT));
        let results = self
            .inner
            .admin
            .create_topics(topics, &opts)
            .await
            .map_err(|error| {
                StreamError::internal(&format!("kafka: cannot create {what}"), error)
            })?;
        for result in results {
            match result {
                Ok(_) | Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
                Err((topic, code)) => {
                    return Err(StreamError::internal(
                        &format!("kafka: cannot create topic {topic}"),
                        code,
                    ));
                }
            }
        }
        Ok(())
    }

    /// Records a declaration, or removes it when `stream` is `None`.
    async fn write_meta(&self, id: &str, stream: Option<&Stream>) -> Result<()> {
        let topic = self.inner.meta_topic();
        let body = stream
            .map(serde_json::to_vec)
            .transpose()
            .map_err(|error| {
                StreamError::internal(&format!("kafka: cannot encode stream {id}"), error)
            })?;
        let mut record = FutureRecord::<str, [u8]>::to(&topic).key(id);
        // No payload is a tombstone, which compaction eventually removes.
        if let Some(body) = &body {
            record = record.payload(body);
        }
        // Keyed by the stream id, so declarations of one stream stay in order.
        self.inner
            .producer(Some(id))
            .send(record, DIAL_TIMEOUT)
            .await
            .map_err(|(error, _)| {
                StreamError::internal(&format!("kafka: cannot record stream {id}"), error)
            })?;
        Ok(())
    }

    /// Folds the metadata topic into the current declarations.
    ///
    /// The topic is read whole on every call rather than cached: a declaration
    /// made by another process has to be visible here, and caching it would
    /// make `get` answer from a snapshot of whenever this process last looked.
    async fn read_meta(&self) -> Result<BTreeMap<String, Stream>> {
        let inner = Arc::clone(&self.inner);
        blocking(move || read_meta_blocking(&inner)).await?
    }
}

/// Reads the metadata topic from the beginning to its current end.
///
/// The end is found with `enable.partition.eof` rather than by counting
/// offsets, because compaction leaves gaps: a log ending at offset 100 may hold
/// ten records, and a reader waiting for a hundred would sit out its timeout.
fn read_meta_blocking(inner: &Inner) -> Result<BTreeMap<String, Stream>> {
    let fail =
        |error: KafkaError| StreamError::internal("kafka: cannot read stream metadata", error);
    let topic = inner.meta_topic();

    let mut client = inner.client.clone();
    // A plain reader, not a group member: nothing about this read should be
    // remembered, and joining a group would leave server state behind per call.
    client
        .set("group.id", reader_group())
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "true");
    let consumer: BaseConsumer = client.create().map_err(fail)?;

    let metadata = consumer
        .fetch_metadata(Some(&topic), DIAL_TIMEOUT)
        .map_err(fail)?;
    let mut assignment = TopicPartitionList::new();
    for described in metadata.topics() {
        if let Some(code) = described.error() {
            return Err(StreamError::internal(
                &format!("kafka: cannot read stream metadata from {topic}"),
                RDKafkaErrorCode::from(code),
            ));
        }
        for partition in described.partitions() {
            assignment
                .add_partition_offset(&topic, partition.id(), Offset::Beginning)
                .map_err(fail)?;
        }
    }
    let partitions = assignment.count();
    consumer.assign(&assignment).map_err(fail)?;

    let deadline = Instant::now() + DIAL_TIMEOUT;
    let mut finished = HashSet::new();
    let mut out = BTreeMap::new();
    while finished.len() < partitions {
        let Some(left) = deadline.checked_duration_since(Instant::now()) else {
            // Go returns whatever it had read by now. An error is more honest:
            // a partial read would turn a stream that exists into NotFound.
            return Err(StreamError::Internal(format!(
                "kafka: stream metadata in {topic} was not read to its end within {DIAL_TIMEOUT:?}"
            )));
        };
        match consumer.poll(left) {
            None => {}
            Some(Err(KafkaError::PartitionEOF(partition))) => {
                finished.insert(partition);
            }
            Some(Err(error)) => return Err(fail(error)),
            Some(Ok(record)) => {
                let id = String::from_utf8_lossy(record.key().unwrap_or_default()).into_owned();
                match record.payload() {
                    None => {
                        out.remove(&id);
                    }
                    Some(body) => match serde_json::from_slice::<Stream>(body) {
                        Ok(stream) => {
                            out.insert(id, stream);
                        }
                        Err(error) => tracing::warn!(
                            id = %id, error = %error,
                            "skipping malformed stream metadata"
                        ),
                    },
                }
            }
        }
    }
    // Closing leaves nothing behind on the server, but it can block for a
    // round trip; this is already off the async workers.
    drop(consumer);
    Ok(out)
}

/// A declaration as stored, from what the caller passed.
fn declaration(id: String, stream: Stream) -> Stream {
    Stream {
        id,
        name: stream.name,
        description: stream.description,
        subjects: stream.subjects,
        user_id: stream.user_id,
        active: true,
        headers: stream.headers,
    }
}

#[async_trait::async_trait]
impl Streams for KafkaProvider {
    /// Declares a stream: its subjects' topics, then its declaration.
    ///
    /// Default headers are checked first, so a bad one fails here rather than
    /// at every publish. Kafka reserves no header names.
    async fn create(&self, stream: Stream) -> Result<Stream> {
        stream.headers.validate()?;
        let id = if stream.id.is_empty() {
            provider::new_stream_id(&stream.name)
        } else {
            stream.id.clone()
        };
        let out = declaration(safe_name(&id), stream);

        // Topics first: a declaration naming subjects that do not exist would
        // let a publish fail against a stream the caller was just told exists.
        self.create_subject_topics(&out).await?;
        self.write_meta(&out.id, Some(&out)).await?;
        tracing::info!(id = %out.id, name = %out.name, subjects = ?out.subjects, "stream created");
        Ok(out)
    }

    async fn get(&self, id: &str) -> Result<Stream> {
        self.read_meta()
            .await?
            .remove(&safe_name(id))
            .ok_or_else(|| StreamError::NotFound(format!("stream {id}")))
    }

    async fn bind(&self, id: &str) -> Result<Arc<dyn Manager>> {
        let stream = self.get(id).await?;
        tracing::debug!(id = %stream.id, subjects = ?stream.subjects, "bound to stream");
        Ok(Arc::new(KafkaManager::new(Arc::clone(&self.inner), stream)))
    }

    /// Replaces a stream's configuration, keeping its id.
    ///
    /// Topics for added subjects are created; topics for dropped subjects are
    /// left in place, because deleting them would discard a log the
    /// declaration never owned exclusively.
    async fn update(&self, id: &str, stream: Stream) -> Result<Stream> {
        stream.headers.validate()?;
        let name = safe_name(id);
        self.get(&name).await?;
        let out = declaration(name, stream);
        self.create_subject_topics(&out).await?;
        self.write_meta(&out.id, Some(&out)).await?;
        tracing::info!(id = %out.id, "stream updated");
        Ok(out)
    }

    /// Removes a stream and the topics carrying its subjects.
    async fn delete(&self, id: &str) -> Result<()> {
        let name = safe_name(id);
        let stream = self.get(&name).await?;
        let topics: Vec<String> = stream
            .subjects
            .iter()
            .map(|subject| self.inner.topic(&name, subject))
            .collect();
        if !topics.is_empty() {
            let topics: Vec<&str> = topics.iter().map(String::as_str).collect();
            let opts = AdminOptions::new().operation_timeout(Some(DIAL_TIMEOUT));
            // Per-topic results are not checked, as in Go: a topic someone
            // already removed is the outcome a delete wanted anyway.
            self.inner
                .admin
                .delete_topics(&topics, &opts)
                .await
                .map_err(|error| {
                    StreamError::internal(
                        &format!("kafka: cannot delete topics for stream {id}"),
                        error,
                    )
                })?;
        }
        self.write_meta(&name, None).await?;
        tracing::info!(id = %name, "stream deleted");
        Ok(())
    }

    async fn list(&self) -> Result<Vec<Stream>> {
        // A BTreeMap is already ordered by id, which is the order Go sorts into.
        let out: Vec<Stream> = self.read_meta().await?.into_values().collect();
        tracing::debug!(count = out.len(), "listed streams");
        Ok(out)
    }

    /// Flushes what the producers still hold. The caller's config is not a
    /// connection, so there is nothing of theirs to close.
    async fn close(&self) -> Result<()> {
        let producers = self.inner.producers.clone();
        blocking(move || {
            use rdkafka::producer::Producer as _;
            producers
                .iter()
                .try_for_each(|producer| producer.flush(DIAL_TIMEOUT))
        })
        .await?
        .map_err(|error| StreamError::internal("kafka: cannot flush the producer", error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_declaration_is_active_and_keeps_its_fields() {
        let out = declaration(
            "s-1".into(),
            Stream {
                id: "ignored".into(),
                name: "users".into(),
                description: "d".into(),
                subjects: vec!["a".into()],
                user_id: "u".into(),
                active: false,
                ..Stream::default()
            },
        );
        assert_eq!(out.id, "s-1");
        assert!(out.active);
        assert_eq!((out.name.as_str(), out.user_id.as_str()), ("users", "u"));
    }

    #[test]
    fn a_declaration_keeps_its_headers_and_leaves_none_out_of_the_json() {
        let with = declaration("s".into(), Stream::named("n").header("lang", "rust"));
        assert_eq!(with.headers.get("lang"), Some("rust"));
        let json = serde_json::to_value(&with).unwrap();
        assert_eq!(json["headers"]["lang"], "rust");
        let back: Stream = serde_json::from_value(json).unwrap();
        assert_eq!(back.headers, with.headers);

        let without = serde_json::to_value(declaration("s".into(), Stream::default())).unwrap();
        assert!(without.get("headers").is_none(), "{without}");
    }

    #[tokio::test]
    async fn create_and_update_refuse_a_bad_default_header() {
        let mut client = rdkafka::ClientConfig::new();
        client.set("bootstrap.servers", "127.0.0.1:1");
        let provider = KafkaProvider::build(client, super::super::KafkaConfig::default()).unwrap();
        let bad = || Stream::named("n").subject("a").header("has space", "x");
        // Refused before any network I/O: the broker here is unreachable.
        let error = provider.create(bad()).await.unwrap_err();
        assert!(matches!(error, StreamError::InvalidHeader(_)), "{error}");
        let error = provider.update("s", bad()).await.unwrap_err();
        assert!(matches!(error, StreamError::InvalidHeader(_)), "{error}");
    }

    #[test]
    fn a_declaration_is_go_shaped_json() {
        let json = serde_json::to_value(declaration("s".into(), Stream::default())).unwrap();
        for key in ["id", "name", "description", "subjects", "user_id", "active"] {
            assert!(json.get(key).is_some(), "{key} missing from {json}");
        }
    }
}
