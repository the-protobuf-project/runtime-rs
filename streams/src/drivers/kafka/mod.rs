//! Streams over Apache Kafka, through librdkafka.
//!
//! Kafka is a stored log with consumer groups, so it satisfies the whole
//! contract: [`Publisher`](crate::Publisher), [`Subscriber`](crate::Subscriber),
//! [`Durable`](crate::Durable), [`Positioned`](crate::Positioned), and a native
//! [`Batch`](crate::Batch). It is also the only provider for which
//! [`Options::partition_key`](crate::Options::partition_key) does anything:
//! Kafka orders within a partition and nowhere else, so two messages that must
//! be seen in order need the same key.
//!
//! # This provider builds its own clients
//!
//! The other providers take a client you built and never close it. This one
//! takes the [`ClientConfig`] you would have built one from, because a Kafka
//! consumer group is chosen when a consumer is constructed rather than when it
//! reads: every named consumer needs a connection of its own. Handing this
//! module one consumer would mean it could never honor a second
//! [`Durable::consume`](crate::Durable::consume). The config is still yours,
//! TLS, SASL, and timeouts included; it is cloned into every client made here
//! and never changed, except that the producer's copy gets `acks=all` and
//! `enable.idempotence=true` when the config does not say otherwise, as
//! runtime-go's producer has.
//!
//! # A stream is a group of topics
//!
//! A stream declares subjects; each becomes its own Kafka topic, named
//! `<prefix>.<stream-id>.<subject>`. One topic per subject rather than one per
//! stream, because a consumer group's offsets are per topic: sharing a topic
//! across subjects would make a group that wants one subject read, and commit,
//! all of them.
//!
//! The stream's own metadata lives in a compacted topic alongside them, since
//! Kafka has nowhere on a topic to hang a description or a user id. Topic
//! names, the metadata topic, its record keys, and its JSON are runtime-go's,
//! so a Go and a Rust service declare and read the same streams on one cluster.
//!
//! ```no_run
//! # async fn example() -> streams::Result<()> {
//! use rdkafka::ClientConfig;
//! use streams::{Streams, drivers::kafka::{KafkaConfig, KafkaProvider}};
//!
//! let mut client = ClientConfig::new();
//! client.set("bootstrap.servers", "localhost:9092");
//! let provider = KafkaProvider::connect(client, KafkaConfig {
//!     prefix: "example".into(),
//!     ..KafkaConfig::default()
//! }).await?;
//! let stream = provider.create(streams::Stream {
//!     name: "orders".into(),
//!     subjects: vec!["order.placed".into()],
//!     ..Default::default()
//! }).await?;
//! let manager = provider.bind(&stream.id).await?;
//! # Ok(()) }
//! ```

mod manager;
mod stream;

use std::{sync::Arc, time::Duration};

use rdkafka::{
    ClientConfig,
    admin::AdminClient,
    client::DefaultClientContext,
    producer::{FutureProducer, Producer},
};

use crate::{Registry, Result, StreamError, provider::Metrics};

pub use manager::KafkaManager;
pub use rdkafka;

/// Bounds the startup reachability check and every metadata read, so an
/// unreachable broker fails [`KafkaProvider::connect`] instead of hanging it.
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);

/// Names the compacted topic holding stream declarations, after the prefix.
const META_SUFFIX: &str = "_streams_meta";

/// What a [`KafkaProvider`] needs beyond the client config: naming, topic
/// shape, and where its measurements go.
#[derive(Clone)]
pub struct KafkaConfig {
    /// Namespaces every topic these streams create and read.
    ///
    /// Use it to run several independent sets of streams against one cluster,
    /// or to share a cluster with another concern. Empty means no namespace.
    pub prefix: String,

    /// How many partitions a subject's topic is created with. Defaults to 1.
    ///
    /// Partitions are the unit of parallelism and of ordering: a consumer group
    /// can have at most one reader per partition, and Kafka orders within one
    /// and nowhere else. One partition means total ordering and a single
    /// reader; more means more readers and ordering only among messages sharing
    /// a partition key.
    pub partitions: i32,

    /// How many brokers hold a copy of each topic. Defaults to 1, which is
    /// right for a single-broker cluster and wrong for anything you would miss.
    pub replication_factor: i32,

    /// The codec names this program decodes. A frame naming any other codec
    /// is refused when it arrives rather than handed over unreadable.
    pub registry: Registry,

    /// Delivery instruments for durable consumers; the default records nothing.
    pub metrics: Metrics,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            prefix: String::new(),
            partitions: 1,
            replication_factor: 1,
            registry: Registry::new(),
            metrics: Metrics::default(),
        }
    }
}

/// [`Streams`](crate::Streams) backed by Kafka.
///
/// Cloning is cheap and shares the producer. [`Streams::close`](crate::Streams::close)
/// flushes what the producer still holds; consumers end when their
/// subscription is dropped.
#[derive(Clone)]
pub struct KafkaProvider {
    inner: Arc<Inner>,
}

/// State every manager and subscription shares.
pub(crate) struct Inner {
    /// The caller's config, cloned into every consumer built here.
    client: ClientConfig,
    producer: FutureProducer,
    admin: AdminClient<DefaultClientContext>,
    config: KafkaConfig,
}

impl KafkaProvider {
    /// Builds a provider from `client` and checks the cluster is there.
    ///
    /// It fails rather than returning a provider when the cluster is
    /// unreachable: a misconfigured broker list is worth hearing about at
    /// startup rather than at the first publish. It also creates the metadata
    /// topic if no process has yet.
    pub async fn connect(client: ClientConfig, config: KafkaConfig) -> Result<Self> {
        let provider = Self::build(client, config)?;
        let producer = provider.inner.producer.clone();
        let servers = provider.inner.servers();
        blocking(move || producer.client().fetch_metadata(None, DIAL_TIMEOUT))
            .await?
            .map_err(|error| {
                StreamError::internal(&format!("kafka: cannot reach {servers}"), error)
            })?;
        provider.ensure_meta().await?;
        Ok(provider)
    }

    /// Builds the clients without touching the network. librdkafka connects
    /// lazily, so nothing here can tell a good broker list from a bad one.
    fn build(client: ClientConfig, config: KafkaConfig) -> Result<Self> {
        if client.get("bootstrap.servers").is_none_or(str::is_empty) {
            return Err(StreamError::Internal(
                "kafka: no seed brokers given; set bootstrap.servers on the client config".into(),
            ));
        }
        let producer = producer_config(&client)
            .create::<FutureProducer>()
            .map_err(|error| StreamError::internal("kafka: cannot build the producer", error))?;
        let admin = client
            .create::<AdminClient<DefaultClientContext>>()
            .map_err(|error| {
                StreamError::internal("kafka: cannot build the admin client", error)
            })?;
        Ok(Self {
            inner: Arc::new(Inner {
                client,
                producer,
                admin,
                config,
            }),
        })
    }
}

impl Inner {
    /// The broker list, for error messages.
    fn servers(&self) -> String {
        self.client
            .get("bootstrap.servers")
            .unwrap_or_default()
            .to_owned()
    }

    /// Where stream declarations live.
    fn meta_topic(&self) -> String {
        meta_topic(&self.config.prefix)
    }

    /// The topic carrying one subject's messages.
    fn topic(&self, stream: &str, subject: &str) -> String {
        topic(&self.config.prefix, stream, subject)
    }
}

/// The producer's config: the caller's, with durable delivery filled in where
/// the caller left it unsaid.
///
/// A publish that returned an id has to mean the record is in the log, once.
/// `acks=all` waits for every in-sync replica rather than the leader alone, and
/// idempotence stops a retry after a lost acknowledgement from appending the
/// record twice. runtime-go's client (franz-go) does both by default and
/// librdkafka does not enable idempotence, so without this a Rust publisher
/// would be weaker than a Go one on the same cluster. A caller who sets either
/// key keeps their value; only the producer's copy is changed, since a consumer
/// warns about producer-only keys.
fn producer_config(client: &ClientConfig) -> ClientConfig {
    let mut producer = client.clone();
    for (key, value) in [("acks", "all"), ("enable.idempotence", "true")] {
        if producer.get(key).is_none() {
            producer.set(key, value);
        }
    }
    producer
}

/// The metadata topic under `prefix`, as runtime-go names it.
fn meta_topic(prefix: &str) -> String {
    if prefix.is_empty() {
        META_SUFFIX.trim_start_matches('_').to_owned()
    } else {
        safe_name(prefix) + META_SUFFIX
    }
}

/// The topic for `subject` on `stream` under `prefix`, as runtime-go names it.
fn topic(prefix: &str, stream: &str, subject: &str) -> String {
    if prefix.is_empty() {
        safe_name(&format!("{stream}.{subject}"))
    } else {
        safe_name(&format!("{prefix}.{stream}.{subject}"))
    }
}

/// Makes an id or subject usable as part of a Kafka topic or group name.
///
/// Kafka allows letters, digits, dot, dash, and underscore; a generated id
/// carries the caller's stream name for readability, which may contain
/// anything. Every other character becomes a dash, rune for rune, as in Go.
fn safe_name(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '-' | '_' => c,
            _ => '-',
        })
        .collect()
}

/// A group id for a consumer that only ever assigns partitions itself.
///
/// librdkafka refuses `assign` from a consumer with no `group.id`, yet a
/// consumer that assigns rather than subscribes never joins its group and,
/// never committing, stores nothing under it. A fresh id per reader keeps two
/// readers from ever being mistaken for one, should anything look.
fn reader_group() -> String {
    format!("streams-reader-{}", crate::provider::new_id())
}

/// Runs a blocking librdkafka call off the async workers.
///
/// Metadata reads, synchronous commits, and closing a consumer all block a
/// thread for up to a network round trip or a group leave, which would stall
/// every other task sharing the worker.
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|error| StreamError::internal("kafka: a blocking call did not finish", error))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_name_matches_go() {
        assert_eq!(safe_name("01J:orders/eu"), "01J-orders-eu");
        assert_eq!(safe_name("a.b-c_D9"), "a.b-c_D9");
        // One dash per character, not per byte, as Go's strings.Map does.
        assert_eq!(safe_name("café"), "caf-");
    }

    #[test]
    fn topics_are_named_as_go_names_them() {
        assert_eq!(
            topic("", "01J:orders", "order.placed"),
            "01J-orders.order.placed"
        );
        assert_eq!(topic("ex", "s1", "a.b"), "ex.s1.a.b");
        assert_eq!(meta_topic(""), "streams_meta");
        assert_eq!(meta_topic("my app"), "my-app_streams_meta");
    }

    #[test]
    fn build_needs_seed_brokers() {
        let error = KafkaProvider::build(ClientConfig::new(), KafkaConfig::default())
            .err()
            .unwrap();
        assert!(error.to_string().contains("bootstrap.servers"), "{error}");
    }

    #[test]
    fn the_producer_is_durable_unless_told_otherwise() {
        let mut client = ClientConfig::new();
        client.set("bootstrap.servers", "127.0.0.1:1");
        let producer = producer_config(&client);
        assert_eq!(producer.get("acks"), Some("all"));
        assert_eq!(producer.get("enable.idempotence"), Some("true"));
        // The caller's own config is left alone, for the consumers built from it.
        assert_eq!(client.get("acks"), None);

        client.set("acks", "1");
        assert_eq!(producer_config(&client).get("acks"), Some("1"));
    }

    #[test]
    fn config_defaults_to_one_partition_and_replica() {
        let config = KafkaConfig::default();
        assert_eq!((config.partitions, config.replication_factor), (1, 1));
        assert!(config.prefix.is_empty());
    }
}
