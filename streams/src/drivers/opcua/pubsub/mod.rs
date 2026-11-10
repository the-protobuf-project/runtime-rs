//! Streams over OPC UA PubSub (Part 14) on an MQTT broker, JSON message
//! mapping.
//!
//! Where client/server reads and writes a server's nodes, PubSub has no
//! server: a publisher sends DataSets, the values of a set of named fields,
//! to a broker, and any number of readers take them from there. The values
//! are OPC UA's own typed DataValues, so a SCADA system, a historian, or
//! Microsoft's OPC Publisher on the same broker reads what this provider
//! writes, and this provider reads what they write.
//!
//! # The mapping
//!
//! | Contract | OPC UA PubSub |
//! | --- | --- |
//! | provider | a PubSub connection: one [`PublisherId`](PubSubConfig::publisher_id) on one broker |
//! | stream | a WriterGroup: id `<group>` on this publisher, or `<publisher>/<group>` to read another's |
//! | subject `<writer>` | a DataSetWriter, all of its fields |
//! | subject `<writer>/<field>` | one field of that writer's DataSet |
//! | [`publish`](crate::Publisher::publish) | one DataSetMessage in one NetworkMessage, at QoS 1, returning after the PUBACK |
//! | [`publish_batch`](crate::Batch::publish_batch) | several DataSetMessages in one NetworkMessage |
//! | [`subscribe`](crate::Subscriber::subscribe) | an MQTT subscription to the writer's topics; one message per field |
//! | payload | a [`DataValue`](opcua::types::DataValue): [`OpcUaValue`](super::OpcUaValue) for a field, [`DataSet`] for a writer |
//!
//! **Topics** follow Part 14's MQTT mapping (7.3.5):
//!
//! ```text
//! <prefix>/json/data/<PublisherId>/<WriterGroup>/<DataSetWriter>      data, not retained
//! <prefix>/json/metadata/<PublisherId>/<WriterGroup>/<DataSetWriter>  ua-metadata, retained
//! ```
//!
//! with `opcua` as the prefix unless [`PubSubConfig::prefix`] says
//! otherwise. A subscription also listens on the writer-less
//! `.../<WriterGroup>` topic, which Part 14 lets a publisher use for every
//! writer of a group, and keeps only its own writer's DataSetMessages there.
//!
//! **Messages** are Part 14 JSON (7.2.5): a NetworkMessage with MessageType
//! `ua-data`, MessageId, PublisherId and WriterGroupName, holding
//! DataSetMessages with DataSetWriterId, DataSetWriterName, SequenceNumber,
//! Timestamp, MetaDataVersion once metadata is published, and a Payload of
//! DataValues keyed by field name, each in Part 6's reversible JSON so its
//! built-in type survives. [`NetworkMessage`] encodes and decodes them on
//! their own. UADP, the binary mapping, is not implemented. A DataSetWriter's
//! numeric id is a 16-bit FNV-1a hash of its name, so two processes
//! declaring the same writer agree without coordination; two names in one
//! stream that hash alike are refused at `create`.
//!
//! **Publish** to `<writer>/<field>` takes an [`OpcUaValue`](super::OpcUaValue)
//! payload and sends a DataSet of that one field; publish to `<writer>`
//! takes a [`DataSet`] and sends all of its fields in one DataSetMessage.
//! The id returned is the NetworkMessage's MessageId:
//! [`Options::id`](crate::Options::id) or a new ULID, and unlike client/server
//! it travels. A batch's entries are `<MessageId>.<n>`, n counting the
//! DataSetMessages of one NetworkMessage; a batch longer than
//! [`PubSubConfig::max_dataset_messages`] goes out as several.
//!
//! **Subscribe** delivers every field of every DataSetMessage of the writer
//! as its own [`Message`](crate::Message), on subject `<writer>/<field>`,
//! codec [`CODEC_NAME`](super::CODEC_NAME), data the field's DataValue in OPC
//! UA binary: the same bytes a client/server subscriber receives, decoded
//! the same way, with [`OpcUaValue::data_value`](super::OpcUaValue::data_value).
//! The fields of one DataSetMessage share its id. A subscription to
//! `<writer>/<field>` delivers that field only. Retained data messages, which
//! were not published after the subscription, are skipped, as are
//! keep-alives.
//!
//! # Headers and expiry
//!
//! Headers ([`Stream::headers`](crate::Stream::headers) and
//! [`Options::headers`](crate::Options::headers)) travel as MQTT 5 user
//! properties on the PUBLISH that carries the NetworkMessage, beside its
//! `application/json` content type; the Part 14 JSON is exactly as it would
//! be without them, so another Part 14 reader is unaffected, and an MQTT 5
//! tool sees them. Every message a subscription takes from that
//! NetworkMessage carries them as [`Message::headers`](crate::Message::headers),
//! as it does the user properties another publisher set. A batch's
//! NetworkMessages all carry the same headers. The stream's own headers are
//! stamped on its `ua-metadata` messages too.
//!
//! [`Options::with_expiry`](crate::Options::with_expiry) is the MQTT 5
//! Message Expiry Interval on the data NetworkMessage, rounded up to whole
//! seconds as on the MQTT driver: a value nobody was there to read in time
//! is dropped by the broker rather than delivered stale.
//! [`Options::with_retain`](crate::Options::with_retain) is refused: Part 14
//! retains `ua-metadata`, not `ua-data`, and subscribers skip retained data
//! as not published after they joined.
//!
//! # Sequence numbers
//!
//! Each DataSetWriter numbers its DataSetMessages, counting up from 1 in this
//! provider. A subscriber tracks the last number per publisher and writer: a
//! jump forward is a gap, logged as a warning, counted in
//! [`PubSubProvider::sequence_gaps`], and added to the
//! `streams_opcua_pubsub_sequence_gaps_total` counter when a
//! [`meter`](PubSubConfig::meter) is given. A repeat of the last number is a
//! QoS 1 redelivery and is dropped; a jump back is a publisher that restarted,
//! and counting starts again there.
//!
//! # Metadata
//!
//! [`PubSubProvider::publish_metadata`] announces a writer's
//! [`DataSetMetaDataType`] as a retained `ua-metadata` message, so a reader
//! that arrives later still learns the fields' names and types;
//! [`DataSet::metadata`] builds one from a DataSet's values. From then on the
//! writer's DataSetMessages carry its ConfigurationVersion as
//! MetaDataVersion. [`PubSubProvider::metadata`] reads one back.
//!
//! # Refused, and allowed
//!
//! - [`Durable`](crate::Durable) is refused. Part 14 is fire-and-forget at the
//!   application layer: a DataSetReader keeps no position and acknowledges
//!   nothing. An MQTT persistent session could queue NetworkMessages, but one
//!   acknowledgement would settle every field and DataSetMessage of one, so
//!   [`Delivery::ack`](crate::Delivery::ack) on a field would settle its
//!   siblings and a nak could not return one. The MQTT driver's durable
//!   consumer is the honest tool for queued delivery.
//! - [`Positioned`](crate::Positioned) is refused: there is no log.
//! - [`Options::ttl`](crate::Options::ttl) is refused: MQTT cannot schedule.
//! - Publishing on a stream of another PublisherId is refused: it would be
//!   sending as someone else.
//! - [`Options::group`](crate::Options::group) is allowed and is an MQTT shared
//!   subscription, `$share/<group>/...`: the broker hands each NetworkMessage
//!   to one member. Part 14 does not define it, but nothing on the wire
//!   changes and splitting a heavy writer among workers is useful. Each member
//!   sees a share of the sequence, so gap tracking is off for a group.
//!
//! [`Options::partition_key`](crate::Options::partition_key) is ignored.
//!
//! # The broker
//!
//! The provider dials as the MQTT driver does, from an
//! [`MqttOptions`] the caller owns (host, TLS, credentials), and with that
//! driver's handshake: one connection per subscription, and
//! [`publish_connections`](PubSubConfig::publish_connections) for publishing.
//! Declarations are local, as on MQTT.

mod dataset;
mod manager;
mod message;

use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Mutex, MutexGuard, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use tpp_telemetry::opentelemetry::metrics::{Counter, Meter};
use rumqttc::v5::{
    Event, Incoming,
    mqttbytes::v5::{Filter, PublishProperties},
};

pub use opcua::types::{ConfigurationVersionDataType, DataSetMetaDataType, FieldMetaData};

pub use crate::drivers::mqtt::{MqttOptions, QoS};
use crate::{
    Headers, Manager, Result, Stream, StreamError, Streams,
    drivers::mqtt::{LINGER, PublishPool, Session, publish_properties, safe_name, subscribe},
    provider,
};

pub use dataset::{DATASET_CODEC_NAME, DataSet};
pub use message::{
    DataSetMessage, MESSAGE_TYPE_DATA, MESSAGE_TYPE_KEEPALIVE, MESSAGE_TYPE_METADATA,
    MetaDataMessage, NetworkMessage,
};

use manager::PubSubManager;

/// Part 14 JSON messages are JSON (7.3.5.2): the MQTT 5 content type says so.
const CONTENT_TYPE: &str = "application/json";

/// The PUBLISH properties of a Part 14 message: JSON's content type and
/// payload format, `headers` as MQTT 5 user properties, and `expires` as the
/// Message Expiry Interval. The Part 14 JSON itself is never touched.
pub(crate) fn properties(headers: Headers, expires: Option<Duration>) -> PublishProperties {
    let mut properties = publish_properties(headers, expires).unwrap_or_default();
    properties.payload_format_indicator = Some(1);
    properties.content_type = Some(CONTENT_TYPE.into());
    properties
}

/// The topic prefix Part 14 uses when none is configured.
pub const DEFAULT_PREFIX: &str = "opcua";

/// DataSetMessages one NetworkMessage carries at most when
/// [`PubSubConfig::max_dataset_messages`] is not changed.
pub const DEFAULT_MAX_DATASET_MESSAGES: usize = 100;

/// Settings for a [`PubSubProvider`].
#[derive(Clone)]
pub struct PubSubConfig {
    /// The PublisherId every message is sent under, and the topic level that
    /// names it. The default is `runtime-rs-<ULID>`, a new one per config;
    /// set a stable one so readers can find this publisher after a restart.
    pub publisher_id: String,

    /// The first topic level. Part 14's is `opcua`.
    pub prefix: String,

    /// The quality of service of every publish and subscription. Defaults to
    /// at-least-once, so a publish returns once the broker has the message.
    pub qos: QoS,

    /// The client id of the first publishing connection; the others append
    /// `-1`, `-2`, .... `None` generates one.
    pub client_id: Option<String>,

    /// How long a publish waits for the broker's acknowledgement.
    pub publish_timeout: Duration,

    /// How many connections publishes are spread over. At least one. A topic,
    /// and so a writer, always takes the same one, which keeps its messages
    /// in sequence order on the wire.
    pub publish_connections: usize,

    /// The most DataSetMessages one NetworkMessage of a batch carries; a
    /// longer batch is sent as several. Bounded so a batch stays under the
    /// broker's and readers' packet size limits.
    pub max_dataset_messages: usize,

    /// How long [`PubSubProvider::metadata`] waits for the retained message.
    pub metadata_timeout: Duration,

    /// Where the sequence-gap counter is recorded. `None` records nothing.
    pub meter: Option<Meter>,
}

impl Default for PubSubConfig {
    /// Each call draws a fresh random PublisherId, so set one for a stable identity.
    fn default() -> Self {
        Self {
            publisher_id: format!("runtime-rs-{}", provider::new_id().to_lowercase()),
            prefix: DEFAULT_PREFIX.into(),
            qos: QoS::AtLeastOnce,
            client_id: None,
            publish_timeout: Duration::from_secs(10),
            publish_connections: 4,
            max_dataset_messages: DEFAULT_MAX_DATASET_MESSAGES,
            metadata_timeout: Duration::from_secs(5),
            meter: None,
        }
    }
}

/// Streams over OPC UA PubSub on an MQTT broker.
///
/// Built with [`PubSubProvider::connect`]. Cloning is cheap and shares the
/// publishing connections, the declarations, and the sequence counters.
#[derive(Clone)]
pub struct PubSubProvider {
    inner: Arc<Inner>,
}

/// A writer of one of this provider's WriterGroups, as (group, writer).
///
/// No publisher in the key because a provider publishes under exactly one
/// PublisherId: a stream naming another publisher is read-only, and both
/// `publish` and `publish_metadata` refuse it (`check_own`), so only this
/// provider's own writers ever take a SequenceNumber or a version.
type WriterKey = (String, String);

/// What every bound manager shares with the provider.
pub(crate) struct Inner {
    options: MqttOptions,
    config: PubSubConfig,
    links: PublishPool,
    declared: RwLock<BTreeMap<String, Stream>>,
    /// The last SequenceNumber each of this publisher's writers sent.
    sequences: Mutex<HashMap<WriterKey, u32>>,
    /// The ConfigurationVersion each writer's published metadata carries.
    versions: Mutex<HashMap<WriterKey, ConfigurationVersionDataType>>,
    gaps: AtomicU64,
    gap_counter: Option<Counter<u64>>,
}

/// The WriterGroup a stream is, and whose it is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Group {
    publisher: String,
    group: String,
    /// Whether this provider is the group's publisher.
    own: bool,
}

/// Locks `mutex`, recovering from poison: the guarded maps are updated entry by entry.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

impl PubSubProvider {
    /// Connects the publishing connections to the broker `options`
    /// describes. `options` is a template, as for
    /// [`MqttProvider`](crate::drivers::mqtt::MqttProvider): its client id
    /// and session settings are replaced per connection.
    pub async fn connect(options: MqttOptions, config: PubSubConfig) -> Result<Self> {
        if options.broker_address().0.is_empty() {
            return Err(StreamError::Internal(
                "opcua pubsub: no broker address given".into(),
            ));
        }
        check_level("the PublisherId", &config.publisher_id)?;
        if config.prefix.is_empty() || config.prefix.contains(['+', '#']) {
            return Err(StreamError::Unsupported(format!(
                "opcua pubsub: the topic prefix {:?} must be non-empty and free of wildcards",
                config.prefix
            )));
        }
        let client_id = config
            .client_id
            .clone()
            .unwrap_or_else(|| format!("runtime-rs-pubsub-{}", provider::new_id()));
        let links = PublishPool::connect(
            &options,
            &client_id,
            config.publish_connections,
            config.publish_timeout,
        )
        .await?;
        let gap_counter = config.meter.as_ref().map(|meter| {
            meter
                .u64_counter("streams_opcua_pubsub_sequence_gaps_total")
                .with_description("DataSetMessages a subscriber missed, by SequenceNumber")
                .with_unit("1")
                .build()
        });
        Ok(Self {
            inner: Arc::new(Inner {
                options,
                config,
                links,
                declared: RwLock::new(BTreeMap::new()),
                sequences: Mutex::default(),
                versions: Mutex::default(),
                gaps: AtomicU64::new(0),
                gap_counter,
            }),
        })
    }

    /// The PublisherId this provider sends under.
    pub fn publisher_id(&self) -> &str {
        &self.inner.config.publisher_id
    }

    /// DataSetMessages this provider's subscriptions found missing from a
    /// writer's sequence, since it was built.
    pub fn sequence_gaps(&self) -> u64 {
        self.inner.gaps.load(Ordering::Relaxed)
    }

    /// The topic a stream's writer publishes data on.
    pub fn data_topic(&self, stream: &str, writer: &str) -> Result<String> {
        let group = self.inner.group_of(&self.declared(stream)?.id);
        Ok(self.inner.topic("data", &group, Some(writer)))
    }

    /// The stored declaration for `id`, or [`StreamError::NotFound`].
    fn declared(&self, id: &str) -> Result<Stream> {
        self.inner
            .read()
            .get(id)
            .cloned()
            .ok_or_else(|| not_found(id))
    }

    /// Announces a writer's DataSetMetaData as a retained `ua-metadata`
    /// message on its metadata topic, and stamps the writer's DataSetMessages
    /// with its ConfigurationVersion from now on. A version of zero is
    /// replaced with the current time, as Part 14's VersionTime counts it.
    pub async fn publish_metadata(
        &self,
        stream: &str,
        writer: &str,
        mut meta_data: DataSetMetaDataType,
    ) -> Result<ConfigurationVersionDataType> {
        let declared = self.declared(stream)?;
        let group = self.inner.group_of(&declared.id);
        manager::check_own(&declared.id, &group)?;
        if !declared
            .subjects
            .iter()
            .any(|s| s == writer || manager::split(s).0 == writer)
        {
            return Err(provider::unknown_subject(
                &declared.id,
                writer,
                &declared.subjects,
            ));
        }
        if meta_data.configuration_version.major_version == 0 {
            let now = version_time();
            meta_data.configuration_version = ConfigurationVersionDataType {
                major_version: now,
                minor_version: now,
            };
        }
        let version = meta_data.configuration_version.clone();
        let message = MetaDataMessage {
            message_id: Some(provider::new_id()),
            publisher_id: Some(group.publisher.clone()),
            data_set_writer_id: Some(writer_id(writer)),
            writer_group_name: Some(group.group.clone()),
            data_set_writer_name: Some(writer.to_owned()),
            timestamp: Some(opcua::types::DateTime::now()),
            meta_data,
        };
        let topic = self.inner.topic("metadata", &group, Some(writer));
        self.inner
            .links
            .link(&topic)
            .send(
                &topic,
                self.inner.config.qos,
                true,
                Some(properties(declared.headers.clone(), None)),
                message.to_json()?.into(),
            )
            .await
            .map_err(|error| {
                StreamError::internal(
                    &format!("opcua pubsub: cannot publish metadata on {topic:?}"),
                    error,
                )
            })?;
        lock(&self.inner.versions).insert((group.group, writer.to_owned()), version.clone());
        tracing::info!(stream, writer, topic, "opcua pubsub: metadata published");
        Ok(version)
    }

    /// Reads the retained DataSetMetaData of a stream's writer, failing with
    /// [`StreamError::NotFound`] when the broker holds none within
    /// [`PubSubConfig::metadata_timeout`].
    pub async fn metadata(&self, stream: &str, writer: &str) -> Result<MetaDataMessage> {
        let group = self.inner.group_of(&self.declared(stream)?.id);
        let topic = self.inner.topic("metadata", &group, Some(writer));
        let client_id = format!("runtime-rs-pubsub-meta-{}", provider::new_id());
        let context = format!("opcua pubsub: cannot read {topic:?}");
        let subscribed = subscribe(
            &self.inner.options,
            &client_id,
            Session::Clean,
            vec![Filter::new(topic.clone(), self.inner.config.qos)],
            &context,
        )
        .await?;
        let (client, mut events) = (subscribed.client, subscribed.events);
        let read = tokio::time::timeout(self.inner.config.metadata_timeout, async {
            if let Some(publish) = subscribed.early.first() {
                return MetaDataMessage::from_json(&publish.payload);
            }
            loop {
                let event = events
                    .poll()
                    .await
                    .map_err(|e| StreamError::internal(&context, e))?;
                if let Event::Incoming(Incoming::Publish(publish)) = event {
                    return MetaDataMessage::from_json(&publish.payload);
                }
            }
        })
        .await;
        let _ = client.disconnect().await;
        let _ = tokio::time::timeout(LINGER, async { while events.poll().await.is_ok() {} }).await;
        read.unwrap_or_else(|_| {
            Err(StreamError::NotFound(format!(
                "opcua pubsub: no metadata is retained on {topic:?}"
            )))
        })
    }
}

/// Part 14's VersionTime: seconds since 2000-01-01T00:00:00Z.
fn version_time() -> u32 {
    let unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    u32::try_from(unix.saturating_sub(946_684_800)).unwrap_or(u32::MAX)
}

/// A DataSetWriter's numeric id: 32-bit FNV-1a over its name, folded to 16
/// bits, never 0 (which Part 14 reserves).
pub fn writer_id(name: &str) -> u16 {
    let hash = name.bytes().fold(0x811c_9dc5_u32, |hash, byte| {
        (hash ^ u32::from(byte)).wrapping_mul(0x0100_0193)
    });
    match ((hash >> 16) ^ (hash & 0xffff)) as u16 {
        0 => 1,
        id => id,
    }
}

impl Inner {
    /// Reads the declared streams, recovering from a poisoned lock.
    fn read(&self) -> std::sync::RwLockReadGuard<'_, BTreeMap<String, Stream>> {
        self.declared.read().unwrap_or_else(|e| e.into_inner())
    }

    /// Writes the declared streams, recovering from a poisoned lock.
    fn write(&self) -> std::sync::RwLockWriteGuard<'_, BTreeMap<String, Stream>> {
        self.declared.write().unwrap_or_else(|e| e.into_inner())
    }

    /// The WriterGroup a declared stream id names.
    pub(crate) fn group_of(&self, id: &str) -> Group {
        match id.split_once('/') {
            Some((publisher, group)) => Group {
                own: publisher == self.config.publisher_id,
                publisher: publisher.to_owned(),
                group: group.to_owned(),
            },
            None => Group {
                publisher: self.config.publisher_id.clone(),
                group: id.to_owned(),
                own: true,
            },
        }
    }

    /// `<prefix>/json/<kind>/<publisher>/<group>[/<writer>]`.
    pub(crate) fn topic(&self, kind: &str, group: &Group, writer: Option<&str>) -> String {
        let mut topic = format!(
            "{}/json/{kind}/{}/{}",
            self.config.prefix, group.publisher, group.group
        );
        if let Some(writer) = writer {
            topic.push('/');
            topic.push_str(writer);
        }
        topic
    }

    /// Reserves `n` consecutive SequenceNumbers for a writer and returns the
    /// first.
    pub(crate) fn reserve(&self, group: &str, writer: &str, n: u32) -> u32 {
        let mut sequences = lock(&self.sequences);
        let last = sequences
            .entry((group.to_owned(), writer.to_owned()))
            .or_insert(0);
        let first = last.wrapping_add(1);
        *last = last.wrapping_add(n);
        first
    }

    /// The ConfigurationVersion last announced for a writer through `publish_metadata`, if any.
    pub(crate) fn version(
        &self,
        group: &str,
        writer: &str,
    ) -> Option<ConfigurationVersionDataType> {
        lock(&self.versions)
            .get(&(group.to_owned(), writer.to_owned()))
            .cloned()
    }

    /// Adds to the in-process gap total and, when a meter is set, the gap counter.
    pub(crate) fn record_gap(&self, publisher: &str, writer: &str, missed: u64) {
        self.gaps.fetch_add(missed, Ordering::Relaxed);
        if let Some(counter) = &self.gap_counter {
            counter.add(
                missed,
                &[
                    tpp_telemetry::opentelemetry::KeyValue::new("publisher", publisher.to_owned()),
                    tpp_telemetry::opentelemetry::KeyValue::new("writer", writer.to_owned()),
                ],
            );
        }
    }
}

/// The error for a stream this provider has not declared.
fn not_found(id: &str) -> StreamError {
    StreamError::NotFound(format!("stream {id}"))
}

/// Refuses a topic level that is empty or carries MQTT structure.
fn check_level(what: &str, level: &str) -> Result<()> {
    if level.is_empty() || level.contains(['/', '+', '#']) {
        return Err(StreamError::Unsupported(format!(
            "opcua pubsub: {what} {level:?} is a topic level, so it must be non-empty and \
             free of '/', '+' and '#'"
        )));
    }
    Ok(())
}

/// Checks a stream's id, subjects and default headers, and stores it under
/// `id`, active.
fn declaration(id: String, stream: Stream) -> Result<Stream> {
    stream.headers.validate()?;
    match id.split_once('/') {
        Some((publisher, group)) => {
            check_level("the PublisherId", publisher)?;
            check_level("the WriterGroup", group)?;
        }
        None => check_level("the WriterGroup", &id)?,
    }
    let mut ids: HashMap<u16, &str> = HashMap::new();
    for subject in &stream.subjects {
        let (writer, field) = manager::split(subject);
        check_level("the DataSetWriter", writer)?;
        if let Some(field) = field
            && (field.is_empty() || field.contains(['+', '#']))
        {
            return Err(StreamError::Unsupported(format!(
                "opcua pubsub: the field of subject {subject:?} must be non-empty and free of \
                 wildcards"
            )));
        }
        let id = writer_id(writer);
        if let Some(other) = ids.insert(id, writer)
            && other != writer
        {
            return Err(StreamError::Unsupported(format!(
                "opcua pubsub: the DataSetWriters {other:?} and {writer:?} hash to the same \
                 DataSetWriterId {id}; rename one"
            )));
        }
    }
    Ok(Stream {
        id,
        active: true,
        ..stream
    })
}

#[async_trait::async_trait]
impl Streams for PubSubProvider {
    /// Checks the id and subjects as topic levels and refuses two writers whose ids collide.
    async fn create(&self, stream: Stream) -> Result<Stream> {
        let id = if stream.id.is_empty() {
            safe_name(&provider::new_stream_id(&stream.name))
        } else {
            stream.id.clone()
        };
        let out = declaration(id.clone(), stream)?;
        self.inner.write().insert(id.clone(), out.clone());
        tracing::info!(stream = %id, subjects = ?out.subjects, "opcua pubsub: stream declared");
        Ok(out)
    }

    /// Reads the in-process declaration; the broker keeps no stream catalogue.
    async fn get(&self, id: &str) -> Result<Stream> {
        self.declared(id)
    }

    /// The manager keeps a copy of the declaration taken now.
    async fn bind(&self, id: &str) -> Result<Arc<dyn Manager>> {
        let stream = self.declared(id)?;
        Ok(Arc::new(PubSubManager::new(self.inner.clone(), stream)))
    }

    /// Revalidates as create does; managers already bound keep the old declaration.
    async fn update(&self, id: &str, stream: Stream) -> Result<Stream> {
        let out = declaration(id.to_owned(), stream)?;
        let mut declared = self.inner.write();
        let slot = declared.get_mut(id).ok_or_else(|| not_found(id))?;
        *slot = out;
        Ok(slot.clone())
    }

    /// Forgets the declaration; managers already bound and their subscriptions are untouched.
    async fn delete(&self, id: &str) -> Result<()> {
        self.inner
            .write()
            .remove(id)
            .map(|_| ())
            .ok_or_else(|| not_found(id))
    }

    /// Lists only the streams this process declared, in id order.
    async fn list(&self) -> Result<Vec<Stream>> {
        Ok(self.inner.read().values().cloned().collect())
    }

    /// Disconnects the publishing connections. Subscriptions end when their
    /// streams are dropped.
    async fn close(&self) -> Result<()> {
        self.inner.links.close().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writer_ids_are_stable_and_never_zero() {
        assert_eq!(writer_id("boiler"), writer_id("boiler"));
        assert_ne!(writer_id("boiler"), writer_id("pump"));
        for n in 0..5000 {
            assert_ne!(writer_id(&format!("w{n}")), 0);
        }
    }

    #[test]
    fn declarations_check_topic_levels() {
        let stream = |id: &str, subjects: &[&str]| {
            declaration(
                id.into(),
                Stream {
                    subjects: subjects.iter().map(|s| s.to_string()).collect(),
                    ..Stream::default()
                },
            )
        };
        assert!(stream("line-a", &["boiler", "pump/Speed"]).is_ok());
        assert!(stream("plant-2/line-a", &["boiler"]).is_ok());
        for (id, subjects) in [
            ("a/b/c", &["w"][..]),
            ("", &["w"]),
            ("line+", &["w"]),
            ("line", &["w+"]),
            ("line", &["#"]),
            ("line", &["w/"]),
            ("line", &["/f"]),
            ("line", &["w/f#"]),
        ] {
            assert!(
                matches!(stream(id, subjects), Err(StreamError::Unsupported(_))),
                "{id} {subjects:?}"
            );
        }
    }

    #[test]
    fn version_time_counts_from_2000() {
        // 2026 is about 26 years of seconds past 2000.
        assert!(version_time() > 26 * 365 * 24 * 3600);
    }
}
