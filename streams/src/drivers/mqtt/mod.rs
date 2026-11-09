//! Streams over MQTT 5, through `rumqttc`.
//!
//! The provider implements [`Publisher`](crate::Publisher),
//! [`Subscriber`](crate::Subscriber), [`Durable`](crate::Durable), and
//! [`Batch`](crate::Batch) by publishing each payload in turn, but not
//! [`Positioned`](crate::Positioned), and that split is the interesting thing
//! about it.
//!
//! A persistent session is real durability: a named client's subscriptions and
//! its unacknowledged QoS 1 messages are kept by the broker while it is away, so
//! a consumer that dies and comes back is handed what it missed and what it
//! never finished. But a session is a queue, not a log. There is nothing behind
//! it to seek, so there is no position but now, and
//! [`as_positioned`](crate::as_positioned) refuses by name. A contract that had
//! fused the two capabilities would have had to lie about replay or throw away
//! durability MQTT really has.
//!
//! # This provider dials
//!
//! Unlike most providers this one takes connection settings rather than a
//! connected client, as runtime-go's does. A durable consumer *is* an MQTT
//! session, identified by client id and negotiated when the connection opens,
//! so every named consumer needs a connection of its own, and a handed-in
//! `AsyncClient` could only ever be one of them. Its `EventLoop` would also have
//! to be polled by whoever owns it, and a provider that routed someone else's
//! loop would be one forgotten `poll` away from a silent stall.
//!
//! So the caller owns the [`MqttOptions`]: host, port, TLS, credentials,
//! keep-alive, packet sizes. The provider clones it for every connection it
//! opens, overriding only the client id, the clean-start flag, the session
//! expiry, and manual acknowledgement, which are the four things a connection's
//! role decides. Each connection's event loop is driven by a task the provider
//! spawns, and ends with the thing that owns it: the publishing connection with
//! [`Streams::close`] or the last handle to the
//! provider, a subscription's when its stream is dropped.
//!
//! # Subjects are topics
//!
//! A subject is an MQTT topic, levels separated by `/`, with `+` and `#` as the
//! wildcards; a stream may declare a filter such as `user/+` and accept every
//! subject it covers. Messages travel on `<prefix>/<stream id>/<subject>`, the
//! prefix omitted when empty, exactly as runtime-go lays them out, so a Go and a
//! Rust service declaring a stream under the same id share its topics.
//!
//! # Declarations are local
//!
//! MQTT has no registry to keep a stream in, so a stream is a declaration this
//! process holds: two processes each declare their own, and a restart forgets.
//! What it buys is the subject check, so publishing to a topic the stream never
//! declared fails at the call that made the typo.
//!
//! # Headers, expiry, retain
//!
//! - **Headers** travel as MQTT 5 user properties on the PUBLISH, one per
//!   header in key order, the stream's [`Stream::headers`] with the
//!   publish's own [`Options::headers`](crate::Options::headers) on top; a
//!   batch stamps them on every message. The payload stays runtime-go's
//!   version-1 frame, so a Go peer reads it unchanged, and any MQTT 5 tool
//!   sees the headers (`mosquitto_sub -V mqttv5 -F '%P %t'`). Subscriptions
//!   and durable consumers put a message's user properties back on it as
//!   [`Message::headers`](crate::Message::headers), including on a
//!   redelivery after a restart, since the broker stores the properties with
//!   the queued message. MQTT reserves no user property names, so no key is
//!   refused beyond [`Headers::validate`]; a key repeated by another
//!   publisher keeps its last value. `create` and `update` refuse invalid
//!   stream headers.
//! - **Expiry** ([`Options::with_expiry`](crate::Options::with_expiry)) is
//!   the MQTT 5 Message Expiry Interval, in whole seconds, rounded up: a
//!   sub-second expiry is one second, never zero, so a message is never
//!   dropped sooner than asked. The broker discards the message once it has
//!   waited that long in a session's queue or as a retained value; a message
//!   handed to a connected subscriber at once is delivered whatever its
//!   expiry.
//! - **Retain** ([`Options::with_retain`](crate::Options::with_retain)) is
//!   the retain flag: the broker keeps the message as the topic's last value
//!   and hands it to every subscriber that joins later, framed as any other
//!   message, so it decodes. A durable consumer is handed it when its
//!   subscription is new, not again on each resume.
//!   [`MqttProvider::clear_retained`] clears it with MQTT's own empty
//!   retained publish; subscriptions skip that empty message.
//! - **Delayed nak** ([`Delivery::nak_after`](crate::Delivery::nak_after)) is
//!   refused: MQTT has no negative acknowledgement and no redelivery delay,
//!   and a client-side sleep would be a lie about where the message is.

mod manager;
mod topic;

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{
        Arc, Mutex, MutexGuard, RwLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use rumqttc::{
    Outgoing,
    v5::{
        AsyncClient, ConnectionError, Event, EventLoop, Incoming,
        mqttbytes::v5::{
            Filter, PubAckReason, PubRecReason, Publish, PublishProperties, SubscribeReasonCode,
        },
    },
};
use tokio::{sync::oneshot, task::JoinHandle, time::Instant};

use crate::{Headers, Manager, Registry, Result, Stream, StreamError, Streams, provider::Metrics};

use manager::MqttManager;

pub use rumqttc::v5::{MqttOptions, mqttbytes::QoS};

/// How long a broker keeps a departed durable consumer's session when
/// [`MqttConfig::session_expiry`] is not changed.
///
/// A day is long enough to survive a deploy and short enough that a consumer
/// nobody runs again is not kept forever. It is runtime-go's default.
pub const DEFAULT_SESSION_EXPIRY: Duration = Duration::from_secs(24 * 60 * 60);

/// How long a publish waits for the broker's acknowledgement when
/// [`MqttConfig::publish_timeout`] is not changed.
pub const DEFAULT_PUBLISH_TIMEOUT: Duration = Duration::from_secs(10);

/// Requests a connection can queue ahead of its event loop.
///
/// Deep enough that a few dozen concurrent publishers, or a batch of a few
/// thousand, queue without ever finding it full: a full channel makes a
/// publish back off and retry, which costs a millisecond each time.
const REQUEST_CAPACITY: usize = 8192;

/// Publishing connections a provider opens when
/// [`MqttConfig::publish_connections`] is not changed.
///
/// One, because MQTT orders messages per connection: with one, every publish
/// on a topic reaches the broker in the order it was sent, concurrent ones
/// included. Raising it speeds up batches when the broker is not the limit.
pub const DEFAULT_PUBLISH_CONNECTIONS: usize = 1;

/// How long the publishing connection waits before redialing a lost broker,
/// so an outage is a retry a second rather than a spin.
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

/// How long a closing connection is given to put its DISCONNECT on the wire.
pub(crate) const LINGER: Duration = Duration::from_secs(1);

/// Settings for an [`MqttProvider`], beyond the connection itself.
#[derive(Clone)]
pub struct MqttConfig {
    /// Namespaces every topic these streams use, so several independent sets
    /// can share a broker. Empty means no prefix.
    pub prefix: String,

    /// The quality of service every publish and subscription uses. Defaults to
    /// at-least-once.
    ///
    /// At-most-once is fire-and-forget: the broker does not acknowledge,
    /// nothing is redelivered, and a durable consumer built on it would be
    /// durable in name only. Exactly-once costs a second round trip for a
    /// guarantee the contract does not promise.
    pub qos: QoS,

    /// How long the broker keeps a departed durable consumer's session: its
    /// subscriptions and the messages it has not acknowledged.
    ///
    /// This is the window in which a restart still resumes rather than starts
    /// over. Past it the broker discards the session and the consumer comes
    /// back as a stranger, having missed whatever arrived while it was gone.
    /// Whole seconds only, as MQTT counts it.
    pub session_expiry: Duration,

    /// The client id of the publishing connection. `None` generates one.
    ///
    /// Durable consumers ignore it: their identity is the consumer name, which
    /// is the whole point of naming them.
    pub client_id: Option<String>,

    /// The codecs a delivered frame may name. JSON is always accepted.
    pub registry: Registry,

    /// Delivery and settlement counters for durable consumers.
    pub metrics: Metrics,

    /// How long a publish waits for the broker to take responsibility for the
    /// message before failing. At QoS 0 there is no acknowledgement and it
    /// bounds only the hand-off to the connection.
    pub publish_timeout: Duration,

    /// How many connections publishes are spread over. At least one.
    ///
    /// Every QoS 1 publish waits for its PUBACK, and one connection carries
    /// every request through one event loop and one socket in turn, so with
    /// many concurrent publishers it becomes the queue they all wait in.
    /// Several connections share the load and the broker acknowledges each
    /// one independently. A publish goes out whole on one connection and still
    /// returns only once the broker has it, and a batch stays on one
    /// connection, in order.
    ///
    /// The cost of more than one is ordering. MQTT orders messages per
    /// connection, so two publishes sent concurrently without awaiting the
    /// first can reach the broker in either order once they may take
    /// different connections. Publishes awaited one after another keep their
    /// order whatever this is. The default, one, keeps every publish in order.
    ///
    /// With [`client_id`](Self::client_id) set, the first connection uses it
    /// and the others append `-1`, `-2`, and so on, since a broker gives a
    /// client id to one connection at a time.
    pub publish_connections: usize,
}

impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            prefix: String::new(),
            qos: QoS::AtLeastOnce,
            session_expiry: DEFAULT_SESSION_EXPIRY,
            client_id: None,
            registry: Registry::new(),
            metrics: Metrics::default(),
            publish_timeout: DEFAULT_PUBLISH_TIMEOUT,
            publish_connections: DEFAULT_PUBLISH_CONNECTIONS,
        }
    }
}

/// Streams over an MQTT 5 broker.
///
/// Built with [`MqttProvider::connect`], which opens the publishing connection
/// and fails if the broker cannot be reached. Cloning is cheap and shares that
/// connection.
#[derive(Clone)]
pub struct MqttProvider {
    inner: Arc<Inner>,
}

/// What every bound manager shares with the provider.
pub(crate) struct Inner {
    options: MqttOptions,
    config: MqttConfig,
    publisher: PublishPool,
    declared: RwLock<BTreeMap<String, Stream>>,
}

impl MqttProvider {
    /// Connects to the broker `options` describes.
    ///
    /// `options` is a template: its client id, clean-start flag, session
    /// expiry, and manual-acknowledgement setting are replaced per connection,
    /// and everything else is used as given.
    pub async fn connect(options: MqttOptions, config: MqttConfig) -> Result<Self> {
        if options.broker_address().0.is_empty() {
            return Err(StreamError::Internal(
                "mqtt: no broker address given".into(),
            ));
        }
        let client_id = config
            .client_id
            .clone()
            .unwrap_or_else(|| format!("runtime-rs-{}", crate::provider::new_id()));

        // Connections for publishing. Subscriptions get their own, because a
        // session's identity is its client id and is fixed when it connects.
        let publisher = PublishPool::connect(
            &options,
            &client_id,
            config.publish_connections,
            config.publish_timeout,
        )
        .await?;
        Ok(Self {
            inner: Arc::new(Inner {
                options,
                config,
                publisher,
                declared: RwLock::new(BTreeMap::new()),
            }),
        })
    }
}

impl MqttProvider {
    /// Clears the value retained on `subject` of stream `stream`, so a
    /// subscriber that joins later is handed nothing there.
    ///
    /// A message published with [`Options::with_retain`](crate::Options::with_retain)
    /// stays the topic's retained value until another retained publish
    /// replaces it or this clears it. Clearing is MQTT's own way: a retained
    /// publish with an empty payload, which the broker takes as "retain
    /// nothing" and forwards to current subscribers as an empty message.
    /// This provider's subscriptions skip that empty message rather than
    /// failing to decode it, since it carries no frame.
    pub async fn clear_retained(&self, stream: &str, subject: &str) -> Result<()> {
        let declared = self.get(stream).await?;
        let manager = MqttManager::new(self.inner.clone(), declared);
        let topic = manager.publish_topic(subject, &crate::Options::default())?;
        self.inner
            .publisher
            .link()
            .send(&topic, self.inner.config.qos, true, None, Bytes::new())
            .await
            .map_err(|error| {
                StreamError::internal(
                    &format!("mqtt: cannot clear the retained value on {subject:?}"),
                    error,
                )
            })
    }
}

impl Inner {
    /// The MQTT topic a subject's messages travel on.
    pub(crate) fn topic(&self, stream: &str, subject: &str) -> String {
        if self.config.prefix.is_empty() {
            format!("{stream}/{subject}")
        } else {
            format!("{}/{stream}/{subject}", self.config.prefix)
        }
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, BTreeMap<String, Stream>> {
        self.declared.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, BTreeMap<String, Stream>> {
        self.declared.write().unwrap_or_else(|e| e.into_inner())
    }
}

/// The error for a stream this process has not declared.
fn not_found(id: &str) -> StreamError {
    StreamError::NotFound(format!("stream {id}"))
}

/// The declaration a stream is stored as: the caller's fields under `id`,
/// always active.
fn declaration(id: String, stream: Stream) -> Stream {
    Stream {
        id,
        active: true,
        ..stream
    }
}

#[async_trait::async_trait]
impl Streams for MqttProvider {
    /// Declares the stream, refusing [`Stream::headers`] that fail
    /// [`Headers::validate`], so a bad default fails here rather than at the
    /// first publish.
    async fn create(&self, stream: Stream) -> Result<Stream> {
        stream.headers.validate()?;
        let id = if stream.id.is_empty() {
            safe_name(&crate::provider::new_stream_id(&stream.name))
        } else {
            stream.id.clone()
        };
        let out = declaration(id.clone(), stream);
        self.inner.write().insert(id.clone(), out.clone());
        tracing::info!(stream = %id, name = %out.name, subjects = ?out.subjects, "mqtt: stream declared");
        Ok(out)
    }

    async fn get(&self, id: &str) -> Result<Stream> {
        self.inner
            .read()
            .get(id)
            .cloned()
            .ok_or_else(|| not_found(id))
    }

    async fn bind(&self, id: &str) -> Result<Arc<dyn Manager>> {
        let stream = self.get(id).await?;
        Ok(Arc::new(MqttManager::new(self.inner.clone(), stream)))
    }

    async fn update(&self, id: &str, stream: Stream) -> Result<Stream> {
        stream.headers.validate()?;
        let mut declared = self.inner.write();
        let slot = declared.get_mut(id).ok_or_else(|| not_found(id))?;
        *slot = declaration(id.to_owned(), stream);
        Ok(slot.clone())
    }

    async fn delete(&self, id: &str) -> Result<()> {
        self.inner
            .write()
            .remove(id)
            .map(|_| ())
            .ok_or_else(|| not_found(id))
    }

    async fn list(&self) -> Result<Vec<Stream>> {
        // A BTreeMap lists in id order, the order runtime-go sorts into.
        Ok(self.inner.read().values().cloned().collect())
    }

    /// Disconnects the publishing connections.
    ///
    /// Subscriptions hold connections of their own and end when their streams
    /// are dropped, so drop those first or their sessions stay attached until
    /// the broker notices.
    async fn close(&self) -> Result<()> {
        self.inner.publisher.close().await;
        Ok(())
    }
}

/// What a connection's session is for.
#[derive(Clone, Copy)]
pub(crate) enum Session {
    /// Starts fresh and is discarded on disconnect, acknowledging on receipt.
    Clean,
    /// Resumes whatever the broker kept under this client id, keeps it for
    /// `expiry` seconds after disconnect, and leaves acknowledgement to the
    /// consumer.
    Durable {
        /// Session expiry interval, in seconds.
        expiry: u32,
        /// How many unacknowledged messages the broker may have out to this
        /// consumer at once: the MQTT 5 Receive Maximum, which is the
        /// consumer's prefetch.
        prefetch: u16,
    },
}

/// Opens a connection under `client_id` and completes the MQTT handshake.
///
/// It reports whether the broker already had a session for this id, which
/// decides whether the caller still has to subscribe: a resumed session comes
/// back with its subscriptions intact.
pub(crate) async fn dial(
    template: &MqttOptions,
    client_id: &str,
    session: Session,
) -> Result<(AsyncClient, EventLoop, bool)> {
    let mut options = template.clone();
    options.set_client_id(client_id.to_owned());
    let mut capacity = REQUEST_CAPACITY;
    match session {
        Session::Clean => {
            options
                .set_clean_start(true)
                .set_session_expiry_interval(None)
                .set_manual_acks(false);
        }
        Session::Durable { expiry, prefetch } => {
            options
                .set_clean_start(false)
                .set_session_expiry_interval(Some(expiry))
                // Acknowledgement belongs to the consumer, after the work.
                // Acknowledging on receipt would turn at-least-once into
                // at-most-once behind the caller's back.
                .set_manual_acks(true)
                // The broker holds back what the consumer has not caught up
                // with, in the session's queue, instead of pushing it all down
                // the socket. That bounds the acknowledgements the consumer
                // can owe at once, and the request channel is sized above it,
                // so queueing a PUBACK never waits on the event loop, which
                // may itself be waiting on the consumer to take a message.
                .set_receive_maximum(Some(prefetch.max(1)));
            capacity = capacity.max(usize::from(prefetch) + REQUEST_CAPACITY);
        }
    }
    let (host, port) = template.broker_address();
    let (client, mut events) = AsyncClient::new(options, capacity);
    // The event loop dials on its first poll, bounded by the template's
    // connection timeout, and yields the CONNACK as its first event.
    loop {
        match events.poll().await {
            Ok(Event::Incoming(Incoming::ConnAck(ack))) => {
                return Ok((client, events, ack.session_present));
            }
            Ok(_) => {}
            Err(error) => {
                return Err(StreamError::internal(
                    &format!("mqtt: cannot connect to {host}:{port}"),
                    error,
                ));
            }
        }
    }
}

/// Strips the characters MQTT reserves for topic structure, so a generated id
/// carrying the caller's stream name cannot change the topic tree.
pub(crate) fn safe_name(s: &str) -> String {
    s.chars()
        .map(|c| if matches!(c, '/' | '+' | '#') { '-' } else { c })
        .collect()
}

/// Locks a mutex, recovering it if a holder panicked: the waiter table stays
/// consistent under every operation that touches it.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// A publish waiting to hear whether the broker took it.
type Waiter = oneshot::Sender<Result<()>>;

/// The publishes the connection has been handed and not yet settled.
///
/// rumqttc's `publish` returns once the request is queued, not once the broker
/// has it, and its acknowledgement events name a packet id the caller never
/// saw. The event loop takes requests in order and reports each as an
/// `Outgoing::Publish(pkid)` in that order, so a FIFO of waiters, pushed under
/// the same lock as the request is queued, pairs each publish with its packet
/// id, and the PUBACK settles it. That is how [`Publisher::publish`] keeps
/// runtime-go's meaning: it returns when the broker has the message.
///
/// [`Publisher::publish`]: crate::Publisher::publish
#[derive(Default)]
struct Waiters {
    /// Queued and not yet written, in request order.
    queued: VecDeque<Waiter>,
    /// Written and awaiting the broker's acknowledgement, by packet id.
    inflight: HashMap<u16, Waiter>,
}

impl Waiters {
    fn fail_all(&mut self, error: &StreamError) {
        for waiter in self
            .queued
            .drain(..)
            .chain(self.inflight.drain().map(|(_, w)| w))
        {
            let _ = waiter.send(Err(error.clone()));
        }
    }

    fn settle(&mut self, pkid: u16, outcome: Result<()>) {
        if let Some(waiter) = self.inflight.remove(&pkid) {
            let _ = waiter.send(outcome);
        }
    }
}

/// The longest Message Expiry Interval MQTT 5 can carry, in seconds.
const MAX_EXPIRY_SECS: u64 = u32::MAX as u64;

/// A [`Duration`] as MQTT 5's Message Expiry Interval: whole seconds,
/// rounded up, so a message is never dropped sooner than asked. A
/// sub-second expiry is one second, never zero; an expiry beyond
/// `u32::MAX` seconds (136 years) is capped there.
pub(crate) fn expiry_interval(expires: Duration) -> u32 {
    let mut secs = expires.as_secs();
    if expires.subsec_nanos() > 0 {
        secs += 1;
    }
    secs.clamp(1, MAX_EXPIRY_SECS) as u32
}

/// The PUBLISH properties for `headers` and `expires`: the headers as MQTT 5
/// user properties, in key order, and the expiry as the Message Expiry
/// Interval. `None` when there is neither, so a header-less publish is
/// byte for byte what runtime-go sends.
pub(crate) fn publish_properties(
    headers: Headers,
    expires: Option<Duration>,
) -> Option<PublishProperties> {
    if headers.is_empty() && expires.is_none() {
        return None;
    }
    Some(PublishProperties {
        user_properties: headers.into_iter().collect(),
        message_expiry_interval: expires.map(expiry_interval),
        ..PublishProperties::default()
    })
}

/// The headers a PUBLISH arrived with: its MQTT 5 user properties. MQTT
/// allows a key more than once, which [`Headers`] does not; the last one
/// wins, as it does for a key a publish sets over its stream's.
pub(crate) fn headers_of(publish: &Publish) -> Headers {
    match &publish.properties {
        Some(properties) if !properties.user_properties.is_empty() => properties
            .user_properties
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect(),
        _ => Headers::new(),
    }
}

/// A connection subscribed and acknowledged, not yet routing.
pub(crate) struct Subscribed {
    pub(crate) client: AsyncClient,
    pub(crate) events: EventLoop,
    /// Publishes that arrived before the SUBACK, in order, held rather than
    /// dropped.
    pub(crate) early: Vec<Publish>,
    /// Whether the broker resumed a session it kept under this client id.
    pub(crate) resumed: bool,
}

/// Opens a connection under `client_id`, subscribes it to `filters`, and
/// returns once the broker has granted every one, as the contract promises
/// before a subscribe returns.
///
/// Errors read `<context>: <why>`, with the broker's reason codes when it
/// refused, which is the difference between a refused filter and a quota.
pub(crate) async fn subscribe(
    template: &MqttOptions,
    client_id: &str,
    session: Session,
    filters: Vec<Filter>,
    context: &str,
) -> Result<Subscribed> {
    let failed = |error: &dyn std::fmt::Display| StreamError::internal(context, error);
    let (client, mut events, resumed) = dial(template, client_id, session).await?;
    let wanted = filters.len();
    client
        .subscribe_many(filters)
        .await
        .map_err(|e| failed(&e))?;
    let mut early = Vec::new();
    loop {
        match events.poll().await.map_err(|e| failed(&e))? {
            Event::Incoming(Incoming::SubAck(ack)) => {
                if ack.return_codes.len() == wanted
                    && ack
                        .return_codes
                        .iter()
                        .all(|c| matches!(c, SubscribeReasonCode::Success(_)))
                {
                    break;
                }
                let _ = client.try_disconnect();
                return Err(failed(&format_args!(
                    "broker reasons {:?}",
                    ack.return_codes
                )));
            }
            Event::Incoming(Incoming::Publish(publish)) => early.push(publish),
            _ => {}
        }
    }
    Ok(Subscribed {
        client,
        events,
        early,
        resumed,
    })
}

/// The publishing connections, taken in turn.
pub(crate) struct PublishPool {
    links: Vec<PublishLink>,
    next: AtomicUsize,
}

impl PublishPool {
    /// Opens `count` (at least one) clean connections under `client_id`,
    /// `client_id-1`, `client_id-2`, ..., each acknowledging within
    /// `timeout`.
    pub(crate) async fn connect(
        options: &MqttOptions,
        client_id: &str,
        count: usize,
        timeout: Duration,
    ) -> Result<Self> {
        let mut links = Vec::new();
        for n in 0..count.max(1) {
            let id = if n == 0 {
                client_id.to_owned()
            } else {
                format!("{client_id}-{n}")
            };
            match dial(options, &id, Session::Clean).await {
                Ok((client, events, _)) => {
                    links.push(PublishLink::spawn(client, events, timeout));
                }
                Err(error) => {
                    for link in &links {
                        link.close().await;
                    }
                    return Err(error);
                }
            }
        }
        Ok(Self {
            links,
            next: AtomicUsize::new(0),
        })
    }

    /// The connection the next publish goes out on.
    pub(crate) fn link(&self) -> &PublishLink {
        let n = self.next.fetch_add(1, Ordering::Relaxed);
        &self.links[n % self.links.len()]
    }

    /// Disconnects every connection.
    pub(crate) async fn close(&self) {
        futures::future::join_all(self.links.iter().map(PublishLink::close)).await;
    }
}

/// One publish as it goes on the wire: the body is sent as given, unframed,
/// so a caller with a format of its own (OPC UA PubSub's Part 14 JSON) and
/// one writing runtime-go's frame share the connection and its PUBACK
/// pairing.
pub(crate) struct Unframed {
    pub(crate) topic: String,
    pub(crate) retain: bool,
    pub(crate) properties: Option<PublishProperties>,
    pub(crate) body: Bytes,
}

/// A publishing connection and the task driving its event loop.
pub(crate) struct PublishLink {
    client: AsyncClient,
    waiters: Arc<Mutex<Waiters>>,
    closed: Arc<AtomicBool>,
    timeout: Duration,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl PublishLink {
    fn spawn(client: AsyncClient, events: EventLoop, timeout: Duration) -> Self {
        let waiters = Arc::new(Mutex::new(Waiters::default()));
        let closed = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn(drive_publisher(events, waiters.clone(), closed.clone()));
        Self {
            client,
            waiters,
            closed,
            timeout,
            task: Mutex::new(Some(task)),
        }
    }

    /// Publishes `body`, unframed, on `topic` with the `retain` flag and
    /// `properties` (user properties, expiry, content type) as given, and
    /// waits for the broker to take it.
    pub(crate) async fn send(
        &self,
        topic: &str,
        qos: QoS,
        retain: bool,
        properties: Option<PublishProperties>,
        body: Bytes,
    ) -> Result<()> {
        let deadline = Instant::now() + self.timeout;
        let settled = self
            .enqueue(topic, qos, retain, &properties, &body, deadline)
            .await?;
        self.settled(settled, deadline).await
    }

    /// Publishes every message, all in flight at once, and waits for the
    /// broker to take each: one round trip for the batch rather than one per
    /// message. Outcomes are in `messages` order; the messages go on the
    /// wire in that order too, since one caller queues them back to back.
    pub(crate) async fn send_all(&self, qos: QoS, messages: Vec<Unframed>) -> Vec<Result<()>> {
        let deadline = Instant::now() + self.timeout;
        let mut pending = Vec::with_capacity(messages.len());
        for m in messages {
            pending.push(
                self.enqueue(&m.topic, qos, m.retain, &m.properties, &m.body, deadline)
                    .await,
            );
        }
        futures::future::join_all(pending.into_iter().map(|queued| async move {
            match queued {
                Ok(settled) => self.settled(settled, deadline).await,
                Err(error) => Err(error),
            }
        }))
        .await
    }

    /// Hands a publish to the connection and lines up its waiter, returning
    /// what will hear the broker's answer.
    async fn enqueue(
        &self,
        topic: &str,
        qos: QoS,
        retain: bool,
        properties: &Option<PublishProperties>,
        body: &Bytes,
        deadline: Instant,
    ) -> Result<oneshot::Receiver<Result<()>>> {
        let (tx, rx) = oneshot::channel();
        let mut tx = Some(tx);
        loop {
            if self.closed.load(Ordering::Acquire) {
                return Err(StreamError::Internal("mqtt: the provider is closed".into()));
            }
            {
                // Queued and registered under one lock, so the event loop
                // cannot report this request before its waiter is in line.
                let mut waiters = lock(&self.waiters);
                let queued = match properties {
                    None => self.client.try_publish(topic, qos, retain, body.clone()),
                    Some(properties) => self.client.try_publish_with_properties(
                        topic,
                        qos,
                        retain,
                        body.clone(),
                        properties.clone(),
                    ),
                };
                if queued.is_ok() {
                    waiters.queued.push_back(tx.take().expect("queued once"));
                    return Ok(rx);
                }
            }
            // The request channel is full: the connection is behind or
            // redialing. Waiting here is the backpressure a blocking send
            // would give, without holding the lock across it.
            if Instant::now() >= deadline {
                return Err(self.timed_out());
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Waits for the broker's answer to one queued publish.
    async fn settled(&self, rx: oneshot::Receiver<Result<()>>, deadline: Instant) -> Result<()> {
        match tokio::time::timeout_at(deadline, rx).await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(_)) => Err(StreamError::Internal(
                "mqtt: the connection closed before the broker acknowledged".into(),
            )),
            Err(_) => Err(self.timed_out()),
        }
    }

    fn timed_out(&self) -> StreamError {
        StreamError::Internal(format!(
            "mqtt: the broker did not acknowledge within {:?}",
            self.timeout
        ))
    }

    async fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        let _ = self.client.disconnect().await;
        let task = lock(&self.task).take();
        if let Some(mut task) = task
            && tokio::time::timeout(LINGER, &mut task).await.is_err()
        {
            task.abort();
        }
    }
}

/// Drives the publishing connection, settling waiters as the broker answers.
async fn drive_publisher(
    mut events: EventLoop,
    waiters: Arc<Mutex<Waiters>>,
    closed: Arc<AtomicBool>,
) {
    let gone = || StreamError::Internal("mqtt: the publishing connection is closed".into());
    loop {
        match events.poll().await {
            Ok(Event::Outgoing(Outgoing::Publish(pkid))) => {
                let mut waiters = lock(&waiters);
                if let Some(waiter) = waiters.queued.pop_front() {
                    if pkid == 0 {
                        // QoS 0: written is all the broker will ever say.
                        let _ = waiter.send(Ok(()));
                    } else {
                        waiters.inflight.insert(pkid, waiter);
                    }
                }
            }
            Ok(Event::Incoming(Incoming::PubAck(ack))) => {
                let outcome = match ack.reason {
                    PubAckReason::Success | PubAckReason::NoMatchingSubscribers => Ok(()),
                    reason => Err(refused(format!("{reason:?}"))),
                };
                lock(&waiters).settle(ack.pkid, outcome);
            }
            Ok(Event::Incoming(Incoming::PubRec(rec))) => {
                // QoS 2 settles on PUBCOMP, unless the broker already refused.
                if !matches!(
                    rec.reason,
                    PubRecReason::Success | PubRecReason::NoMatchingSubscribers
                ) {
                    lock(&waiters).settle(rec.pkid, Err(refused(format!("{:?}", rec.reason))));
                }
            }
            Ok(Event::Incoming(Incoming::PubComp(comp))) => {
                lock(&waiters).settle(comp.pkid, Ok(()));
            }
            Ok(Event::Outgoing(Outgoing::Disconnect)) => {
                lock(&waiters).fail_all(&gone());
                return;
            }
            Ok(_) => {}
            Err(ConnectionError::RequestsDone) => {
                // Every handle to the provider is gone.
                lock(&waiters).fail_all(&gone());
                return;
            }
            Err(error) => {
                if closed.load(Ordering::Acquire) {
                    lock(&waiters).fail_all(&gone());
                    return;
                }
                tracing::warn!(%error, "mqtt: publishing connection lost; redialing");
                let failure =
                    StreamError::internal("mqtt: connection lost before acknowledgement", &error);
                {
                    // The session is clean, so the broker kept nothing of what
                    // was in flight. Drop the requests rumqttc would replay and
                    // fail their waiters under the queueing lock, so the next
                    // publish pairs with the next packet id rather than a
                    // stranger's.
                    let mut waiters = lock(&waiters);
                    events.clean();
                    events.pending.clear();
                    waiters.fail_all(&failure);
                }
                tokio::time::sleep(RECONNECT_DELAY).await;
            }
        }
    }
}

fn refused(reason: String) -> StreamError {
    StreamError::Internal(format!("mqtt: the broker refused the publish: {reason}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expiry_rounds_up_to_whole_seconds() {
        assert_eq!(expiry_interval(Duration::from_millis(1)), 1);
        assert_eq!(expiry_interval(Duration::from_millis(999)), 1);
        assert_eq!(expiry_interval(Duration::from_secs(1)), 1);
        assert_eq!(expiry_interval(Duration::from_millis(1001)), 2);
        assert_eq!(expiry_interval(Duration::from_secs(90)), 90);
        assert_eq!(expiry_interval(Duration::from_secs(u64::MAX)), u32::MAX);
    }

    #[test]
    fn properties_carry_headers_and_expiry_only_when_set() {
        assert!(publish_properties(Headers::new(), None).is_none());
        let headers: Headers = [("site", "plant-3"), ("lang", "rust")]
            .into_iter()
            .collect();
        let p = publish_properties(headers.clone(), Some(Duration::from_millis(1500))).unwrap();
        assert_eq!(
            p.user_properties,
            vec![
                ("lang".to_owned(), "rust".to_owned()),
                ("site".to_owned(), "plant-3".to_owned())
            ],
            "user properties in key order"
        );
        assert_eq!(p.message_expiry_interval, Some(2));
        assert!(p.content_type.is_none());
        let p = publish_properties(Headers::new(), Some(Duration::from_secs(5))).unwrap();
        assert!(p.user_properties.is_empty());
        assert_eq!(p.message_expiry_interval, Some(5));

        let publish = Publish::new("t", QoS::AtLeastOnce, "x", Some(p));
        assert!(headers_of(&publish).is_empty());
        let publish = Publish::new(
            "t",
            QoS::AtLeastOnce,
            "x",
            publish_properties(headers.clone(), None),
        );
        assert_eq!(headers_of(&publish), headers);
        assert!(headers_of(&Publish::new("t", QoS::AtLeastOnce, "x", None)).is_empty());
    }

    #[test]
    fn a_repeated_user_property_keeps_the_last() {
        let properties = PublishProperties {
            user_properties: vec![("k".into(), "1".into()), ("k".into(), "2".into())],
            ..PublishProperties::default()
        };
        let publish = Publish::new("t", QoS::AtLeastOnce, "x", Some(properties));
        assert_eq!(headers_of(&publish).get("k"), Some("2"));
    }

    #[test]
    fn safe_name_strips_topic_structure() {
        for s in ["a/b", "a+b", "a#b"] {
            assert_ne!(safe_name(s), s, "{s}");
        }
        assert_eq!(safe_name("01J:orders"), "01J:orders");
    }
}
