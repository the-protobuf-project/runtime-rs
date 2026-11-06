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

mod manager;
mod topic;

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{
        Arc, Mutex, MutexGuard, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use rumqttc::{
    Outgoing,
    v5::{
        AsyncClient, ConnectionError, Event, EventLoop, Incoming,
        mqttbytes::v5::{PubAckReason, PubRecReason},
    },
};
use tokio::{sync::oneshot, task::JoinHandle, time::Instant};

use crate::{Manager, Registry, Result, Stream, StreamError, Streams, provider::Metrics};

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
const REQUEST_CAPACITY: usize = 256;

/// How long the publishing connection waits before redialing a lost broker,
/// so an outage is a retry a second rather than a spin.
const RECONNECT_DELAY: Duration = Duration::from_secs(1);

/// How long a closing connection is given to put its DISCONNECT on the wire.
const LINGER: Duration = Duration::from_secs(1);

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
    publisher: PublishLink,
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

        // One connection for publishing. Subscriptions get their own, because a
        // session's identity is its client id and is fixed when it connects.
        let (client, events, _) = dial(&options, &client_id, Session::Clean).await?;
        let publisher = PublishLink::spawn(client, events, config.publish_timeout);
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
    async fn create(&self, stream: Stream) -> Result<Stream> {
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

    /// Disconnects the publishing connection.
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
    match session {
        Session::Clean => {
            options
                .set_clean_start(true)
                .set_session_expiry_interval(None)
                .set_manual_acks(false);
        }
        Session::Durable { expiry } => {
            options
                .set_clean_start(false)
                .set_session_expiry_interval(Some(expiry))
                // Acknowledgement belongs to the consumer, after the work.
                // Acknowledging on receipt would turn at-least-once into
                // at-most-once behind the caller's back.
                .set_manual_acks(true);
        }
    }
    let (host, port) = template.broker_address();
    let (client, mut events) = AsyncClient::new(options, REQUEST_CAPACITY);
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

/// The publishing connection and the task driving its event loop.
struct PublishLink {
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

    /// Publishes `body` on `topic` and waits for the broker to take it.
    async fn publish(&self, topic: &str, qos: QoS, body: Bytes) -> Result<()> {
        let deadline = Instant::now() + self.timeout;
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
                if self
                    .client
                    .try_publish(topic, qos, false, body.clone())
                    .is_ok()
                {
                    waiters.queued.push_back(tx.take().expect("queued once"));
                    break;
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
    fn safe_name_strips_topic_structure() {
        for s in ["a/b", "a+b", "a#b"] {
            assert_ne!(safe_name(s), s, "{s}");
        }
        assert_eq!(safe_name("01J:orders"), "01J:orders");
    }
}
