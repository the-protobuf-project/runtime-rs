//! Streams over ZeroMQ PUB/SUB, through the pure-Rust `zeromq` crate.
//!
//! The provider implements [`Publisher`](crate::Publisher) and
//! [`Subscriber`](crate::Subscriber), plus [`Batch`](crate::Batch) by publishing
//! each payload in turn, and nothing beyond them. There is no broker here: a
//! publisher binds a socket and subscribers connect to it directly, so there is
//! nowhere for a message to be stored, no record of what anyone handled, and
//! nothing to replay. [`as_durable`](crate::as_durable) and
//! [`as_positioned`](crate::as_positioned) both refuse by name.
//!
//! # Subscribe is best-effort, not race-free
//!
//! [`Subscriber::subscribe`](crate::Subscriber::subscribe) promises that a
//! subscription is active before it returns. **This provider approximates that
//! promise and cannot guarantee it.**
//!
//! A ZeroMQ subscription travels from subscriber to publisher asynchronously and
//! is never acknowledged, so there is no event to wait on. This is the
//! slow-joiner problem, a property of the protocol rather than a gap here:
//! messages published before the subscription arrives are dropped by a
//! publisher that does not yet know anyone wants them. `subscribe` waits
//! [`ZmqConfig::settle`] before returning to make the window small; raise it on
//! a slow or long-haul link. Where a message must not be missed, reach for a
//! provider with a broker behind it.
//!
//! # One side binds
//!
//! A stream is a socket, not a name on a server. [`ZmqProvider::publisher`]
//! binds the endpoint and [`ZmqProvider::subscriber`] connects to it. The
//! asymmetry is ZeroMQ's, and it is why there are two constructors: getting the
//! role wrong in ZeroMQ is a silent no-op, so the choice is made where it can be
//! seen. A provider built as a subscriber refuses to publish rather than drop
//! the message.
//!
//! # Declarations are local
//!
//! Streams live in this process because ZeroMQ has nowhere else to put them:
//! two processes each declare their own, and a restart forgets. What that buys
//! is the subject check. The stream id is part of the wire topic, so both sides
//! must declare the stream under the same id.
//!
//! # Wire layout
//!
//! Two frames, identical to runtime-go: the topic `"<stream id>|<subject>"`,
//! then the [`pack`](crate::provider::pack)ed payload. The topic is a frame of
//! its own so a SUB socket filters on it without touching the payload.

mod manager;

use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
    time::Duration,
};

use tokio::sync::{Mutex, watch};
use zeromq::{PubSocket, Socket};

use crate::{Manager, Registry, Result, Stream, StreamError, Streams, provider};

use manager::ZmqManager;

/// How long [`Subscriber::subscribe`](crate::Subscriber::subscribe) waits for a
/// subscription to reach the publisher when [`ZmqConfig::settle`] is not
/// changed.
///
/// It is a guess at a round trip, because ZeroMQ offers nothing better to wait
/// on: long enough for a local socket or a LAN, and runtime-go's default.
pub const DEFAULT_SETTLE: Duration = Duration::from_millis(250);

/// Settings for a [`ZmqProvider`].
#[derive(Clone, Debug)]
pub struct ZmqConfig {
    /// A ZeroMQ transport string: `tcp://127.0.0.1:5563` or `ipc:///tmp/feed`.
    ///
    /// A publisher may bind port 0 and read the port the system chose from
    /// [`ZmqProvider::endpoint`]. The `zeromq` crate has no `inproc` transport.
    pub endpoint: String,

    /// How long a subscribe waits before returning, for the subscription to
    /// reach the publisher.
    ///
    /// It is a delay rather than a handshake because ZeroMQ does not
    /// acknowledge a subscription; raising it narrows the slow-joiner window
    /// and never closes it.
    pub settle: Duration,

    /// The codecs a subscriber accepts. A frame naming any other is dropped
    /// with a warning rather than delivered as bytes nobody can read.
    pub registry: Registry,
}

impl Default for ZmqConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            settle: DEFAULT_SETTLE,
            registry: Registry::new(),
        }
    }
}

impl ZmqConfig {
    /// A configuration for `endpoint` with every other setting at its default.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            ..Self::default()
        }
    }

    /// Waits `settle` after each subscribe; see [`ZmqConfig::settle`].
    pub fn with_settle(mut self, settle: Duration) -> Self {
        self.settle = settle;
        self
    }

    /// Accepts frames written by the codecs in `registry`.
    pub fn with_registry(mut self, registry: Registry) -> Self {
        self.registry = registry;
        self
    }
}

/// A [`Streams`] over one ZeroMQ endpoint, in the publisher or subscriber role.
///
/// The provider owns its sockets, because with no broker there is no client
/// for the caller to hand over: the publisher's bound socket, and one SUB
/// socket per live subscription. [`Streams::close`] releases all of them;
/// dropping a subscription releases its own.
pub struct ZmqProvider {
    shared: Arc<Shared>,
}

/// State every manager bound from one provider shares.
pub(crate) struct Shared {
    /// The endpoint as resolved by the bind, so a port of 0 reads back as the
    /// port the system chose.
    pub(crate) endpoint: String,
    pub(crate) settle: Duration,
    pub(crate) registry: Registry,
    pub(crate) role: Role,
    declared: RwLock<BTreeMap<String, Stream>>,
    /// Flips to `true` once on close. Every receive task watches it, which is
    /// how close reaches subscriptions the caller is still holding.
    pub(crate) closed: watch::Sender<bool>,
}

/// Which end of the PUB/SUB pair a provider is.
pub(crate) enum Role {
    /// Binds and sends. The socket is taken on close, after which publishing
    /// fails rather than sending into a closed socket. It sits behind an async
    /// mutex because sending needs `&mut`.
    Publisher(Mutex<Option<PubSocket>>),
    /// Connects and receives; cannot send.
    Subscriber,
}

impl ZmqProvider {
    /// Binds `config.endpoint` and returns a provider that publishes on it.
    ///
    /// One process in a PUB/SUB topology binds and the rest connect; this is
    /// the one that binds.
    pub async fn publisher(config: ZmqConfig) -> Result<Self> {
        check_endpoint(&config.endpoint)?;
        let mut socket = PubSocket::new();
        let bound = socket.bind(&config.endpoint).await.map_err(|error| {
            StreamError::internal(&format!("zeromq: cannot bind {}", config.endpoint), error)
        })?;
        let endpoint = bound.to_string();
        tracing::info!(endpoint, "zeromq: publishing");
        Ok(Self::build(
            endpoint,
            config,
            Role::Publisher(Mutex::new(Some(socket))),
        ))
    }

    /// Returns a provider that connects to `config.endpoint` and receives
    /// from it.
    ///
    /// Nothing is dialled until the first subscribe, which opens a socket of its
    /// own. Publishing through a manager from here fails with
    /// [`StreamError::Unsupported`]: a SUB socket cannot send, and refusing
    /// beats accepting a message that would go nowhere.
    pub fn subscriber(config: ZmqConfig) -> Result<Self> {
        check_endpoint(&config.endpoint)?;
        let endpoint = config.endpoint.clone();
        tracing::info!(endpoint, "zeromq: subscribing");
        Ok(Self::build(endpoint, config, Role::Subscriber))
    }

    fn build(endpoint: String, config: ZmqConfig, role: Role) -> Self {
        Self {
            shared: Arc::new(Shared {
                endpoint,
                settle: config.settle,
                registry: config.registry,
                role,
                declared: RwLock::new(BTreeMap::new()),
                closed: watch::Sender::new(false),
            }),
        }
    }

    /// The endpoint this provider binds or connects to, with a bound port of 0
    /// resolved to the real one.
    pub fn endpoint(&self) -> &str {
        &self.shared.endpoint
    }

    fn declared(&self) -> std::sync::RwLockReadGuard<'_, BTreeMap<String, Stream>> {
        // The map is only ever replaced whole-entry, so a writer that panicked
        // cannot have left it half-updated; recovering beats poisoning every
        // later call.
        self.shared
            .declared
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn declared_mut(&self) -> std::sync::RwLockWriteGuard<'_, BTreeMap<String, Stream>> {
        self.shared
            .declared
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Shared {
    pub(crate) fn is_closed(&self) -> bool {
        *self.closed.borrow()
    }
}

fn check_endpoint(endpoint: &str) -> Result<()> {
    if endpoint.is_empty() {
        return Err(StreamError::Internal("zeromq: no endpoint given".into()));
    }
    Ok(())
}

fn not_found(id: &str) -> StreamError {
    StreamError::NotFound(format!("stream {id}"))
}

/// The declaration a stream is stored as: the caller's fields under `id`,
/// active, as runtime-go stores it.
fn declaration(id: String, stream: Stream) -> Stream {
    Stream {
        id,
        active: true,
        ..stream
    }
}

#[async_trait::async_trait]
impl Streams for ZmqProvider {
    async fn create(&self, stream: Stream) -> Result<Stream> {
        let id = if stream.id.is_empty() {
            provider::new_stream_id(&stream.name)
        } else {
            stream.id.clone()
        };
        let out = declaration(id.clone(), stream);
        self.declared_mut().insert(id.clone(), out.clone());
        tracing::info!(id, name = out.name, subjects = ?out.subjects, "zeromq: stream declared");
        Ok(out)
    }

    async fn get(&self, id: &str) -> Result<Stream> {
        self.declared()
            .get(id)
            .cloned()
            .ok_or_else(|| not_found(id))
    }

    async fn bind(&self, id: &str) -> Result<Arc<dyn Manager>> {
        let stream = self.get(id).await?;
        Ok(Arc::new(ZmqManager::new(self.shared.clone(), stream)))
    }

    async fn update(&self, id: &str, stream: Stream) -> Result<Stream> {
        let mut declared = self.declared_mut();
        let slot = declared.get_mut(id).ok_or_else(|| not_found(id))?;
        *slot = declaration(id.to_owned(), stream);
        Ok(slot.clone())
    }

    async fn delete(&self, id: &str) -> Result<()> {
        self.declared_mut()
            .remove(id)
            .map(drop)
            .ok_or_else(|| not_found(id))
    }

    async fn list(&self) -> Result<Vec<Stream>> {
        // A BTreeMap already iterates in id order, the order runtime-go sorts to.
        Ok(self.declared().values().cloned().collect())
    }

    /// Ends every live subscription from this provider and unbinds the
    /// publisher's socket. Calling it again does nothing.
    async fn close(&self) -> Result<()> {
        if self.shared.closed.send_replace(true) {
            return Ok(());
        }
        let Role::Publisher(socket) = &self.shared.role else {
            return Ok(());
        };
        let Some(socket) = socket.lock().await.take() else {
            return Ok(());
        };
        let errors = socket.close().await;
        match errors.first() {
            None => Ok(()),
            Some(error) => Err(StreamError::internal("zeromq: cannot close", error)),
        }
    }
}
