//! The vocabulary a [`crate::Runtime`] and the protocol crates share: what a service
//! speaks, what it needs, and where the runtime decided to put it.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use crate::error::ServiceError;

// Re-exported so `crate::agents::Config` and friends keep naming one place, wherever the
// definitions actually live.
pub use crate::config::Config;
pub use crate::mux::{GrpcRoutes, Mux};

/// Names what a [`Service`] speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Protocol {
    /// The Model Context Protocol — a model reaching your tools.
    Mcp,
    /// Agent2Agent — another agent delegating a task to yours.
    A2a,
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Mcp => "mcp",
            Self::A2a => "a2a",
        })
    }
}

/// Where the shared listener binds when a config names no host.
pub const DEFAULT_HOST: &str = "0.0.0.0";

/// Bounds the wait for a service to mount itself. Generous because mounting is not
/// supposed to take measurable time — a service that hits this is stuck, not slow.
pub const DEFAULT_READY_TIMEOUT: Duration = Duration::from_secs(5);

/// Bounds the drain of a listener the runtime owns.
pub const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// What a service calls itself, passed to every protocol so they all say the same thing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Identity {
    /// The service name.
    pub name: String,
    /// A one-line description.
    pub description: String,
    /// The version string.
    pub version: String,
}

/// What a [`Service`] needs from the runtime placing it. The default asks for nothing,
/// which is what a service that neither listens nor registers would need — and there is no
/// such service, so every implementation sets at least one field.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Requirements {
    /// A listen address this service must have to itself, as `host:port`. `None` means it
    /// shares the runtime's, which is the usual answer: two protocols on one port under
    /// different base paths is the whole reason a runtime groups them.
    ///
    /// Services naming the same address share one listener and one router with each other,
    /// so a caller separating protocols by port gets exactly the ports it asked for.
    pub addr: Option<String>,

    /// Whether this service mounts HTTP handlers.
    ///
    /// Asked rather than assumed because not every protocol listens. A2A serving only its
    /// gRPC binding mounts nothing, and a listener opened for it would bind a port with
    /// nothing behind it.
    pub http: bool,

    /// Whether this service registers on [`Config::grpc_routes`]. A runtime with no
    /// registry refuses a service that needs one.
    pub grpc: bool,
}

/// Where the runtime decided a service sits. A service is handed one and mounts itself
/// into it; it does not choose.
#[derive(Debug, Clone)]
pub struct Placement {
    /// The runtime's identity, unchanged. Every protocol reports the same.
    pub identity: Identity,

    /// The address the listener for this service's group binds, as `host:port`. It is what
    /// a protocol should advertise when it has no [`Placement::public_url`].
    pub addr: String,

    /// Where HTTP handlers go. Shared with every sibling at the same address, so a service
    /// must mount under a path of its own.
    pub mux: Mux,

    /// Where a gRPC binding registers, and `None` when the runtime has no registry.
    pub grpc_routes: Option<GrpcRoutes>,

    /// The runtime's public URL, for protocols that advertise an address.
    pub public_url: String,
}

/// One place a protocol answers, as it should be reported to a human reading a startup
/// summary or to a client reading a manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    /// What answers here.
    pub protocol: Protocol,
    /// The binding within that protocol — `streamable-http`, `stdio`, `jsonrpc`, `grpc`.
    pub transport: String,
    /// Where a client connects. For a gRPC binding this is a `host:port` dial target rather
    /// than a URL, which is what a gRPC client expects.
    pub url: String,
    /// Anything else worth printing: the agent card's address, the base path a tool server
    /// mounted under. Free text, for people.
    pub detail: String,
}

/// The handle a service reports itself mounted through.
///
/// It replaces the `ready func([]Endpoint)` callback Go passes to `Serve`. Reporting more
/// than once is a no-op rather than an error, which is what Go's `sync.Once` around the
/// same callback buys.
pub struct Ready {
    tx: Mutex<Option<oneshot::Sender<Vec<Endpoint>>>>,
}

impl Ready {
    pub(crate) fn new() -> (Arc<Self>, oneshot::Receiver<Vec<Endpoint>>) {
        let (tx, rx) = oneshot::channel();
        (
            Arc::new(Self {
                tx: Mutex::new(Some(tx)),
            }),
            rx,
        )
    }

    /// Reports this service mounted, on the endpoints it answers.
    ///
    /// It must be called exactly once, before [`Service::serve`] blocks: the runtime opens
    /// listeners only after every service has mounted, so a service that never reports
    /// ready holds up the start and one that reports late is not listened to.
    pub fn ready(&self, endpoints: Vec<Endpoint>) {
        if let Some(tx) = self.tx.lock().expect("agents: ready poisoned").take() {
            let _ = tx.send(endpoints);
        }
    }
}

impl fmt::Debug for Ready {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ready").finish_non_exhaustive()
    }
}

/// One protocol registered on a [`crate::Runtime`].
///
/// It is a trait rather than a struct because the two protocols share nothing but their
/// shape: MCP serves generated functions against a config it builds, A2A serves an executor
/// over transports it resolves. What they have in common is exactly this — they can say
/// what they need, and they can mount themselves into what they are given.
#[async_trait::async_trait]
pub trait Service: Send + Sync + 'static {
    /// Names what this speaks.
    fn protocol(&self) -> Protocol;

    /// Reports what this service needs from the runtime.
    fn requires(&self) -> Requirements;

    /// Mounts the protocol into `placement` and blocks until `cancel` fires.
    ///
    /// It must call [`Ready::ready`] before it blocks. Returning an error before then fails
    /// the start; returning one after is logged by the runtime and ends that service alone.
    async fn serve(
        &self,
        cancel: CancellationToken,
        placement: Placement,
        ready: Arc<Ready>,
    ) -> Result<(), ServiceError>;
}
