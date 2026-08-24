//! The errors a [`crate::Runtime`] returns, nearly all of them before it serves anything.

use crate::Protocol;

/// A service's own failure, kept untyped so a protocol crate can return whatever it
/// wants without this crate knowing about it — the Rust home of Go's bare `error`
/// return from `Service.Serve`.
pub type ServiceError = Box<dyn std::error::Error + Send + Sync>;

/// Every error a [`crate::Runtime`] produces.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// [`crate::Runtime::start`] was called with nothing registered. A runtime with no
    /// services would bind a port and answer nothing, which is worth failing on rather
    /// than discovering from the other end.
    #[error("agents: no services registered")]
    NoServices,

    /// A second [`crate::Runtime::start`]. Starting twice would mount every service
    /// again onto routers that already have them.
    #[error("agents: runtime already started")]
    AlreadyStarted,

    /// A registered service needs [`crate::Config::grpc_routes`] and the runtime has none.
    /// Refused up front rather than started into a missing registry.
    #[error("agents: {0} needs a gRPC route registry and Config::grpc_routes is None")]
    NeedsGrpcRoutes(Protocol),

    /// A service did not report itself mounted inside [`crate::Config::ready_timeout`].
    /// Mounting is not supposed to take measurable time, so this means stuck, not slow.
    #[error("agents: {protocol} did not mount within {timeout:?}")]
    MountTimeout {
        /// The protocol that never reported ready.
        protocol: Protocol,
        /// The budget it overran.
        timeout: std::time::Duration,
    },

    /// A service returned before ever reporting ready, without an error of its own.
    #[error("agents: {0} stopped before it mounted")]
    StoppedBeforeMount(Protocol),

    /// A service failed on the way up.
    #[error("agents: {protocol} failed to start: {source}")]
    FailedToStart {
        /// The protocol that failed.
        protocol: Protocol,
        /// What it reported.
        #[source]
        source: ServiceError,
    },

    /// Binding a listener this runtime owns failed.
    #[error("agents: listening on {addr}: {source}")]
    Listen {
        /// The address that could not be bound.
        addr: String,
        /// The underlying bind failure.
        #[source]
        source: std::io::Error,
    },

    /// The context governing the start was cancelled before every service mounted.
    #[error("agents: cancelled before every service mounted")]
    Cancelled,

    /// Draining a listener this runtime owns failed, or timed out.
    #[error("agents: draining {addr}: {message}")]
    Drain {
        /// The listener that would not drain.
        addr: String,
        /// What went wrong.
        message: String,
    },

    /// Several failures at once, from a shutdown that drained more than one listener.
    /// The Rust stand-in for Go's `errors.Join`.
    #[error("agents: {} errors during shutdown: {}", .0.len(), .0.iter().map(ToString::to_string).collect::<Vec<_>>().join("; "))]
    Multiple(Vec<Error>),
}

/// The result type every fallible runtime operation returns.
pub type Result<T, E = Error> = std::result::Result<T, E>;
