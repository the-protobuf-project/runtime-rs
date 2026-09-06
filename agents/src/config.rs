//! A runtime's settings, and the defaults it fills in.

use std::fmt;
use std::time::Duration;

use crate::agents::Identity;
use crate::mux::{GrpcRoutes, Mux};

/// A runtime's settings. Only the identity fields have no working default, because an
/// agent or tool server that will not say what it is cannot be described to a client.
#[derive(Default)]
pub struct Config {
    /// The identity every registered protocol advertises. Declaring it once is the point:
    /// an MCP server and an A2A card in the same process describing themselves differently
    /// is a bug nobody notices until a client is confused by it.
    pub identity: Identity,

    /// Where the shared listener binds. Services that name no address of their own answer
    /// here, each under its own base path.
    pub host: String,
    /// The shared listener's port.
    pub port: u16,

    /// The base URL clients should be told to use, for a process behind a proxy or inside
    /// a container. Protocols that advertise their own address use it in place of the
    /// listen address, which is right locally and wrong past any hop.
    pub public_url: String,

    /// When set, every sharing service mounts here instead of onto a listener this runtime
    /// opens. The host owns the server then, and [`crate::Runtime::start`] binds nothing.
    pub mux: Option<Mux>,

    /// Where protocols with a gRPC binding register. A2A's gRPC transport needs it; MCP
    /// does not use it.
    ///
    /// The routes must still be un-served: a tonic server takes its routes by value when it
    /// starts, so a runtime sharing a registry has to finish starting before the host reads
    /// it out.
    pub grpc_routes: Option<GrpcRoutes>,

    /// Bounds reads on the listeners this runtime owns. `None` means no limit, which is
    /// what streaming protocols need — an agent working for a minute before its first
    /// artifact would otherwise be cut off.
    pub read_timeout: Option<Duration>,
    /// Bounds writes on the listeners this runtime owns. `None` means no limit.
    pub write_timeout: Option<Duration>,

    /// How long [`crate::Runtime::start`] waits for one service to report itself mounted.
    /// `None` means [`DEFAULT_READY_TIMEOUT`].
    pub ready_timeout: Option<Duration>,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("identity", &self.identity)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("public_url", &self.public_url)
            .field("mux", &self.mux.is_some())
            .field("grpc_routes", &self.grpc_routes.is_some())
            .field("ready_timeout", &self.ready_timeout)
            .finish()
    }
}
