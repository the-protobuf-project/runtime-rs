//! Configuration for the [`crate::HybridServer`]: service identity, the gRPC/HTTP/MCP/A2A
//! endpoints, the environment mode, and the health and HTTP/3 toggles.
//!
//! Build an [`Options`] value and hand it to [`crate::HybridServer::new`].
//!
//! ```
//! use grpc::options::{Endpoint, Options, ServerEnvironment};
//!
//! let opts = Options {
//!     service_name: "bookstore".into(),
//!     environment: ServerEnvironment::Production,
//!     grpc: Endpoint::new("0.0.0.0", 50051),
//!     http: Endpoint::new("0.0.0.0", 8080),
//!     enable_http: true,
//!     enable_health: true,
//!     ..Default::default()
//! };
//! # let _ = opts;
//! ```

mod agents;
mod environment;
pub mod opentelemetry;

pub use agents::{A2aOptions, A2aTransport, DEFAULT_A2A_BASE_PATH, McpOptions, McpTransport};
pub use environment::ServerEnvironment;

use std::fmt;

/// A network host and port a component listens on.
///
/// Go declares `GRPCOptions` and `HTTPOptions` separately with identical shapes; the two
/// names below are aliases of this one type, so either reads naturally at a call site.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Endpoint {
    /// The network interface to listen on.
    pub host: String,
    /// The port number. Zero asks the operating system for a free port, and the one it
    /// chose is reported by [`crate::HybridServer::grpc_addr`],
    /// [`crate::HybridServer::http_addr`] and [`crate::HybridServer::agent_endpoints`].
    ///
    /// The default ports are not expressed by zero: they are what [`Options::default`]
    /// starts from.
    pub port: u16,
}

impl Endpoint {
    /// A host and port together.
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    /// The `host:port` form a listener or dial target wants.
    ///
    /// An IPv6 host is bracketed — `[::1]:50051` — because its own colons would otherwise
    /// make the port ambiguous and the address unparseable.
    pub fn addr(&self) -> String {
        self.to_string()
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') && !self.host.starts_with('[') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

/// The network host and port for the gRPC server.
pub type GrpcOptions = Endpoint;
/// The network host and port for the HTTP gateway.
pub type HttpOptions = Endpoint;

/// The complete set of configuration options for a [`crate::HybridServer`].
///
/// [`Options::default`] carries the default gRPC and HTTP ports, 65020 and 6502, so a
/// caller who names neither gets those, while port 0 stays free to mean "any free port".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Options {
    /// The service name, also used to build the environment-variable override prefix.
    pub service_name: String,
    /// A brief description of the service.
    pub description: String,
    /// The service version string.
    pub version: String,

    /// The gRPC server endpoint.
    pub grpc: GrpcOptions,
    /// The HTTP gateway endpoint.
    pub http: HttpOptions,
    /// The MCP server. Its port 0 means one past the HTTP port, or a free port when the
    /// HTTP port is itself 0.
    pub mcp: McpOptions,
    /// The Agent2Agent server. Its port 0 means one past the MCP port, or a free port when
    /// the HTTP and MCP ports are both 0.
    pub a2a: A2aOptions,

    /// Whether the HTTP gateway is started.
    pub enable_http: bool,
    /// Whether the standard gRPC health checking service is registered.
    pub enable_health: bool,
    /// Whether the MCP server is started.
    pub enable_mcp: bool,
    /// Whether the Agent2Agent server is started.
    pub enable_a2a: bool,

    /// The server's operating mode.
    pub environment: ServerEnvironment,

    /// Enables experimental HTTP/3 on `http.port + 1`. Requires a TLS certificate.
    pub experimental_http3: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            service_name: String::new(),
            description: String::new(),
            version: String::new(),
            grpc: Endpoint::new("", crate::validate::DEFAULT_GRPC_PORT),
            http: Endpoint::new("", crate::validate::DEFAULT_HTTP_PORT),
            mcp: McpOptions::default(),
            a2a: A2aOptions::default(),
            enable_http: false,
            enable_health: false,
            enable_mcp: false,
            enable_a2a: false,
            environment: ServerEnvironment::default(),
            experimental_http3: false,
        }
    }
}
