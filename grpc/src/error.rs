//! The single error type every fallible operation in this crate returns.

/// Every error a [`crate::HybridServer`] can produce.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// [`crate::options::Options::grpc`] named no host.
    #[error("grpc.host must not be empty")]
    EmptyGrpcHost,

    /// The HTTP gateway is enabled and [`crate::options::Options::http`] named no host.
    #[error("http.host must not be empty if the HTTP gateway is enabled")]
    EmptyHttpHost,

    /// A port resolved to zero after defaults were applied, which binds an arbitrary port
    /// rather than the one a client was told about.
    #[error("{component}.port must be between 1 and 65535")]
    InvalidPort {
        /// Which component's port was out of range.
        component: &'static str,
    },

    /// HTTP/3 was enabled without TLS. QUIC has no plaintext mode, so this cannot be
    /// started rather than merely degraded.
    #[error("a TLS certificate is required when experimental HTTP/3 support is enabled")]
    Http3NeedsTls,

    /// A certificate or key file could not be read.
    #[error("failed to read {path}: {source}")]
    ReadCertificate {
        /// The file that could not be read.
        path: String,
        /// The underlying I/O failure.
        #[source]
        source: std::io::Error,
    },

    /// A certificate or key file was read but held no usable PEM material.
    #[error("no {kind} found in {path}")]
    EmptyCertificate {
        /// Either `certificate` or `private key`.
        kind: &'static str,
        /// The file that was parsed.
        path: String,
    },

    /// rustls rejected the certificate and key pair.
    #[error("invalid TLS certificate or key: {0}")]
    Tls(#[from] rustls::Error),

    /// Binding a listener failed.
    #[error("failed to listen on {addr}: {source}")]
    Listen {
        /// The address that could not be bound.
        addr: String,
        /// The underlying bind failure.
        #[source]
        source: std::io::Error,
    },

    /// A registered gateway handler refused to mount.
    #[error("failed to register HTTP gateway: {0}")]
    Gateway(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// The transport underneath tonic failed.
    #[error("gRPC transport: {0}")]
    Transport(#[from] tonic::transport::Error),

    /// The agent runtime refused to start, or failed to drain.
    #[error("agent protocols: {0}")]
    Agents(#[from] agents::Error),

    /// An environment variable or option named a transport this server does not speak.
    #[error("unknown MCP transport: {0}")]
    UnknownMcpTransport(String),

    /// An environment variable or option named an A2A transport this server does not speak.
    #[error("unknown A2A transport: {0}")]
    UnknownA2aTransport(String),

    /// An environment variable named an environment that is not one of the four.
    #[error("unknown environment: {0}")]
    UnknownEnvironment(String),

    /// A stop did not finish within the shutdown timeout; what was still running was cut
    /// off.
    #[error("shutdown did not finish within {0:?}")]
    ShutdownTimeout(std::time::Duration),

    /// [`crate::CorsPolicy::validate`] refused the policy given to
    /// [`crate::HybridServer::with_cors`].
    #[error("cors: {0}")]
    InvalidCors(String),

    /// An I/O failure with no more specific home.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// HTTP/3 failed to start or serve.
    #[cfg(feature = "http3")]
    #[error("HTTP/3: {0}")]
    Http3(String),
}

/// The result type every fallible operation in this crate returns.
pub type Result<T, E = Error> = std::result::Result<T, E>;
