use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use crate::error::Error;

/// Identifies which kind of network client [`crate::Network::new_connection`] creates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClientType {
    /// A GraphQL API client ([`crate::GraphQLClient`]).
    GraphQL,
    /// An HTTP REST client ([`crate::HttpClient`]).
    Http,
    /// A WebSocket client ([`crate::WebSocketClient`]).
    WebSocket,
}

impl fmt::Display for ClientType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::GraphQL => "graphql",
            Self::Http => "http",
            Self::WebSocket => "websocket",
        })
    }
}

impl FromStr for ClientType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "graphql" => Ok(Self::GraphQL),
            "http" => Ok(Self::Http),
            "websocket" => Ok(Self::WebSocket),
            other => Err(Error::UnsupportedClientType(other.to_string())),
        }
    }
}

/// The protocol part of a URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum UrlScheme {
    /// Plain HTTP.
    #[default]
    Http,
    /// TLS-secured HTTP.
    Https,
    /// Plain WebSocket.
    Ws,
    /// TLS-secured WebSocket.
    Wss,
}

impl fmt::Display for UrlScheme {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Http => "http",
            Self::Https => "https",
            Self::Ws => "ws",
            Self::Wss => "wss",
        })
    }
}

impl FromStr for UrlScheme {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "http" => Ok(Self::Http),
            "https" => Ok(Self::Https),
            "ws" => Ok(Self::Ws),
            "wss" => Ok(Self::Wss),
            other => Err(Error::InvalidScheme(other.to_string())),
        }
    }
}

/// The target URL: scheme, host, path(s), and optional query parameters. `host` may include a
/// port (e.g. `"localhost:8080"`). `paths` is a list of path segments; a client selects one by
/// index (e.g. path index 0 for the first).
#[derive(Debug, Clone, Default)]
pub struct UrlOptions {
    /// The protocol: `http`, `https`, `ws`, or `wss`.
    pub scheme: UrlScheme,
    /// The hostname, optionally with a port (e.g. `"api.example.com:443"`).
    pub host: String,
    /// Candidate paths to choose from; a client selects one by index (e.g. path index 0 for the
    /// first) when building the full URL.
    pub paths: Vec<String>,
    /// Query parameters to append to the built URL.
    pub params: HashMap<String, String>,
}

/// Settings shared by all client types. Pass it to [`crate::Network::with_opts`] or to a client's
/// `connect` method.
#[derive(Clone, Default)]
pub struct ConnectionOptions {
    /// The target endpoint. For HTTP/GraphQL use http or https; for WebSocket use ws or wss.
    pub url: UrlOptions,
    /// Applies to connection establishment and to individual requests. If zero, [`DEFAULT_TIMEOUT`]
    /// is used.
    pub timeout: Duration,
    /// Sent on every request (and on the WebSocket handshake).
    pub headers: HashMap<String, String>,
    /// The maximum number of retries for HTTP requests.
    pub retries: usize,
    /// The pause between retries. If zero, a default of 2s is used.
    pub retry_delay: Duration,
    /// When true, skips the initial HTTP/GraphQL reachability check. Ignored for WebSocket, where
    /// the connection is established by the handshake itself.
    pub skip_connectivity_check: bool,
    /// Overrides the query used to verify a GraphQL server is reachable. If `None`,
    /// [`crate::graphql::DEFAULT_GRAPHQL_CONNECTIVITY_QUERY`] is used.
    pub graphql_connectivity_query: Option<String>,
    /// When set, injects the calling span's context into every outgoing GraphQL and HTTP request
    /// as headers, so a downstream service that continues the same propagator sees this request as
    /// a child span. `None` (the default) injects nothing.
    pub trace_propagator:
        Option<Arc<dyn opentelemetry::propagation::TextMapPropagator + Send + Sync>>,
}

impl fmt::Debug for ConnectionOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionOptions")
            .field("url", &self.url)
            .field("timeout", &self.timeout)
            .field("headers", &self.headers)
            .field("retries", &self.retries)
            .field("retry_delay", &self.retry_delay)
            .field("skip_connectivity_check", &self.skip_connectivity_check)
            .field(
                "graphql_connectivity_query",
                &self.graphql_connectivity_query,
            )
            .field("trace_propagator", &self.trace_propagator.is_some())
            .finish()
    }
}

/// Used when [`ConnectionOptions::timeout`] is zero. Applies to connection establishment, HTTP
/// requests, and GraphQL operations.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// A GraphQL `ID` scalar. GraphQL's `ID` type serializes as a JSON string on the wire even when
/// the underlying value looks numeric.
#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct Id(pub String);

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for Id {
    fn from(v: &str) -> Self {
        Self(v.to_string())
    }
}

impl From<String> for Id {
    fn from(v: String) -> Self {
        Self(v)
    }
}
