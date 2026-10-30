//! The bindings an agent answers on, and how each one names itself on a card.

use a2a::{TRANSPORT_PROTOCOL_GRPC, TRANSPORT_PROTOCOL_HTTP_JSON, TRANSPORT_PROTOCOL_JSONRPC};

/// A transport binding an agent answers on.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum Transport {
    /// JSON-RPC 2.0 over HTTP, with server-sent events for streaming. Every A2A client is
    /// required to support it, which is why it is the default.
    #[default]
    JsonRpc,
    /// A gRPC service, registered on the runtime's own route registry rather than on a
    /// listener of its own.
    Grpc,
    /// The HTTP+JSON binding, served beneath the base path.
    Rest,
}

impl Transport {
    /// The wire name, matching the Go constant's string value.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::JsonRpc => "jsonrpc",
            Self::Grpc => "grpc",
            Self::Rest => "rest",
        }
    }

    /// The name this binding goes by on a card.
    pub fn protocol_binding(&self) -> &'static str {
        match self {
            Self::JsonRpc => TRANSPORT_PROTOCOL_JSONRPC,
            Self::Grpc => TRANSPORT_PROTOCOL_GRPC,
            Self::Rest => TRANSPORT_PROTOCOL_HTTP_JSON,
        }
    }

    /// Whether this binding needs an HTTP listener. gRPC is the one that does not: it
    /// registers on the runtime's server instead.
    pub fn listens(&self) -> bool {
        !matches!(self, Self::Grpc)
    }
}

impl std::str::FromStr for Transport {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "jsonrpc" => Ok(Self::JsonRpc),
            "grpc" => Ok(Self::Grpc),
            "rest" => Ok(Self::Rest),
            _ => Err(()),
        }
    }
}

/// Splits a comma-separated transport list, as an environment variable would carry it.
///
/// Unknown and empty entries are dropped rather than rejected, so one bad value does not
/// take the process down; a list that resolves to nothing falls back to the default.
pub fn parse_transports(raw: &str) -> Vec<Transport> {
    raw.split(',')
        .filter_map(|part| part.parse::<Transport>().ok())
        .collect()
}
