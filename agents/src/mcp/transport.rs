//! The transports an MCP server answers on.

/// The transport protocol for an MCP server.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum Transport {
    /// The modern Streamable HTTP transport.
    #[default]
    StreamableHttp,
    /// The legacy HTTP+SSE transport from the 2024-11-05 spec, served at the base path plus
    /// `/` as Go mounts it.
    Sse,
    /// Runs over the process's own stdin and stdout, for an IDE launching a subprocess.
    Stdio,
}

impl Transport {
    /// The wire name, matching the Go constant's string value.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::StreamableHttp => "streamable-http",
            Self::Sse => "sse",
            Self::Stdio => "stdio",
        }
    }

    /// Whether this transport needs a listener. Stdio is the one that does not.
    pub fn listens(&self) -> bool {
        !matches!(self, Self::Stdio)
    }
}

/// A transport name this runtime does not speak.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("runtime: unsupported transport {0:?}")]
pub struct UnsupportedTransport(pub String);

impl std::str::FromStr for Transport {
    type Err = UnsupportedTransport;

    /// Parses the exact wire name, as Go compares it. There are no aliases and no case
    /// folding, so a value the Go server refuses is refused here too.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "streamable-http" => Ok(Self::StreamableHttp),
            "sse" => Ok(Self::Sse),
            "stdio" => Ok(Self::Stdio),
            other => Err(UnsupportedTransport(other.to_string())),
        }
    }
}
