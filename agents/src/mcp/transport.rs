//! The transports an MCP server answers on.

/// The transport protocol for an MCP server.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum Transport {
    /// The modern Streamable HTTP transport.
    #[default]
    StreamableHttp,
    /// The legacy SSE transport from the 2024-11-05 spec.
    ///
    /// rmcp folds SSE into the Streamable HTTP service — that transport already answers with
    /// `text/event-stream` when a response streams — so asking for this serves the same
    /// endpoint rather than a second, separate one.
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

impl std::str::FromStr for Transport {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "streamable-http" | "streamable_http" | "http" => Ok(Self::StreamableHttp),
            "sse" => Ok(Self::Sse),
            "stdio" => Ok(Self::Stdio),
            _ => Err(()),
        }
    }
}
