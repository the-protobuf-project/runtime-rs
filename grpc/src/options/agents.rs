//! The two agent-facing components' settings: which transports they speak, and where.

use std::fmt;
use std::str::FromStr;

/// The transport protocol for the MCP server.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum McpTransport {
    /// Runs the MCP server over standard input/output.
    Stdio,
    /// Runs the MCP server over Streamable HTTP.
    #[default]
    StreamableHttp,
    /// Runs the MCP server over Server-Sent Events.
    Sse,
}

impl McpTransport {
    /// The wire name, matching the Go constant's string value.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Stdio => "stdio",
            Self::StreamableHttp => "streamable-http",
            Self::Sse => "sse",
        }
    }

    /// Whether this transport needs a listener. Stdio is the one that does not: it talks
    /// over the process's own pipes.
    pub fn listens(&self) -> bool {
        !matches!(self, Self::Stdio)
    }
}

impl fmt::Display for McpTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for McpTransport {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "stdio" => Ok(Self::Stdio),
            "streamable-http" | "streamable_http" | "http" => Ok(Self::StreamableHttp),
            "sse" => Ok(Self::Sse),
            other => Err(crate::Error::UnknownMcpTransport(other.to_string())),
        }
    }
}

/// The configuration for the MCP server.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct McpOptions {
    /// The interface the MCP server listens on. Ignored for stdio. Defaults to `0.0.0.0`.
    pub host: String,
    /// The port for HTTP-based transports. Ignored for stdio. Defaults to `http.port + 1`.
    pub port: u16,
    /// The transport. Defaults to [`McpTransport::StreamableHttp`].
    pub transport: Option<McpTransport>,
}

/// A transport protocol for the Agent2Agent server.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum A2aTransport {
    /// Serves A2A as JSON-RPC 2.0 over HTTP, with server-sent events for streaming. Every
    /// A2A client speaks it.
    #[default]
    JsonRpc,
    /// Serves A2A as a gRPC service on the hybrid server's own gRPC port, alongside the
    /// process's other services.
    Grpc,
    /// Serves the HTTP+JSON binding beneath the base path.
    Rest,
}

impl A2aTransport {
    /// The wire name, matching the Go constant's string value.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::JsonRpc => "jsonrpc",
            Self::Grpc => "grpc",
            Self::Rest => "rest",
        }
    }
}

impl fmt::Display for A2aTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for A2aTransport {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "jsonrpc" => Ok(Self::JsonRpc),
            "grpc" => Ok(Self::Grpc),
            "rest" => Ok(Self::Rest),
            other => Err(crate::Error::UnknownA2aTransport(other.to_string())),
        }
    }
}

/// Where JSON-RPC mounts and the prefix REST is served under.
pub const DEFAULT_A2A_BASE_PATH: &str = "/a2a";

/// The configuration for the Agent2Agent server.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct A2aOptions {
    /// The interface the HTTP-based transports listen on. Defaults to `0.0.0.0`.
    pub host: String,
    /// The port for the HTTP-based transports (jsonrpc, rest). Ignored by the gRPC
    /// transport, which shares the gRPC server's port. Defaults to `mcp.port + 1`.
    pub port: u16,
    /// The bindings the agent answers on. Empty defaults to [`A2aTransport::JsonRpc`].
    ///
    /// Go carries both a singular `Transport` and a plural `Transports`, with the plural
    /// winning when both are set. One list says the same thing without the tie-break.
    pub transports: Vec<A2aTransport>,
    /// Where JSON-RPC mounts and the prefix REST is served under. Defaults to
    /// [`DEFAULT_A2A_BASE_PATH`].
    pub base_path: String,
    /// The base URL clients should be told to use, for a server behind a proxy or inside a
    /// container. Without it the agent card advertises the listen address, which is wrong
    /// past any hop.
    pub public_url: String,
}
