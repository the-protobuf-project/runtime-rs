//! The Model Context Protocol runtime: a tool server as one [`crate::Service`].
//!
//! It wraps the official [`rmcp`](https://docs.rs/rmcp) SDK. The protocol, the tool schemas
//! and the dispatch are the SDK's; what this module adds is placement — a tool server
//! registered here gets its identity from the runtime and mounts under a base path on
//! whatever listener the runtime decided it shares.

mod serve;
mod service;
mod transport;

pub use service::Service;
pub use transport::Transport;

/// Where a tool server mounts when nothing overrides it.
pub const DEFAULT_BASE_PATH: &str = "/mcp";
