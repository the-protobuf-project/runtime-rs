//! The Model Context Protocol runtime: a tool server as one [`crate::Service`], plus the
//! vocabulary generated code is written against.
//!
//! It wraps the official [`rmcp`](https://docs.rs/rmcp) SDK. The protocol, the schemas and the
//! dispatch are the SDK's; what this module adds is two things — placement, so a tool server
//! shares a listener with the other protocols instead of binding one of its own, and the
//! helpers `protoc-gen-mcp` output calls into.
//!
//! # For generated code
//!
//! A generated `foo_service.mcp.rs` builds its tools from the schemas the plugin derived from
//! the proto, then hands the handler to the runtime:
//!
//! ```no_run
//! # use agents::{Config, Runtime, mcp};
//! # #[derive(Clone)] struct FooServiceMcpHandler;
//! # impl rmcp::ServerHandler for FooServiceMcpHandler {}
//! # async fn example(handler: FooServiceMcpHandler) -> agents::Result<()> {
//! let runtime = Runtime::new(Config { port: 9000, ..Default::default() })
//!     .register(mcp::Service::from_handler(handler).base_path("/foo/v1/fooservice/mcp"));
//!
//! runtime.serve(tokio_util::sync::CancellationToken::new()).await
//! # }
//! ```
//!
//! The pieces it uses along the way are all here: [`create_tool`] and [`parse_schema`] for the
//! tool table, [`text_result`] and [`structured_result`] for what a call returns,
//! [`handle_error`] for when the RPC behind it fails, [`ProgressSink`] for a streaming call,
//! [`elicit`] for one that needs to ask the user something, and [`Interceptors`] so all of it
//! runs the same middleware as the wire RPC.

mod cache;
mod completion;
mod elicitation;
mod error;
mod extras;
mod interceptor;
mod progress;
mod resources;
mod serve;
mod service;
mod tool;
mod transport;

pub use crate::shared::status_name;
pub use cache::{CacheHint, CacheHints, CacheScope};
pub use completion::EnumCompletions;
pub use elicitation::{ElicitField, elicit, elicit_schema, merge_elicit_result};
pub use error::{GrpcError, handle_error};
pub use extras::{ExtraProperty, extract_extras};
pub use interceptor::{Interceptors, ToolHandler, ToolInterceptor};
pub use progress::ProgressSink;
pub use resources::{
    app_resource, app_resource_uri, default_app_html, default_app_resource_result,
    default_prompt_result, default_resource_result,
};
pub use service::Service;
pub use tool::{
    create_tool, error_result, parse_schema, prepare_tool_with_extras, set_tool_app_meta,
    structured_result, text_result, with_output_schema,
};
pub use transport::Transport;

/// Where a tool server mounts when nothing overrides it.
///
/// Generated code carries its own proto-derived path and passes it to
/// [`Service::base_path`], so this is the fallback for a hand-written server.
pub const DEFAULT_BASE_PATH: &str = "/mcp";
