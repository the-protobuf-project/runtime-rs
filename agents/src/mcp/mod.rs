//! The Model Context Protocol runtime: a tool server as one [`crate::Service`], plus the
//! vocabulary generated code is written against.
//!
//! It wraps the official [`rmcp`](https://docs.rs/rmcp) SDK. The protocol, the schemas and the
//! dispatch are the SDK's; what this module adds is placement, so a tool server shares a
//! listener with the other protocols instead of binding one of its own, the settings a
//! hosted handler runs with, and the helpers `protoc-gen-mcp` output calls into.
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
//! # Differences from the Go original
//!
//! Go's settings reach generated code through `Config` and the SDK server's receiving
//! middleware. rmcp has neither a shared registry nor a middleware seam, and a generated Rust
//! handler is a self-contained [`rmcp::ServerHandler`], so the runtime wraps each hosted
//! handler instead: extras, the interceptor chain, cache hints, resource overrides and header
//! forwarding are applied around it (see [`Service`]), and a combined endpoint routes to each
//! service by name rather than registering them all onto one server (see
//! [`CombinedServer`]).
//!
//! Go's `context.Context` carries a caller's headers as incoming gRPC metadata to the
//! interceptors a tool call dispatches through. Here a tool calls its gRPC service through
//! the hybrid server's in-process `Loopback`, which runs the server's own interceptor chain,
//! and the caller's headers ride along in a task-local [`crate::shared::CallContext`] the
//! loopback reads — the same effect, without a context parameter generated code would have
//! to thread through.

mod ask;
mod cache;
mod combine;
mod completion;
mod config;
mod elicitation;
mod error;
mod extras;
mod handler;
mod health;
mod hosted;
mod interceptor;
mod invoke;
mod metadata;
mod progress;
mod resources;
pub mod sdk;
mod serve;
mod server;
mod service;
mod sse;
mod stream;
mod tool;
mod transport;

/// The SDK itself, for anything [`sdk`] does not name.
pub use rmcp;

pub use crate::shared::{HeaderMapping, default_header_mappings, status_name};
pub use ask::{ASK_DOMAIN, ASK_REASON, ask, ask_id, ask_pending, asked, merge_answers};
pub use cache::{CacheHint, CacheHints, CacheScope};
pub use combine::{
    CombineError, CombinedHandler, CombinedServer, HandlerFactory, Inventory, inventory,
};
pub use completion::EnumCompletions;
pub use config::{Config, ResourceHandler, resource_handler_for};
pub use elicitation::{
    ELICIT_REQUEST_ID, ElicitField, ElicitHook, Elicitation, elicit, elicit_schema,
    merge_elicit_result, missing_elicit_fields, run_elicitation,
};
pub use error::{GrpcError, code_string, handle_any_error, handle_error};
pub use extras::{ExtraProperty, ExtraValues, extract_extras};
pub use handler::{BoxedHandler, DynHandler};
pub use health::{HealthCheck, HealthState, grpc_health_check, health_check_handler};
pub use interceptor::{Interceptors, ToolHandler, ToolInterceptor};
pub use invoke::{chain_unary_interceptors, invoke_unary};
pub use metadata::{GRPC_PROGRESS_TOKEN_KEY, with_incoming_progress_token, with_progress_token};
pub use progress::{
    ProgressMessage, ProgressSink, progress_params, send_done_progress, send_progress_from_proto,
};
pub use resources::{
    app_resource, app_resource_uri, default_app_html, default_app_resource_result,
    default_prompt_result, default_resource_result,
};
pub use serve::new_http_handler;
pub use server::{
    DEFAULT_ADDR, DEFAULT_HEALTH_CHECK_PATH, Endpoint, ServerConfig, parse_transports,
    prefer_generated_base_path, resolve_base_path, server_endpoint, server_endpoint_with,
    start_server,
};
pub use service::{Service, ServiceGroup};
pub use stream::InProcessServerStream;
pub use tool::{
    create_tool, error_result, parse_schema, prepare_tool_with_extras, set_tool_app_meta,
    structured_result, text_result, with_output_schema,
};
pub use transport::{Transport, UnsupportedTransport};

/// Where a tool server mounts when nothing overrides it, and where a combined server mounts:
/// the one path an agent connects to for every service.
pub const DEFAULT_BASE_PATH: &str = "/mcp";
