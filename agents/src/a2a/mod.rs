//! The Agent2Agent runtime: an agent, the skills it advertises, and the transports it
//! answers on, as one [`crate::Service`].
//!
//! It wraps the official [`a2a-rs`](https://github.com/a2aproject/a2a-rs) SDK the same way
//! the Go package wraps `a2a-go` — the protocol is the SDK's, and what this module adds is
//! placement: an agent registered here gets its identity from the runtime and its address
//! from wherever the runtime decided to put it.
//!
//! ```no_run
//! # use agents::{Config, Identity, Runtime};
//! # use agents::a2a;
//! # use a2a_server::{AgentExecutor, ExecutorContext};
//! # use futures::stream::BoxStream;
//! # struct Echo;
//! # #[async_trait::async_trait]
//! # impl AgentExecutor for Echo {
//! #     fn execute(&self, _: ExecutorContext) -> BoxStream<'static, Result<::a2a::StreamResponse, ::a2a::A2AError>> { Box::pin(futures::stream::empty()) }
//! #     fn cancel(&self, _: ExecutorContext) -> BoxStream<'static, Result<::a2a::StreamResponse, ::a2a::A2AError>> { Box::pin(futures::stream::empty()) }
//! # }
//! # async fn example() -> agents::Result<()> {
//! let runtime = Runtime::new(Config {
//!     identity: Identity { name: "my-agent".into(), version: "1.0.0".into(), ..Default::default() },
//!     port: 9000,
//!     ..Default::default()
//! })
//! .register(a2a::Service::new(Echo, vec![a2a::skill("echo", "Echo", "Repeats the input")]));
//!
//! runtime.serve(tokio_util::sync::CancellationToken::new()).await
//! # }
//! ```

//!
//! # Serving without a runtime
//!
//! [`start_server`] serves one agent from a [`ServerConfig`] — on a listener of its own, or
//! onto a host's [`crate::Mux`] and [`crate::GrpcRoutes`]. A [`Service`] registered on a
//! runtime goes through the same function; the runtime only decides where.
//!
//! # Differences from the Go original
//!
//! - Go's `ServiceFrom(fn, opts...)` and `ServiceOption`s are [`Service::from_fn`] and chained
//!   setters; `context.Context` is a [`tokio_util::sync::CancellationToken`].
//! - The gRPC binding is the SDK's own (`a2a-grpc`, over the `lf.a2a.v1` service `a2a-pb`
//!   generates from the protocol's `a2a.proto` at the pinned revision), registered on
//!   [`crate::GrpcRoutes`] where Go registers on a `*grpc.Server`.
//! - `HandlerOptions` and `TransportOptions` are structs naming what the Rust SDK can vary,
//!   not variadic option lists; see [`HandlerOptions`] and [`TransportOptions`].
//! - Go's `HandleError(ctx, err)` takes any `error`; here the error is a [`tonic::Status`],
//!   and `None` stands for Go's `nil`.

mod card;
mod config;
mod endpoint;
mod error;
mod executor;
mod grpc;
mod handler;
mod serve;
mod service;
mod transport;

pub use card::build_card;
pub use config::{HandlerLayer, HandlerOptions, OnReady, ServerConfig, TransportOptions};
pub use endpoint::{
    Endpoint, host_port_or_default, normalize_base_path, resolve_base_path, server_endpoint,
    server_endpoints,
};
pub use error::{Error, code_name, error_text, handle_error, state_for_code};
pub use executor::{
    Events, ExecutorFunc, STATE_AUTH_REQUIRED, STATE_CANCELED, STATE_COMPLETED, STATE_FAILED,
    STATE_INPUT_REQUIRED, STATE_REJECTED, STATE_SUBMITTED, STATE_WORKING, TextAgent, agent_message,
    artifact_event, data_part, once, request_text, status_event, text_part,
};
pub use grpc::{GRPC_SERVICE_NAME, pb, register_grpc};
pub use handler::{Handler, new_request_handler};
pub use serve::{SHUTDOWN_TIMEOUT, agent_card_handler, start_server};
pub use service::{Service, ServiceFunc, service_fn};
pub use transport::{Transport, parse_transports};

use a2a_server::WELL_KNOWN_AGENT_CARD_PATH;

/// The wire vocabulary, re-exported so a program describing or implementing an agent imports
/// this module and nothing else.
///
/// These are aliases rather than wrappers: the card and the events are formats the protocol
/// defines, and a copy of them here would be one more thing to keep in step for no gain.
pub use a2a::{
    AgentCapabilities as Capabilities, AgentCard as Card, AgentInterface as Interface,
    AgentProvider as Provider, AgentSkill as Skill, Artifact, Message, Part, PartContent, Role,
    StreamResponse as Event, Task, TaskState, TaskStatus,
};
pub use a2a_server::{AgentExecutor as Executor, ExecutorContext};

/// The A2A protocol version this runtime implements.
pub const PROTOCOL_VERSION: &str = a2a::VERSION;

/// Where a client fetches an agent's card. Singular per host.
pub const AGENT_CARD_PATH: &str = WELL_KNOWN_AGENT_CARD_PATH;

/// Where JSON-RPC mounts, and the prefix REST is served under.
pub const DEFAULT_BASE_PATH: &str = "/a2a";

/// The host advertised on a card when nothing better is known.
///
/// It is loopback on purpose: an agent that has not been told its public URL is being
/// developed, not deployed, and advertising a wildcard bind would send clients nowhere.
pub const DEFAULT_HOST: &str = "127.0.0.1";

/// The port the HTTP transports listen on when the runtime names none.
pub const DEFAULT_PORT: u16 = 9000;

/// The content type a card declares when the caller names none.
pub const DEFAULT_MODE: &str = "text/plain";

/// A skill with just the three fields a card cannot do without.
///
/// The SDK's [`AgentSkill`](Skill) has eight more, all optional; this is the shape most callers
/// actually write.
pub fn skill(
    id: impl Into<String>,
    name: impl Into<String>,
    description: impl Into<String>,
) -> Skill {
    Skill {
        id: id.into(),
        name: name.into(),
        description: description.into(),
        tags: Vec::new(),
        examples: None,
        input_modes: None,
        output_modes: None,
        security_requirements: None,
    }
}
