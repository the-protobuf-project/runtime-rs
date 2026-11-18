//! One server speaking gRPC, a JSON/REST surface, optional HTTP/3, and the agent protocols
//! on shared TLS.

#[cfg(feature = "server")]
mod agent_runtime;
#[cfg(feature = "server")]
mod annotations;
#[cfg(feature = "server")]
pub mod banner;
#[cfg(feature = "server")]
mod builder;
#[cfg(feature = "server")]
pub mod codes;
#[cfg(feature = "server")]
pub mod cors;
#[cfg(feature = "server")]
pub mod environment;
#[cfg(feature = "server")]
pub mod error;
#[cfg(feature = "server")]
pub mod gateway_errors;
#[cfg(feature = "server")]
pub mod hints;
#[cfg(feature = "server")]
pub mod http;
#[cfg(feature = "server")]
mod hybrid;
#[cfg(feature = "server")]
pub mod idempotency;
#[cfg(feature = "server")]
pub mod intercept;
#[cfg(feature = "server")]
mod layer;
#[cfg(feature = "server")]
mod lifecycle;
#[cfg(feature = "server")]
mod listener;
#[cfg(feature = "server")]
pub mod loopback;
#[cfg(feature = "server")]
mod mcp_options;
#[cfg(feature = "server")]
pub mod observe;
#[cfg(feature = "server")]
pub mod options;
#[cfg(feature = "server")]
mod otel;
#[cfg(feature = "server")]
pub mod problem;
#[cfg(feature = "server")]
pub mod rest;
#[cfg(feature = "server")]
mod server;
#[cfg(feature = "server")]
pub mod status;
#[cfg(feature = "server")]
pub mod stream_intercept;
// Without the `tls` feature the same module path holds an uninhabited `Certificate`, so the
// transports keep one signature and the compiler proves no TLS path is reachable.
#[cfg(feature = "server")]
#[cfg_attr(not(feature = "tls"), path = "tls_off.rs")]
pub mod tls;
#[cfg(feature = "server")]
mod types;
#[cfg(feature = "server")]
mod validate;
#[cfg(feature = "server")]
pub mod validation;
#[cfg(feature = "server")]
pub mod validation_hints;

#[cfg(feature = "server")]
pub use banner::BannerOutput;
#[cfg(feature = "server")]
pub use codes::{StatusCode, StatusCodeType};
#[cfg(feature = "server")]
pub use cors::{CorsLayer, CorsPolicy, cors};
#[cfg(feature = "server")]
pub use error::{Error, Result};
#[cfg(feature = "server")]
pub use hybrid::{DEFAULT_SHUTDOWN_TIMEOUT, HybridServer};
#[cfg(feature = "server")]
pub use intercept::{Interceptor, Next, UnaryCall, UnaryReply};
#[cfg(feature = "server")]
pub use layer::GrpcService;
#[cfg(feature = "server")]
pub use listener::ListenAddr;
#[cfg(feature = "server")]
pub use loopback::Loopback;
#[cfg(feature = "server")]
pub use observe::{Observer, is_server_error};
#[cfg(feature = "server")]
pub use rest::{GatewayFn, RestGateway};
#[cfg(feature = "server")]
pub use status::Status;
#[cfg(feature = "server")]
pub use stream_intercept::{StreamCall, StreamInterceptor, StreamNext};
/// The signals [`HybridServer::with_telemetry`] and [`Observer::with_telemetry`] report
/// through, so a caller configures them without naming the telemetry crate.
#[cfg(feature = "server")]
pub use telemetry::Telemetry;
#[cfg(feature = "server")]
pub use types::{GatewayTarget, GrpcServiceFn, HttpGatewayFn, UnaryInterceptorFn};
#[cfg(feature = "server")]
pub use validation::Validator;
#[cfg(feature = "server")]
pub use validation::{ProtoValidator, ValidationInterceptor};

#[cfg(feature = "a2a")]
#[cfg(feature = "server")]
mod a2a;
#[cfg(feature = "a2a")]
#[cfg(feature = "server")]
pub use a2a::{A2aAgent, A2aServerConfig, A2aServiceFunc, A2aSkill};

/// The shared runtime of the services protoc-gen-service generates for Rust.
pub mod servicex;
