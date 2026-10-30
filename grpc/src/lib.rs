//! One server speaking gRPC, a JSON/REST surface, optional HTTP/3, and the agent protocols
//! on shared TLS.

mod agent_runtime;
mod annotations;
pub mod banner;
mod builder;
pub mod codes;
pub mod cors;
pub mod environment;
pub mod error;
pub mod gateway_errors;
pub mod hints;
pub mod http;
mod hybrid;
pub mod idempotency;
pub mod intercept;
mod lifecycle;
pub mod loopback;
mod mcp_options;
pub mod observe;
pub mod options;
mod otel;
pub mod problem;
pub mod rest;
mod server;
pub mod status;
pub mod stream_intercept;
pub mod tls;
mod types;
mod validate;
pub mod validation;
pub mod validation_hints;

pub use codes::{StatusCode, StatusCodeType};
pub use cors::{CorsLayer, CorsPolicy, cors};
pub use error::{Error, Result};
pub use hybrid::{DEFAULT_SHUTDOWN_TIMEOUT, HybridServer};
pub use intercept::{Interceptor, Next, UnaryCall, UnaryReply};
pub use loopback::Loopback;
pub use observe::{Observer, is_server_error};
pub use rest::{GatewayFn, RestGateway};
pub use status::Status;
pub use stream_intercept::{StreamCall, StreamInterceptor, StreamNext};
pub use types::{GatewayTarget, GrpcServiceFn, HttpGatewayFn, UnaryInterceptorFn};
pub use validation::Validator;
pub use validation::{ProtoValidator, ValidationInterceptor};

mod a2a;
pub use a2a::{A2aAgent, A2aServerConfig, A2aServiceFunc, A2aSkill};
