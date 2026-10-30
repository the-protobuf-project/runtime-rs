//! One server speaking gRPC, a JSON/REST surface, optional HTTP/3, and the agent protocols
//! on shared TLS.

mod agent_runtime;
pub mod banner;
mod builder;
pub mod codes;
pub mod environment;
pub mod error;
pub mod gateway_errors;
pub mod http;
mod hybrid;
pub mod intercept;
mod lifecycle;
pub mod loopback;
pub mod options;
pub mod rest;
mod server;
pub mod status;
pub mod tls;
mod types;
mod validate;
pub mod validation;

pub use codes::{StatusCode, StatusCodeType};
pub use error::{Error, Result};
pub use hybrid::{DEFAULT_SHUTDOWN_TIMEOUT, HybridServer};
pub use intercept::{Interceptor, Next, UnaryCall, UnaryReply};
pub use loopback::Loopback;
pub use rest::{GatewayFn, RestGateway};
pub use status::Status;
pub use types::{GatewayTarget, GrpcServiceFn, HttpGatewayFn, UnaryInterceptorFn};
pub use validation::Validator;
