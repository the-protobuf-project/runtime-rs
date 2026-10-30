//! The shapes a caller registers with a [`crate::HybridServer`].
//!
//! Go's registration callbacks take `*grpc.Server` and `*runtime.ServeMux` — objects that
//! are mutated in place. The tonic and axum equivalents are consumed by their own
//! combinators, so the callbacks below take and return, which is the same contract stated
//! the way Rust states it.

use axum::Router;
use tonic::service::RoutesBuilder;

/// Registers one or more gRPC services on the server's route registry.
///
/// It is an `Fn` rather than a `FnOnce` because [`crate::HybridServer::restart`] replays
/// every registration onto a fresh registry — a one-shot closure would make a server
/// startable exactly once.
///
/// ```
/// use grpc::GrpcServiceFn;
/// use std::sync::Arc;
/// # use tonic::service::RoutesBuilder;
///
/// let register: GrpcServiceFn = Arc::new(|routes: &mut RoutesBuilder| {
///     // routes.add_service(pb::greeter_server::GreeterServer::new(MyGreeter::default()));
///     let _ = routes;
/// });
/// # let _ = register;
/// ```
pub type GrpcServiceFn = std::sync::Arc<dyn Fn(&mut RoutesBuilder) + Send + Sync>;

/// What a REST gateway handler needs in order to reach the gRPC server behind it.
///
/// It replaces the `(endpoint string, []grpc.DialOption)` pair Go passes: the dial options
/// there carry only the transport credentials, which is one bool once the choice is between
/// "the server's own TLS" and "plaintext".
#[derive(Debug, Clone)]
pub struct GatewayTarget {
    /// The gRPC server's `host:port`, for a generated client to dial.
    pub endpoint: String,
    /// Whether that endpoint speaks TLS.
    pub tls: bool,
}

impl GatewayTarget {
    /// The `http://` or `https://` origin form a tonic channel wants.
    pub fn uri(&self) -> String {
        let scheme = if self.tls { "https" } else { "http" };
        format!("{scheme}://{}", self.endpoint)
    }
}

/// An error a gateway registration reports, kept untyped so generated code can return
/// whatever it builds its channel with.
pub type GatewayRegistrationError = Box<dyn std::error::Error + Send + Sync>;

/// Registers REST gateway routes that proxy JSON to the gRPC services behind them.
///
/// It is handed the router built so far and returns it with its own routes added, which is
/// how several gateways compose onto one mux.
pub type HttpGatewayFn = std::sync::Arc<
    dyn Fn(Router, &GatewayTarget) -> Result<Router, GatewayRegistrationError> + Send + Sync,
>;

/// A unary interceptor: cross-cutting middleware every RPC passes through.
///
/// Go chains `grpc.UnaryServerInterceptor` values. tonic has no single equivalent — its
/// interceptors act on request metadata, and anything richer is a [`tower`] layer — so this
/// is the metadata-level seam, which is what validation and auth actually need.
pub type UnaryInterceptorFn = std::sync::Arc<
    dyn Fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> + Send + Sync,
>;
