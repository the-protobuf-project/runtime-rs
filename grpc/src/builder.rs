//! The registrations a caller chains onto a [`HybridServer`] before starting it.
//!
//! Go states these as variadic functional options. Chained methods are the same idea in the
//! form Rust states it, and they are here rather than beside the struct so that what the
//! server *is* stays readable apart from what a caller may put on it.

use std::sync::Arc;

use crate::hybrid::HybridServer;
use crate::loopback::Loopback;
use crate::rest::RestGateway;
use crate::types::{GrpcServiceFn, HttpGatewayFn, UnaryInterceptorFn};
use crate::validation::Validator;

impl HybridServer {
    /// Registers a generated tonic service, taking its name from
    /// [`tonic::server::NamedService`] so the startup summary can list it.
    ///
    /// This is the ergonomic form; [`HybridServer::with_grpc_services`] is the escape hatch
    /// for anything it cannot express.
    #[must_use]
    pub fn with_service<S>(mut self, service: S) -> Self
    where
        S: tower::Service<
                http::Request<tonic::body::Body>,
                Response = http::Response<tonic::body::Body>,
                Error = std::convert::Infallible,
            > + tonic::server::NamedService
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        let name = <S as tonic::server::NamedService>::NAME.to_string();
        tracing::debug!(%name, "with_service: registering gRPC service");
        self.grpc_services.push((
            Some(name),
            std::sync::Arc::new(move |routes: &mut tonic::service::RoutesBuilder| {
                // Cloned per registration so a restart can replay this onto a new registry.
                routes.add_service(service.clone());
            }),
        ));
        self
    }

    /// Registers gRPC services through a closure over the route registry, for anything
    /// [`HybridServer::with_service`] cannot express — several services at once, or a
    /// generated registration helper.
    #[must_use]
    pub fn with_grpc_services(mut self, register: GrpcServiceFn) -> Self {
        self.grpc_services.push((None, register));
        self
    }

    /// Registers an HTTP gateway handler, proxying RESTful JSON to the gRPC services.
    ///
    /// The handler is given the router built so far and the gRPC endpoint to dial, and
    /// returns the router with its own routes added.
    #[must_use]
    pub fn with_http_gateways(mut self, register: HttpGatewayFn) -> Self {
        tracing::debug!("with_http_gateways: appending gateway registration");
        self.http_gateways.push(register);
        self
    }

    /// Mounts a REST gateway that calls the gRPC services in-process.
    ///
    /// This is the counterpart of Go's `WithHTTPGateways` with grpc-gateway replaced by
    /// [`transcode`]: `build` is handed the server's [`Loopback`] and returns the gateway,
    /// whose dispatcher calls the services through it. A REST call therefore runs the same
    /// interceptor chain as a gRPC one, which is what Go gets by dialling its own server.
    ///
    /// ```ignore
    /// server.with_gateway(|loopback| pb::http::gateway(BookServiceClient::new(loopback)))
    /// ```
    ///
    /// Several gateways may be mounted; a request goes to the first whose routes claim it.
    #[must_use]
    pub fn with_gateway<G, F>(mut self, build: F) -> Self
    where
        G: RestGateway,
        F: Fn(Loopback) -> G + Send + Sync + 'static,
    {
        self.gateways.push(Arc::new(move |loopback| {
            Arc::new(build(loopback)) as Arc<dyn RestGateway>
        }));
        self
    }

    /// Serves `router` in place of the gateway routes, with the health endpoint beside it.
    ///
    /// Go's `WithHTTPHandler`: the seam for a caller composing its own HTTP surface. The
    /// gateways registered with [`HybridServer::with_gateway`] and
    /// [`HybridServer::with_http_gateways`] are not mounted when it is set.
    #[must_use]
    pub fn with_http_handler(mut self, router: axum::Router) -> Self {
        self.http_handler = Some(router);
        self
    }

    /// Bounds how long [`HybridServer::stop`] may take before cutting off what is still
    /// running. Go's `WithShutdownTimeout`; the default is 20 seconds.
    #[must_use]
    pub fn with_shutdown_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Installs a unary interceptor, applied in the order the calls are chained.
    ///
    /// Use it for cross-cutting request middleware such as authentication. Anything that
    /// needs the request body rather than its metadata is a [`tower`] layer instead.
    #[must_use]
    pub fn with_unary_interceptor(mut self, interceptor: UnaryInterceptorFn) -> Self {
        self.interceptors.push(interceptor);
        self
    }

    /// Installs a message-level interceptor, applied in the order the calls are chained and
    /// after every [`HybridServer::with_unary_interceptor`].
    ///
    /// This is Go's `grpc.UnaryServerInterceptor`: it sees the request and the reply, not
    /// just the metadata. It runs for gRPC callers and for REST callers alike, because the
    /// gateways call through the same chain. Decoding the message needs the method's
    /// descriptor set, registered with [`HybridServer::with_reflection`].
    #[must_use]
    pub fn with_interceptor(mut self, interceptor: impl crate::intercept::Interceptor) -> Self {
        self.message_interceptors.push(Arc::new(interceptor));
        self
    }

    /// Makes the REST gateway emit and accept `snake_case` (proto) field names instead of
    /// the default `camelCase`. This matches the field naming the MCP layer uses.
    ///
    /// The flag reaches each [`HybridServer::with_http_gateways`] registration as
    /// [`GatewayTarget::rest_snake_case`](crate::GatewayTarget::rest_snake_case), because
    /// those handlers own their codecs and this crate does not marshal protobuf itself.
    ///
    /// It does not reach a [`HybridServer::with_gateway`] gateway: a `transcode` gateway's
    /// field names are fixed by the serde impls of the generated message types, and
    /// `transcode` has no option to rename them at runtime.
    #[must_use]
    pub fn with_rest_snake_case(mut self) -> Self {
        tracing::debug!("with_rest_snake_case: enabling proto field names");
        self.rest_snake_case = true;
        self
    }

    /// Whether the REST gateway was asked for `snake_case` field names.
    pub fn rest_snake_case(&self) -> bool {
        self.rest_snake_case
    }

    /// Registers an agent-facing protocol — MCP or A2A — on the runtime this server drives.
    ///
    /// Go names the two separately, as `WithMCPServices` and `WithA2AServices`, because each
    /// takes that protocol's own generated function. Both arrive here as an
    /// [`agents::Service`], which is what they are once their crate has built one.
    #[must_use]
    pub fn with_agent_service<S: agents::Service>(mut self, service: S) -> Self {
        self.agent_services.push(Arc::new(service));
        self
    }

    /// [`HybridServer::with_agent_service`] for a service that is already shared.
    #[must_use]
    pub fn with_agent_service_dyn(mut self, service: Arc<dyn agents::Service>) -> Self {
        self.agent_services.push(service);
        self
    }

    /// Loads a TLS certificate and key, enabling TLS for the gRPC server, the REST gateway,
    /// and HTTP/3 alike.
    ///
    /// The files are read at [`HybridServer::start`], so a bad path is a start error rather
    /// than a panic here.
    #[must_use]
    pub fn with_certificates(
        mut self,
        cert_file: impl Into<String>,
        key_file: impl Into<String>,
    ) -> Self {
        let paths = (cert_file.into(), key_file.into());
        tracing::debug!(cert = %paths.0, key = %paths.1, "with_certificates: deferred until start");
        self.cert_paths = Some(paths);
        self
    }

    /// Registers an encoded protobuf file descriptor set for server reflection.
    ///
    /// Go registers reflection with no input, because a Go server can enumerate its own
    /// descriptors from the global registry. Rust has no such registry, so reflection needs
    /// the descriptor set `tonic-build` emits — without one there is nothing to reflect,
    /// which is why this takes an argument where Go's takes none.
    #[must_use]
    pub fn with_reflection(mut self, descriptor_set: &'static [u8]) -> Self {
        self.descriptor_sets.push(descriptor_set);
        self
    }

    /// Enables protovalidate request validation for every unary RPC the server dispatches.
    ///
    /// The interceptor is built during [`HybridServer::start`] and runs before the
    /// caller-supplied chain, unless [`HybridServer::with_validation_last`] moved it.
    #[must_use]
    pub fn with_validation(mut self) -> Self {
        self.enable_validation = true;
        self
    }

    /// Supplies the rules engine [`HybridServer::with_validation`] runs, and enables it.
    ///
    /// Go's validation is protovalidate, found in the global registry; Rust has no such
    /// engine yet, so the caller names one. See [`crate::validation`].
    #[must_use]
    pub fn with_validator(mut self, validator: impl Validator) -> Self {
        self.validator = Some(Arc::new(validator));
        self.enable_validation = true;
        self
    }

    /// Enables validation, as [`HybridServer::with_validation`] does, but runs it after the
    /// interceptors given to [`HybridServer::with_unary_interceptor`] rather than before.
    ///
    /// Go's `WithValidationLast`. Use it when one of those interceptors decides whether the
    /// caller may ask at all: a request without credentials is then answered
    /// `UNAUTHENTICATED`, and only an admitted one learns what is wrong with its fields.
    /// Given both options, validation runs last.
    #[must_use]
    pub fn with_validation_last(mut self) -> Self {
        tracing::debug!("with_validation_last: validation moves to the end of the chain");
        self.enable_validation = true;
        self.validation_last = true;
        self
    }
}
