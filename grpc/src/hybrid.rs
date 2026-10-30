//! [`HybridServer`] — the object every other module in this crate is a method on.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::Result;
use crate::loopback::Loopback;
use crate::options::Options;
use crate::rest::GatewayFn;
use crate::tls::Certificate;
use crate::types::{GrpcServiceFn, HttpGatewayFn, UnaryInterceptorFn};
use crate::validation::Validator;

/// How long a stop may take before what is still running is cut off. Go's default.
pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(20);

/// What a running server is holding, and must give back when it stops.
#[derive(Default)]
pub(crate) struct Running {
    /// Stops the gRPC listener.
    pub(crate) grpc: Option<CancellationToken>,
    /// The gRPC serving task, awaited last so the loopback outlives the HTTP drain.
    pub(crate) grpc_task: Option<JoinHandle<()>>,
    /// The in-process path to the services, for the REST gateways.
    pub(crate) loopback: Option<Loopback>,
    /// Stops the HTTP/1.1 gateway listener.
    pub(crate) http: Option<CancellationToken>,
    /// Stops the HTTP/3 listener.
    pub(crate) http3: Option<CancellationToken>,
    /// Every HTTP serving task, awaited on the way out so a stop is a drain rather than a
    /// drop.
    pub(crate) tasks: Vec<JoinHandle<()>>,
    /// The one runtime serving MCP and A2A, absent when neither is on.
    pub(crate) agent_runtime: Option<Arc<agents::Runtime>>,
    /// Stops the agent protocols.
    pub(crate) agent_cancel: Option<CancellationToken>,
    /// Where the agent protocols came up, for the banner.
    pub(crate) endpoints: Vec<agents::Endpoint>,
}

/// A gRPC server and its optional HTTP/1.1 REST gateway, experimental HTTP/3 gateway, and
/// agent-facing protocols — all sharing one configuration and one TLS identity.
///
/// Build it with [`HybridServer::new`], chain the registrations, then
/// [`HybridServer::start`].
///
/// Go configures this with variadic functional options. The chained methods below are the
/// same idea in the form Rust states it, with one behavioural difference worth naming: Go's
/// `WithCertificates` loads the files immediately and ends the process if they are bad,
/// whereas here loading is deferred to [`HybridServer::start`] so it surfaces as an ordinary
/// error alongside every other configuration problem.
pub struct HybridServer {
    pub(crate) opts: Options,
    pub(crate) cert: Option<Certificate>,
    pub(crate) cert_paths: Option<(String, String)>,

    pub(crate) grpc_services: Vec<(Option<String>, GrpcServiceFn)>,
    pub(crate) http_gateways: Vec<HttpGatewayFn>,
    pub(crate) gateways: Vec<GatewayFn>,
    /// Replaces the gateway routes entirely, which is Go's `WithHTTPHandler`.
    pub(crate) http_handler: Option<axum::Router>,
    pub(crate) agent_services: Vec<Arc<dyn agents::Service>>,
    pub(crate) interceptors: Vec<UnaryInterceptorFn>,
    /// Message-level interceptors, run after the metadata-level ones.
    pub(crate) message_interceptors: Vec<Arc<dyn crate::intercept::Interceptor>>,

    /// Registers the protovalidate interceptor at start. Resolved once, so a restart does
    /// not prepend it twice.
    pub(crate) enable_validation: bool,
    /// The rules engine validation runs, when one was supplied.
    pub(crate) validator: Option<Arc<dyn Validator>>,
    /// How long a stop may take.
    pub(crate) shutdown_timeout: Duration,
    /// Encoded file descriptor sets for server reflection.
    pub(crate) descriptor_sets: Vec<&'static [u8]>,
    /// Whether the REST gateway should emit proto (snake_case) field names.
    pub(crate) rest_snake_case: bool,

    /// The cross-origin policy for the HTTP listeners, which is Go's `WithCORS`.
    pub(crate) cors: Option<crate::cors::CorsPolicy>,
    /// Interceptors for streaming calls, which is Go's `WithStreamInterceptors`.
    pub(crate) stream_interceptors: Vec<Arc<dyn crate::stream_intercept::StreamInterceptor>>,
    /// Runs validation after the caller's unary interceptors rather than before them.
    pub(crate) validation_last: bool,

    pub(crate) state: Mutex<Running>,
}

impl HybridServer {
    /// Builds a server from `opts`, applying environment-variable overrides first.
    ///
    /// Nothing is bound until [`HybridServer::start`].
    pub fn new(mut opts: Options) -> Self {
        tracing::debug!(
            service = %opts.service_name,
            version = %opts.version,
            environment = %opts.environment,
            "NewHybridServer"
        );
        crate::environment::apply_env_overrides(&mut opts);

        Self {
            opts,
            cert: None,
            cert_paths: None,
            grpc_services: Vec::new(),
            http_gateways: Vec::new(),
            gateways: Vec::new(),
            http_handler: None,
            agent_services: Vec::new(),
            interceptors: Vec::new(),
            message_interceptors: Vec::new(),
            enable_validation: false,
            validator: None,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
            descriptor_sets: Vec::new(),
            rest_snake_case: false,
            cors: None,
            stream_interceptors: Vec::new(),
            validation_last: false,
            state: Mutex::new(Running::default()),
        }
    }

    /// The options this server was built with. After [`HybridServer::start`] they carry the
    /// resolved defaults, so this is what the banner and the agent cards agree on.
    pub fn options(&self) -> &Options {
        &self.opts
    }

    /// The TLS certificate in use, once [`HybridServer::start`] has loaded it.
    pub fn certificate(&self) -> Option<&Certificate> {
        self.cert.as_ref()
    }

    /// Where every agent protocol answers. Empty until [`HybridServer::start`] returns.
    pub fn agent_endpoints(&self) -> Vec<agents::Endpoint> {
        self.state
            .lock()
            .expect("grpc: server state poisoned")
            .endpoints
            .clone()
    }

    /// Loads the deferred certificate, if one was named.
    pub(crate) fn load_certificate(&mut self) -> Result<()> {
        if self.cert.is_some() {
            return Ok(());
        }
        if let Some((cert_file, key_file)) = self.cert_paths.clone() {
            self.cert = Some(Certificate::from_pem_files(cert_file, key_file)?);
        }
        Ok(())
    }
}
