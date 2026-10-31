//! The gRPC leg: the listener, the built-in services, and the agent protocols that must
//! register before it serves.

use std::net::SocketAddr;

use agents::GrpcRoutes;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Server, ServerTlsConfig};

use crate::error::{Error, Result};
use crate::hybrid::HybridServer;
use crate::intercept::InterceptLayer;
use crate::loopback::{ChainLayer, Loopback};

impl HybridServer {
    /// Initialises and runs the gRPC server.
    ///
    /// The listener is bound here rather than inside tonic's own `serve`, so an address
    /// already in use is a [`HybridServer::start`] error the caller can act on — the same
    /// place Go's `net.Listen` reports it.
    pub(crate) async fn start_grpc_server(&mut self) -> Result<()> {
        let addr = self.opts.grpc.addr();
        tracing::debug!(%addr, "gRPC: binding TCP listener");

        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .map_err(|source| Error::Listen {
                addr: addr.clone(),
                source,
            })?;
        let bound: SocketAddr = listener.local_addr().map_err(|source| Error::Listen {
            addr: addr.clone(),
            source,
        })?;
        tracing::debug!(%bound, "gRPC: TCP listener ready");

        let routes = GrpcRoutes::new();
        self.register_health(&routes);
        self.register_reflection(&routes)?;
        self.register_grpc_services(&routes);

        // The agent protocols start here rather than from `start`, and the ordering is
        // load-bearing: A2A's gRPC binding is a service like any other, and the routes are
        // read out by value the moment the server below is built.
        self.start_agents(&routes).await?;

        let chain = self.interceptor_chain();
        let messages = InterceptLayer::new(
            self.message_chain(),
            crate::intercept::descriptor_pool(&self.descriptor_sets),
        );
        let streams = crate::stream_intercept::StreamInterceptLayer::new(
            self.stream_interceptors.clone(),
            crate::intercept::descriptor_pool(&self.descriptor_sets),
        );
        let telemetry = crate::otel::OtelLayer::new();
        let loopback = Loopback::new(
            routes.routes(),
            chain.clone(),
            messages.clone(),
            streams.clone(),
            telemetry.clone(),
        );
        // MCP services were handed a deferred loopback while the routes were still open.
        self.mcp.loopback.set(loopback.clone());

        let mut builder = Server::builder();
        if let Some(cert) = &self.cert {
            tracing::debug!("gRPC: TLS enabled — configuring credentials");
            builder = builder.tls_config(ServerTlsConfig::new().identity(cert.identity()))?;
        } else {
            tracing::debug!("gRPC: no certificate — running plaintext");
        }

        let router = builder
            .layer(telemetry)
            .layer(ChainLayer(chain))
            .layer(streams)
            .layer(messages)
            .add_routes(routes.routes());
        let token = CancellationToken::new();
        let shutdown = token.clone();
        let serving = addr.clone();

        tracing::debug!(%addr, "gRPC: starting server task");
        let handle = tokio::spawn(async move {
            let served = router
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    shutdown.cancelled().await;
                })
                .await;
            if let Err(err) = served {
                tracing::error!(addr = %serving, error = %err, "gRPC server stopped");
            }
        });

        let mut state = self.state.lock().expect("grpc: server state poisoned");
        state.grpc = Some(token);
        state.grpc_task = Some(handle);
        state.loopback = Some(loopback);
        Ok(())
    }

    /// The metadata-level unary interceptor chain: the caller's interceptors in the order
    /// they were chained. Validation is message-level; see [`HybridServer::message_chain`].
    fn interceptor_chain(&self) -> Option<crate::types::UnaryInterceptorFn> {
        let mut chain = Vec::new();
        chain.extend(self.interceptors.iter().cloned());
        match chain.len() {
            0 => None,
            1 => chain.pop(),
            _ => Some(crate::validation::chain(chain)),
        }
    }

    /// Registers the standard gRPC health checking service, when it is enabled.
    fn register_health(&self, routes: &GrpcRoutes) {
        if !self.opts.enable_health {
            tracing::debug!("gRPC: health check disabled");
            return;
        }
        tracing::debug!("gRPC: registering health check service (grpc.health.v1)");
        let (reporter, service) = tonic_health::server::health_reporter();
        // The empty service name is the whole-server status, which is what a load balancer
        // probing without a service name asks for.
        tokio::spawn(async move {
            reporter
                .set_service_status("", tonic_health::ServingStatus::Serving)
                .await;
        });
        routes.register(|builder| {
            builder.add_service(service);
        });
    }

    /// Registers server reflection, but only where exposing the service list is a
    /// convenience rather than a disclosure.
    fn register_reflection(&self, routes: &GrpcRoutes) -> Result<()> {
        if !self.opts.environment.exposes_reflection() {
            tracing::debug!(environment = %self.opts.environment, "gRPC: reflection disabled");
            return Ok(());
        }
        if self.descriptor_sets.is_empty() {
            // Go can enumerate its own descriptors from a global registry; Rust cannot, so
            // reflection without a descriptor set would register a service that answers
            // nothing. Saying so beats starting it empty.
            tracing::warn!(
                "gRPC: reflection is enabled for this environment but no descriptor set was \
                 registered — call HybridServer::with_reflection to serve it"
            );
            return Ok(());
        }

        tracing::debug!(
            environment = %self.opts.environment,
            sets = self.descriptor_sets.len(),
            "gRPC: registering reflection service"
        );
        let mut builder = tonic_reflection::server::Builder::configure();
        for set in &self.descriptor_sets {
            builder = builder.register_encoded_file_descriptor_set(set);
        }
        match builder.build_v1() {
            Ok(service) => routes.register(|routes| {
                routes.add_service(service);
            }),
            Err(err) => tracing::error!(error = %err, "gRPC: reflection failed to build"),
        }
        Ok(())
    }

    /// Applies every caller-supplied service registration.
    fn register_grpc_services(&self, routes: &GrpcRoutes) {
        tracing::debug!(
            count = self.grpc_services.len(),
            "gRPC: registering user services"
        );
        for (name, register) in &self.grpc_services {
            if let Some(name) = name {
                tracing::debug!(%name, "gRPC: applying service registration");
            }
            routes.register(|builder| register(builder));
        }
    }

    /// The names of the services registered through [`HybridServer::with_service`], sorted,
    /// for the startup summary.
    ///
    /// Go reads these back off the running server with `GetServiceInfo`; tonic keeps no such
    /// index, so they are recorded at registration instead. Services added through the
    /// closure escape hatch name themselves nowhere and so are absent here.
    pub(crate) fn grpc_service_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .grpc_services
            .iter()
            .filter_map(|(name, _)| name.clone())
            .filter(|name| {
                !matches!(
                    name.as_str(),
                    "grpc.health.v1.Health"
                        | "grpc.reflection.v1.ServerReflection"
                        | "grpc.reflection.v1alpha.ServerReflection"
                )
            })
            .collect();
        names.sort();
        names
    }
}
