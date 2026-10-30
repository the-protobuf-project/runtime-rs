//! Starting, stopping and restarting a [`HybridServer`].

use axum::Router;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::banner::{self, Summary};
use crate::error::{Error, Result};
use crate::http::{Surface, health_router};
use crate::hybrid::HybridServer;
use crate::rest::RestSurface;
use crate::types::GatewayTarget;

impl HybridServer {
    /// Starts every configured component and returns once they are accepting.
    ///
    /// The order is Go's: options are validated and the certificate loaded, then the gRPC
    /// server comes up (registering the agent protocols on the way, since A2A's gRPC binding
    /// must be registered before tonic takes the routes), then HTTP/3, then the HTTP/1.1
    /// gateway, and the banner last — so nothing it lists is still coming up.
    ///
    /// Unlike Go's `Start`, this does not wait for a signal; [`HybridServer::serve`] does.
    ///
    /// # Errors
    ///
    /// Invalid options, an unreadable certificate, an address already in use, or an agent
    /// protocol that refused to start.
    pub async fn start(&mut self) -> Result<()> {
        self.load_certificate()?;
        crate::validate::validate_options(&mut self.opts, self.cert.is_some())?;
        self.validate_cors()?;
        tracing::debug!(
            grpc = %self.opts.grpc.addr(),
            http = self.opts.enable_http,
            mcp = self.opts.enable_mcp,
            a2a = self.opts.enable_a2a,
            "start: options valid"
        );

        if let Err(err) = self.start_components().await {
            // Whatever came up before the failure is stopped, so its ports are free and a
            // later start can retry. The original error is the one worth reporting.
            if let Err(stop) = self.stop().await {
                tracing::warn!(error = %stop, "start: cleanup after a failed start did not finish");
            }
            return Err(err);
        }

        banner::print(&self.opts, &self.summary());
        Ok(())
    }

    /// The components [`HybridServer::start`] brings up, in order.
    async fn start_components(&mut self) -> Result<()> {
        self.start_grpc_server().await?;

        let router = self.apply_cors(self.http_router()?)?;
        if self.opts.experimental_http3 {
            self.start_http3(router.clone()).await?;
        }
        if self.opts.enable_http {
            self.start_http(router).await?;
        } else {
            tracing::debug!("start: HTTP/1.1 gateway disabled");
        }
        Ok(())
    }

    /// Starts, waits for `SIGINT` or `SIGTERM`, then stops. Go's `Start`, for a process that
    /// does nothing else.
    ///
    /// # Errors
    ///
    /// Whatever [`HybridServer::start`] or [`HybridServer::stop`] reports.
    pub async fn serve(mut self) -> Result<()> {
        self.start().await?;
        shutdown_signal().await;
        self.stop().await
    }

    /// Stops every component, bounded by the shutdown timeout.
    ///
    /// Three phases, as in Go. The gRPC listener stops accepting first, so no new RPC
    /// arrives over TCP. HTTP/1.1, HTTP/3 and the agent protocols then drain in parallel —
    /// and their in-flight calls still reach the services, because the REST gateways call
    /// through the loopback rather than the listener. The gRPC server's in-flight calls
    /// drain last. Anything still running when the timeout expires is cut off.
    ///
    /// Safe to call on a server that never started, and safe to call twice.
    ///
    /// # Errors
    ///
    /// The timeout expiring, or an agent protocol failing to drain.
    pub async fn stop(&self) -> Result<()> {
        tracing::info!("Shutting down servers...");
        let (grpc, grpc_task, http, http3, tasks) = {
            let mut state = self.state.lock().expect("grpc: server state poisoned");
            state.loopback = None;
            (
                state.grpc.take(),
                state.grpc_task.take(),
                state.http.take(),
                state.http3.take(),
                std::mem::take(&mut state.tasks),
            )
        };

        // Kept apart from the join handles the drain consumes, so a drain that times out
        // can still stop the tasks rather than leave them running detached.
        let aborts: Vec<_> = tasks
            .iter()
            .chain(grpc_task.iter())
            .map(JoinHandle::abort_handle)
            .collect();

        let drain = async {
            if let Some(grpc) = &grpc {
                grpc.cancel();
            }
            for token in [&http, &http3].into_iter().flatten() {
                token.cancel();
            }
            let (_, agents) = tokio::join!(join_all(tasks), self.stop_agents());
            if let Some(task) = grpc_task {
                let _ = task.await;
            }
            agents
        };

        match tokio::time::timeout(self.shutdown_timeout, drain).await {
            Ok(result) => result,
            Err(_) => {
                for task in aborts {
                    task.abort();
                }
                Err(Error::ShutdownTimeout(self.shutdown_timeout))
            }
        }
    }

    /// Stops the server. Go's `Close`, which also flushes telemetry; the telemetry here is
    /// `tracing`, whose subscriber the application owns, so this is [`HybridServer::stop`].
    ///
    /// # Errors
    ///
    /// Whatever [`HybridServer::stop`] reports.
    pub async fn close(&self) -> Result<()> {
        self.stop().await
    }

    /// Stops every component and starts them again, replaying every registration onto
    /// fresh listeners and a fresh route registry.
    ///
    /// # Errors
    ///
    /// Whatever [`HybridServer::stop`] or [`HybridServer::start`] reports.
    pub async fn restart(&mut self) -> Result<()> {
        tracing::info!("Restarting servers...");
        self.stop().await?;
        self.start().await
    }

    /// The router HTTP/1.1 and HTTP/3 both serve.
    ///
    /// `/health` always; then either the caller's own handler, or every REST gateway over
    /// the loopback plus every dialling gateway registration.
    fn http_router(&self) -> Result<Router> {
        if let Some(handler) = &self.http_handler {
            return Ok(Surface::from_router(handler.clone())
                .mount(health_router())
                .into_router());
        }

        let mut router = health_router();
        let target = GatewayTarget {
            endpoint: self.opts.grpc.addr(),
            tls: self.cert.is_some(),
            rest_snake_case: self.rest_snake_case,
        };
        for register in &self.http_gateways {
            router = register(router, &target).map_err(Error::Gateway)?;
        }

        if !self.gateways.is_empty() {
            let loopback = self
                .state
                .lock()
                .expect("grpc: server state poisoned")
                .loopback
                .clone()
                .ok_or_else(|| Error::Gateway("the gRPC server is not running".into()))?;
            let gateways = self
                .gateways
                .iter()
                .map(|build| build(loopback.clone()))
                .collect();
            router = router.fallback_service(RestSurface::new(gateways));
        }
        Ok(router)
    }

    /// Starts the HTTP/1.1 gateway.
    async fn start_http(&self, mut router: Router) -> Result<()> {
        if self.opts.experimental_http3 {
            // Advertised on every HTTP/1.1 response so a client that speaks HTTP/3 upgrades.
            let alt_svc = format!("h3=\":{}\"; ma=86400", self.http3_port()?);
            if let Ok(value) = http::HeaderValue::from_str(&alt_svc) {
                router = router.layer(tower_http::set_header::SetResponseHeaderLayer::overriding(
                    http::header::ALT_SVC,
                    value,
                ));
            }
        }

        let token = CancellationToken::new();
        let handle = crate::http::h1::serve(
            router,
            &self.opts.http.addr(),
            self.cert.as_ref(),
            token.clone(),
        )
        .await?;
        let mut state = self.state.lock().expect("grpc: server state poisoned");
        state.http = Some(token);
        state.tasks.push(handle);
        Ok(())
    }

    /// Starts HTTP/3 one port past the gateway, on every interface — Go's `":port+1"`.
    #[cfg(feature = "http3")]
    async fn start_http3(&self, router: Router) -> Result<()> {
        let cert = self.cert.as_ref().ok_or(Error::Http3NeedsTls)?;
        let addr = format!("0.0.0.0:{}", self.http3_port()?);
        let token = CancellationToken::new();
        let handle = crate::http::h3::serve(router, &addr, cert, token.clone()).await?;
        let mut state = self.state.lock().expect("grpc: server state poisoned");
        state.http3 = Some(token);
        state.tasks.push(handle);
        Ok(())
    }

    /// Without the `http3` feature there is nothing to start; saying so beats ignoring the
    /// option.
    #[cfg(not(feature = "http3"))]
    async fn start_http3(&self, _router: Router) -> Result<()> {
        tracing::warn!("experimental_http3 is set but this build has no `http3` feature");
        Ok(())
    }

    fn http3_port(&self) -> Result<u16> {
        self.opts
            .http
            .port
            .checked_add(1)
            .ok_or(Error::InvalidPort { component: "http3" })
    }

    /// What the banner reports, from what actually came up.
    fn summary(&self) -> Summary {
        Summary {
            services: self.grpc_service_names(),
            tls: self.cert.is_some(),
            health: self.opts.enable_health,
            reflection: self.opts.environment.exposes_reflection()
                && !self.descriptor_sets.is_empty(),
            otlp_endpoint: String::new(),
            rest_routes: self.gateways.len() + self.http_gateways.len(),
            agents: self.agent_endpoints(),
        }
    }
}

/// Awaits every task, ignoring how each ended: a task that panicked has already logged, and
/// there is nothing left to stop.
async fn join_all(tasks: Vec<JoinHandle<()>>) {
    for task in tasks {
        let _ = task.await;
    }
}

/// Resolves on `SIGINT`, or `SIGTERM` where there is one.
async fn shutdown_signal() {
    let interrupt = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = interrupt => {}
        () = terminate => {}
    }
}
