//! Mounting tool servers into the placement the runtime handed them.
//!
//! Every HTTP transport is mounted per server at its base path: streamable HTTP at the path
//! itself and the legacy SSE transport at the path plus `/`, as Go mounts them. Both stop
//! through the runtime's cancellation token, which also ends long-lived `GET` streams — the
//! job Go's `releaseOnShutdown` does, since `http.Server.Shutdown` would otherwise wait them
//! out.

use std::io;
use std::sync::Arc;

use axum::Router;
use rmcp::ServerHandler;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use tokio_util::sync::CancellationToken;

use crate::agents::{Endpoint, Placement, Protocol, Ready, Requirements};
use crate::error::ServiceError;
use crate::shared::{HeaderMapping, with_header_forwarding};

use super::combine::{CombinedServer, HandlerFactory};
use super::handler::BoxedHandler;
use super::hosted::{Hosted, Hosting};
use super::service::{Service, ServiceGroup};
use super::{
    DEFAULT_BASE_PATH, HealthCheck, ServerConfig, Transport, UnsupportedTransport,
    health_check_handler, server_endpoint,
};

/// How one server is reached over HTTP.
pub(crate) struct Mount {
    pub(crate) base_path: String,
    pub(crate) transports: Vec<Transport>,
    pub(crate) headers: Vec<HeaderMapping>,
    pub(crate) stateless: bool,
    pub(crate) guard: Guard,
    pub(crate) health: Option<(String, HealthCheck)>,
}

/// Which `Host` and `Origin` a server's HTTP transports answer: `None` hosts leaves rmcp's
/// loopback-only default for Streamable HTTP and SSE unchecked; empty origins checks none.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Guard {
    pub(crate) hosts: Option<Vec<String>>,
    pub(crate) origins: Vec<String>,
}

/// The router serving one server's HTTP transports.
pub(crate) fn http_router<S: ServerHandler>(
    factory: Arc<dyn Fn() -> Result<S, io::Error> + Send + Sync>,
    mount: &Mount,
    cancel: CancellationToken,
) -> Router {
    let mut router = Router::new();
    let base = mount.base_path.trim_end_matches('/');
    let base = if base.is_empty() { "/" } else { base };
    for transport in &mount.transports {
        match transport {
            Transport::StreamableHttp => {
                // The config is #[non_exhaustive], so it is built from Default and adjusted.
                let mut config = StreamableHttpServerConfig::default();
                config.cancellation_token = cancel.clone();
                config.legacy_session_mode = !mount.stateless;
                if let Some(hosts) = &mount.guard.hosts {
                    config.allowed_hosts.clone_from(hosts);
                }
                config.allowed_origins.clone_from(&mount.guard.origins);
                let factory = Arc::clone(&factory);
                let service = StreamableHttpService::new(
                    move || factory(),
                    Arc::new(LocalSessionManager::default()),
                    config,
                );
                router = router.route_service(base, service);
            }
            Transport::Sse => {
                let path = format!("{}/", base.trim_end_matches('/'));
                let sse = super::sse::router(&path, Arc::clone(&factory), cancel.clone());
                router = router.merge(match &mount.guard.hosts {
                    Some(hosts) => super::guard::hosts(sse, hosts.clone()),
                    None => sse,
                });
            }
            Transport::Stdio => {}
        }
    }
    let mut router = with_header_forwarding(router, mount.headers.clone());
    if let Some((path, check)) = &mount.health {
        router = router.route(path, health_check_handler(Arc::clone(check)));
    }
    router
}

/// Serves `factory`'s handlers over `config`'s HTTP transports at its base path, with its
/// header mappings applied — Go's `NewHTTPHandler`, the router a process merges into one it
/// already owns. A stdio transport is ignored, and a config naming no HTTP transport is
/// served over streamable HTTP.
///
/// # Errors
///
/// None today; the result is kept so a transport this runtime cannot mount is reported
/// rather than silently skipped.
pub fn new_http_handler<H, F>(
    factory: F,
    config: &ServerConfig,
    cancel: CancellationToken,
) -> Result<Router, UnsupportedTransport>
where
    H: ServerHandler,
    F: Fn() -> H + Send + Sync + 'static,
{
    let mut transports: Vec<Transport> = config
        .resolved_transports()
        .into_iter()
        .filter(Transport::listens)
        .collect();
    if transports.is_empty() {
        transports.push(Transport::StreamableHttp);
    }
    let hosting = Arc::new(Hosting {
        interceptors: config.interceptors.clone(),
        headers: config.header_mappings.clone(),
        ..Hosting::default()
    });
    let factory = Arc::new(move || {
        Ok(Hosted {
            inner: factory(),
            hosting: Arc::clone(&hosting),
        })
    });
    let mount = Mount {
        base_path: config.resolved_base_path(),
        transports,
        headers: config.header_mappings.clone(),
        stateless: config.stateless,
        guard: Guard::default(),
        health: config.health_check.clone().map(|check| {
            let path = if config.health_check_path.is_empty() {
                super::server::DEFAULT_HEALTH_CHECK_PATH.to_string()
            } else {
                config.health_check_path.clone()
            };
            (path, check)
        }),
    };
    Ok(http_router(factory, &mount, cancel))
}

/// The endpoint one mount is reported as: the placement's public URL when it has one, and
/// otherwise Go's `ServerEndpoint` over the listen address.
fn endpoint(placement: &Placement, base_path: &str, transports: &[Transport]) -> Endpoint {
    let config = ServerConfig {
        transports: transports.to_vec(),
        addr: placement.addr.clone(),
        base_path: base_path.to_string(),
        ..Default::default()
    };
    let resolved = server_endpoint(&config);
    let url = if placement.public_url.is_empty() {
        resolved.url
    } else {
        format!(
            "{}{}",
            placement.public_url.trim_end_matches('/'),
            config.resolved_base_path()
        )
    };
    Endpoint {
        protocol: Protocol::Mcp,
        transport: resolved.transport,
        url,
        detail: config.resolved_base_path(),
    }
}

/// Serves one handler over stdio until `cancel` fires or the client hangs up.
async fn serve_stdio(
    handler: impl ServerHandler,
    cancel: CancellationToken,
) -> Result<(), ServiceError> {
    use rmcp::ServiceExt;
    let running = handler.serve(rmcp::transport::io::stdio()).await?;
    tokio::select! {
        _ = cancel.cancelled() => {}
        result = running.waiting() => { result?; }
    }
    Ok(())
}

/// The stdio endpoint, which Go reports with no URL and no detail.
fn stdio_endpoint() -> Endpoint {
    Endpoint {
        protocol: Protocol::Mcp,
        transport: Transport::Stdio.as_str().to_string(),
        url: String::new(),
        detail: String::new(),
    }
}

impl ServiceGroup {
    /// The factory for one member as the group serves it: the group's interceptors and
    /// headers in a layer outside the member's own.
    fn member_factory(&self, member: &Service<BoxedHandler>) -> HandlerFactory {
        let inner = Arc::clone(&member.factory);
        if self.interceptors.is_empty() && self.headers.is_none() {
            return inner;
        }
        let hosting = Arc::new(Hosting {
            interceptors: self.interceptors.clone(),
            headers: self.headers.clone().unwrap_or_default(),
            ..Hosting::default()
        });
        Arc::new(move || {
            Ok(BoxedHandler::new(Hosted {
                inner: inner()?,
                hosting: Arc::clone(&hosting),
            }))
        })
    }

    /// The mount for a server at `base_path`.
    fn mount(&self, base_path: String, member: Option<&Service<BoxedHandler>>) -> Mount {
        Mount {
            base_path,
            transports: self.resolved_transports(),
            headers: self
                .headers
                .clone()
                .or_else(|| member.map(|m| m.headers.clone()))
                .unwrap_or_default(),
            stateless: self.stateless || member.is_some_and(|m| m.stateless),
            guard: self
                .guard
                .clone()
                .or_else(|| member.map(|m| m.guard.clone()))
                .unwrap_or_default(),
            health: None,
        }
    }

    /// Builds the one server a combined group serves.
    async fn combined_server(&self, placement: &Placement) -> Result<CombinedServer, ServiceError> {
        let factories = self
            .members
            .iter()
            .map(|member| self.member_factory(member))
            .collect();
        Ok(CombinedServer::new(
            &placement.identity.name,
            &placement.identity.version,
            &placement.identity.description,
            factories,
        )
        .await?)
    }
}

#[async_trait::async_trait]
impl crate::Service for ServiceGroup {
    fn protocol(&self) -> Protocol {
        Protocol::Mcp
    }

    /// MCP never registers on a gRPC server — it dispatches to one in-process — so it asks
    /// only for a place to listen, and only when a transport it speaks actually listens.
    fn requires(&self) -> Requirements {
        Requirements {
            addr: self
                .addr
                .clone()
                .or_else(|| self.members.first().and_then(|m| m.addr.clone())),
            http: self.serves_http(),
            grpc: false,
        }
    }

    async fn serve(
        &self,
        cancel: CancellationToken,
        placement: Placement,
        ready: Arc<Ready>,
    ) -> Result<(), ServiceError> {
        if self.members.is_empty() {
            ready.ready(Vec::new());
            cancel.cancelled().await;
            return Ok(());
        }
        let transports = self.resolved_transports();
        let has_stdio = transports.contains(&Transport::Stdio);
        let mut endpoints = Vec::new();
        let mut routers = Vec::new();
        let combined = if self.combined {
            Some(self.combined_server(&placement).await?)
        } else {
            None
        };

        if self.serves_http() {
            match &combined {
                Some(server) => {
                    let base_path = self
                        .base_path
                        .clone()
                        .unwrap_or_else(|| DEFAULT_BASE_PATH.to_string());
                    let server = server.clone();
                    let factory = Arc::new(move || server.session());
                    let mount = self.mount(base_path.clone(), None);
                    routers.push(http_router(factory, &mount, cancel.clone()));
                    endpoints.push(endpoint(&placement, &base_path, &transports));
                }
                None => {
                    for member in &self.members {
                        let mount = self.mount(member.base_path.clone(), Some(member));
                        let factory = self.member_factory(member);
                        routers.push(http_router(factory, &mount, cancel.clone()));
                        endpoints.push(endpoint(&placement, &member.base_path, &transports));
                        if let Some((path, check)) = &member.health {
                            routers.push(
                                Router::new().route(path, health_check_handler(Arc::clone(check))),
                            );
                        }
                    }
                }
            }
            if let Some((path, check)) = &self.health {
                routers.push(Router::new().route(path, health_check_handler(Arc::clone(check))));
            }
            placement.mux.mount(move |mux| {
                routers
                    .into_iter()
                    .fold(mux, |mux, router| mux.merge(router))
            });
        }

        if has_stdio {
            endpoints.push(stdio_endpoint());
        }
        tracing::debug!(
            endpoints = endpoints.len(),
            combined = self.combined,
            "mcp: mounted"
        );
        ready.ready(endpoints);

        if has_stdio {
            // Stdio carries one server: the combined one, or the first member's.
            return match combined {
                Some(server) => serve_stdio(server.session()?, cancel).await,
                None => {
                    if self.members.len() > 1 {
                        tracing::warn!(
                            "mcp: stdio carries one server; serving only the first of {}",
                            self.members.len()
                        );
                    }
                    let factory = self.member_factory(&self.members[0]);
                    serve_stdio(factory()?, cancel).await
                }
            };
        }

        cancel.cancelled().await;
        Ok(())
    }
}

#[async_trait::async_trait]
impl<H: ServerHandler> crate::Service for Service<H> {
    fn protocol(&self) -> Protocol {
        Protocol::Mcp
    }

    /// See [`ServiceGroup`]'s: one service is a group of one.
    fn requires(&self) -> Requirements {
        Requirements {
            addr: self.addr.clone(),
            http: self.serves_http(),
            grpc: false,
        }
    }

    async fn serve(
        &self,
        cancel: CancellationToken,
        placement: Placement,
        ready: Arc<Ready>,
    ) -> Result<(), ServiceError> {
        let group = ServiceGroup::new().with_boxed(self.boxed());
        crate::Service::serve(&group, cancel, placement, ready).await
    }
}
