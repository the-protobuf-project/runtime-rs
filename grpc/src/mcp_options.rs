//! The MCP registrations a [`HybridServer`] takes: Go's `WithMCPServices`, `WithMCPEndpoint`,
//! `WithoutMCPServicePaths`, `WithStatelessMCP`, `WithMCPHeaderMappings` and
//! `WithElicitHook`, and the `mcpGroups` that turns them into agent services.
//!
//! # The interceptor push-down
//!
//! Go hands the server's unary interceptor chain to MCP tool dispatch
//! (`ServiceInterceptors(s.unaryChain()...)`), so a tool call runs the same validation and
//! auth as the wire RPC. Here an MCP service is built from the server's [`Loopback`] — the
//! in-process routes behind that same chain — and calls its gRPC service through it. The
//! chain then applies by construction, exactly as it does for the REST gateway, and the
//! caller's mapped headers reach it as incoming metadata through
//! [`agents::shared::CallContext`].

use std::sync::Arc;

use agents::mcp::{self, BoxedHandler, ElicitHook, HeaderMapping, Transport};

use crate::hybrid::HybridServer;
use crate::loopback::{Loopback, LoopbackSlot};
use crate::options::McpTransport;

/// Builds one MCP service from the server's loopback.
pub(crate) type McpServiceFn = Arc<dyn Fn(Loopback) -> mcp::Service<BoxedHandler> + Send + Sync>;

/// What the MCP options recorded.
#[derive(Clone, Default)]
pub(crate) struct McpSettings {
    services: Vec<McpServiceFn>,
    endpoint: Option<String>,
    no_service_paths: bool,
    stateless: bool,
    headers: Option<Vec<HeaderMapping>>,
    elicit_hook: Option<ElicitHook>,
    pub(crate) loopback: LoopbackSlot,
}

impl HybridServer {
    /// Registers an MCP service, built from the server's [`Loopback`] so its tools call the
    /// gRPC services through the server's own interceptor chain — Go's `WithMCPServices`.
    ///
    /// Services share the MCP port, each at its own base path;
    /// [`HybridServer::with_mcp_endpoint`] also serves them as one.
    ///
    /// ```ignore
    /// server.with_mcp_service(|loopback| {
    ///     mcp::Service::from_handler(FooServiceMcpHandler::new(Foo::new(FooClient::new(loopback))))
    ///         .base_path(FOO_SERVICE_MCP_DEFAULT_BASE_PATH)
    /// })
    /// ```
    #[must_use]
    pub fn with_mcp_service<H, F>(mut self, build: F) -> Self
    where
        H: agents::mcp::rmcp::ServerHandler,
        F: Fn(Loopback) -> mcp::Service<H> + Send + Sync + 'static,
    {
        tracing::debug!("with_mcp_service: appending an MCP service");
        self.mcp
            .services
            .push(Arc::new(move |loopback| build(loopback).boxed()));
        self
    }

    /// Serves every MCP service together as ONE server at `path` (`/mcp` when empty) on the
    /// MCP port — Go's `WithMCPEndpoint`. The server reports the options' service name and
    /// version, and their description as its instructions.
    ///
    /// Two services declaring the same tool, prompt, resource or resource template make
    /// start fail with an error naming each clash. Each service keeps answering at its own
    /// path too, until [`HybridServer::without_mcp_service_paths`].
    #[must_use]
    pub fn with_mcp_endpoint(mut self, path: impl Into<String>) -> Self {
        let mut path = path.into();
        if path.is_empty() {
            path = mcp::DEFAULT_BASE_PATH.to_string();
        }
        tracing::debug!(%path, "with_mcp_endpoint: serving every MCP service as one");
        self.mcp.endpoint = Some(path);
        self
    }

    /// Stops mounting each MCP service at its own path. With
    /// [`HybridServer::with_mcp_endpoint`] the combined endpoint is then the only MCP
    /// surface; without it, no MCP is served.
    #[must_use]
    pub fn without_mcp_service_paths(mut self) -> Self {
        tracing::debug!("without_mcp_service_paths: per-service MCP paths disabled");
        self.mcp.no_service_paths = true;
        self
    }

    /// Serves MCP streamable HTTP without sessions, at every MCP path, so any replica behind
    /// a load balancer answers any request. It gives up server-to-client requests, the
    /// standalone `GET` stream and `DELETE`. Stdio and SSE are unaffected.
    #[must_use]
    pub fn with_stateless_mcp(mut self) -> Self {
        tracing::debug!("with_stateless_mcp: MCP streamable HTTP serves without sessions");
        self.mcp.stateless = true;
        self
    }

    /// The HTTP headers handed to MCP tool calls as gRPC metadata. Without it the defaults
    /// — `authorization`, `x-request-id`, `x-trace-id` — are forwarded; an empty list
    /// forwards none.
    #[must_use]
    pub fn with_mcp_header_mappings(
        mut self,
        mappings: impl IntoIterator<Item = HeaderMapping>,
    ) -> Self {
        let mappings: Vec<HeaderMapping> = mappings.into_iter().collect();
        tracing::debug!(count = mappings.len(), "with_mcp_header_mappings");
        self.mcp.headers = Some(mappings);
        self
    }

    /// Puts `hook` on every MCP tool call, to change elicitation fields at runtime.
    #[must_use]
    pub fn with_elicit_hook(mut self, hook: ElicitHook) -> Self {
        self.mcp.elicit_hook = Some(hook);
        self
    }

    /// The mapped headers: what was set, or the defaults.
    fn mcp_header_mappings(&self) -> Vec<HeaderMapping> {
        self.mcp
            .headers
            .clone()
            .unwrap_or_else(agents::shared::default_header_mappings)
    }

    /// The configured transport, defaulting to streamable HTTP.
    fn mcp_transport(&self) -> Transport {
        match self.opts.mcp.transport {
            None => {
                tracing::debug!(
                    "agents: no MCP transport specified, defaulting to streamable-http"
                );
                Transport::StreamableHttp
            }
            Some(McpTransport::StreamableHttp) => Transport::StreamableHttp,
            Some(McpTransport::Sse) => Transport::Sse,
            Some(McpTransport::Stdio) => Transport::Stdio,
        }
    }

    /// The MCP the server registers — Go's `mcpGroups`: each service at its own path, every
    /// service together at the endpoint path, or both while clients move over. All share the
    /// MCP port.
    pub(crate) fn mcp_groups(&self) -> Vec<Arc<dyn agents::Service>> {
        if !self.opts.enable_mcp || self.mcp.services.is_empty() {
            return Vec::new();
        }
        let transport = self.mcp_transport();
        let host = if self.opts.mcp.host.is_empty() {
            agents::DEFAULT_HOST
        } else {
            &self.opts.mcp.host
        };
        let addr = format!("{host}:{}", self.opts.mcp.port);
        let loopback = Loopback::deferred(self.mcp.loopback.clone());
        let hook = self.mcp.elicit_hook.clone();
        let build = || {
            let group = self.mcp.services.iter().fold(
                mcp::ServiceGroup::new()
                    .addr(addr.clone())
                    .transports([transport])
                    .headers(self.mcp_header_mappings()),
                |group, build| {
                    let mut service = build(loopback.clone());
                    if let Some(hook) = &hook {
                        service = service.elicit_hook(Arc::clone(hook));
                    }
                    group.with_boxed(service)
                },
            );
            if self.mcp.stateless {
                group.stateless()
            } else {
                group
            }
        };

        let mut groups: Vec<Arc<dyn agents::Service>> = Vec::new();
        // Stdio carries one server, so with a combined endpoint it is the combined one's.
        let service_paths = !self.mcp.no_service_paths
            && (self.mcp.endpoint.is_none() || transport != Transport::Stdio);
        if service_paths {
            tracing::debug!(
                count = self.mcp.services.len(),
                %addr,
                transport = transport.as_str(),
                "agents: registering MCP services"
            );
            groups.push(Arc::new(build()));
        }
        if let Some(endpoint) = &self.mcp.endpoint {
            tracing::debug!(
                count = self.mcp.services.len(),
                %addr,
                %endpoint,
                "agents: serving MCP services as one"
            );
            groups.push(Arc::new(build().combined().base_path(endpoint.clone())));
        }
        if groups.is_empty() {
            tracing::warn!(
                "agents: without_mcp_service_paths without with_mcp_endpoint leaves no MCP \
                 endpoint; none is served"
            );
        }
        groups
    }
}
