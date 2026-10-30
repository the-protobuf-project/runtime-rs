//! Agent2Agent on a [`HybridServer`]: Go's `WithA2AServices` and `WithA2AAgent`, and the
//! types they take.
//!
//! The services are built at start rather than when registered, as Go builds them in
//! `startAgents`: the address, transports and base path come from `opts.a2a`, which is only
//! final once environment overrides and port defaults have been applied. The HTTP transports
//! share one listener on the A2A port; the gRPC transport registers on the server's own gRPC
//! routes beside every other service.

use std::sync::Arc;

use agents::a2a::{self, Transport};

use crate::hybrid::HybridServer;

/// A blocking function that serves an Agent2Agent service against a config the server builds
/// from its A2A options — transports, listen address, base path, and the gRPC routes the
/// binding registers on.
///
/// It mirrors the MCP registration so a generated `serve_foo_a2a` can be registered the same
/// way. [`HybridServer::with_a2a_agent`] wraps a hand-written agent into one.
pub type A2aServiceFunc = a2a::ServiceFunc;

/// The config an [`A2aServiceFunc`] is handed, re-exported so a caller need not import the
/// agents crate.
pub type A2aServerConfig = a2a::ServerConfig;

/// What an agent implements: a request in, a stream of events out.
pub type A2aAgent = Arc<dyn a2a::Executor>;

/// One distinct capability an agent advertises on its card.
pub type A2aSkill = a2a::Skill;

impl HybridServer {
    /// Registers one or more Agent2Agent service functions.
    ///
    /// Each is started with a config the server builds: the HTTP transports share one
    /// listener on `opts.a2a.port`, and the gRPC transport registers on the server's own gRPC
    /// port alongside every other service. Nothing is served unless `opts.enable_a2a` is set.
    ///
    /// This is the registration a generated `serve_foo_a2a` uses. For a hand-written agent,
    /// [`HybridServer::with_a2a_agent`] is the shorter form.
    #[must_use]
    pub fn with_a2a_services(mut self, services: impl IntoIterator<Item = A2aServiceFunc>) -> Self {
        let before = self.a2a_service_funcs.len();
        self.a2a_service_funcs.extend(services);
        tracing::debug!(
            count = self.a2a_service_funcs.len() - before,
            "with_a2a_services: appending A2A service func(s)"
        );
        self
    }

    /// Serves `agent` over Agent2Agent, advertising `skills` on its card.
    ///
    /// The agent's identity — name, description, version — comes from the server's own
    /// options rather than being restated here, so the card and the service cannot disagree
    /// about what this process is.
    #[must_use]
    pub fn with_a2a_agent(self, agent: impl a2a::Executor, skills: Vec<A2aSkill>) -> Self {
        let agent: A2aAgent = Arc::new(agent);
        self.with_a2a_services([a2a::service_fn(move |cancel, mut cfg: A2aServerConfig| {
            cfg.skills = skills.clone();
            a2a::start_server(cancel, cfg, Arc::clone(&agent))
        })])
    }

    /// The registered A2A functions as runtime services, built from the resolved options.
    ///
    /// Empty unless A2A is enabled, so registering an agent on a server that has A2A turned
    /// off costs a log line rather than a port.
    pub(crate) fn a2a_services(&self) -> Vec<Arc<dyn agents::Service>> {
        if self.a2a_service_funcs.is_empty() {
            return Vec::new();
        }
        if !self.opts.enable_a2a {
            tracing::debug!(
                count = self.a2a_service_funcs.len(),
                "agents: A2A disabled; registered A2A services are not served"
            );
            return Vec::new();
        }

        let addr = format!("{}:{}", self.opts.a2a.host, self.opts.a2a.port);
        let transports = self.a2a_transports();
        tracing::debug!(
            count = self.a2a_service_funcs.len(),
            %addr,
            "agents: registering A2A service(s)"
        );
        self.a2a_service_funcs
            .iter()
            .map(|serve| {
                Arc::new(
                    a2a::Service::from_fn(Arc::clone(serve))
                        .addr(addr.clone())
                        .transports(transports.clone())
                        .base_path(self.opts.a2a.base_path.clone()),
                ) as Arc<dyn agents::Service>
            })
            .collect()
    }

    /// The server's configured transports in the runtime's own terms, defaulting to JSON-RPC
    /// when nothing is named.
    fn a2a_transports(&self) -> Vec<Transport> {
        if self.opts.a2a.transports.is_empty() {
            tracing::debug!("agents: no A2A transport specified, defaulting to jsonrpc");
            return vec![Transport::JsonRpc];
        }
        self.opts
            .a2a
            .transports
            .iter()
            .filter_map(|transport| transport.as_str().parse().ok())
            .collect()
    }
}
