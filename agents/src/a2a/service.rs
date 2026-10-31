//! The agent as a registrable service: what it is, and what a caller may adjust.
//!
//! Go's `Service`, `ServiceWith` and `ServiceFrom` with their `ServiceOption`s become one type
//! with two constructors — [`Service::new`] over an agent, [`Service::from_fn`] over a
//! [`ServiceFunc`] — and chained setters in place of the options.

use std::future::Future;
use std::sync::Arc;

use a2a::AgentCard;
use a2a_server::{AgentExecutor, TaskStore};
use futures::future::BoxFuture;
use tokio_util::sync::CancellationToken;

use crate::agents::Placement;
use crate::error::ServiceError;
use crate::shared::HeaderMapping;

use super::config::{HandlerOptions, ServerConfig};
use super::{Capabilities, Provider, Skill, Transport};

/// A blocking function that serves one agent against a config the runtime assembled — the
/// shape a generated `serve_foo_a2a` takes, and the seam a host uses to adjust that config
/// before serving.
///
/// It is handed a config with [`ServerConfig::on_ready`] already set and must leave it in
/// place (passing the config on to [`super::start_server`] does), or the runtime placing this
/// service never learns it mounted.
pub type ServiceFunc = Arc<
    dyn Fn(CancellationToken, ServerConfig) -> BoxFuture<'static, Result<(), ServiceError>>
        + Send
        + Sync,
>;

/// Wraps an async closure as a [`ServiceFunc`], so a caller writes `async move { .. }` rather
/// than boxing the future by hand.
pub fn service_fn<F, Fut>(f: F) -> ServiceFunc
where
    F: Fn(CancellationToken, ServerConfig) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), ServiceError>> + Send + 'static,
{
    Arc::new(move |cancel, cfg| Box::pin(f(cancel, cfg)))
}

/// What a [`Service`] serves: an agent this crate serves itself, or a function that does.
pub(super) enum Inner {
    Agent(Arc<dyn AgentExecutor>),
    Func(ServiceFunc),
}

/// An agent registered on a [`crate::Runtime`].
///
/// Its identity comes from the runtime rather than being restated here, so its card and the
/// process's other protocols cannot disagree about what this service is. What the card
/// cannot get from the runtime is what the agent can actually do, which is what skills are.
pub struct Service {
    pub(super) inner: Inner,
    pub(super) skills: Vec<Skill>,
    pub(super) transports: Vec<Transport>,
    pub(super) addr: Option<String>,
    pub(super) base_path: String,
    pub(super) card: Option<AgentCard>,
    pub(super) capabilities: Capabilities,
    pub(super) headers: Vec<HeaderMapping>,
    pub(super) serve_agent_card: bool,
    pub(super) provider: Option<Provider>,
    pub(super) documentation_url: Option<String>,
    pub(super) icon_url: Option<String>,
    pub(super) default_input_modes: Vec<String>,
    pub(super) default_output_modes: Vec<String>,
    pub(super) handler_options: HandlerOptions,
}

impl Service {
    /// An agent and the skills it advertises, backed by an in-memory task store.
    pub fn new(executor: impl AgentExecutor, skills: Vec<Skill>) -> Self {
        Self::with_inner(Inner::Agent(Arc::new(executor)), skills)
    }

    /// [`Service::new`] for an agent that is already shared.
    pub fn from_arc(executor: Arc<dyn AgentExecutor>, skills: Vec<Skill>) -> Self {
        Self::with_inner(Inner::Agent(executor), skills)
    }

    /// [`Service::new`] with a task store of the caller's choosing, for an agent whose tasks
    /// must outlive the process.
    pub fn with_task_store(
        executor: impl AgentExecutor,
        task_store: impl TaskStore,
        skills: Vec<Skill>,
    ) -> Self {
        let mut service = Self::new(executor, skills);
        service.handler_options.task_store = Some(Arc::new(task_store));
        service
    }

    /// Registers a [`ServiceFunc`] rather than an agent — Go's `ServiceFrom` — for a
    /// generated entrypoint, or a host that wants the assembled config in hand before serving.
    ///
    /// Skills, card and handler settings are the function's to choose on the config it is
    /// handed; the setters here still decide placement: transports, address, base path.
    pub fn from_fn(serve: ServiceFunc) -> Self {
        Self::with_inner(Inner::Func(serve), Vec::new())
    }

    fn with_inner(inner: Inner, skills: Vec<Skill>) -> Self {
        Self {
            inner,
            skills,
            transports: vec![Transport::JsonRpc],
            addr: None,
            base_path: String::new(),
            card: None,
            capabilities: Capabilities::default(),
            headers: Vec::new(),
            serve_agent_card: true,
            provider: None,
            documentation_url: None,
            icon_url: None,
            default_input_modes: Vec::new(),
            default_output_modes: Vec::new(),
            handler_options: HandlerOptions::default(),
        }
    }

    /// The config this service serves with at `placement`, before [`super::start_server`]
    /// resolves its defaults.
    pub(super) fn config_for(&self, placement: &Placement) -> ServerConfig {
        ServerConfig {
            name: placement.identity.name.clone(),
            description: placement.identity.description.clone(),
            version: placement.identity.version.clone(),
            transports: self.transports.clone(),
            addr: placement.addr.clone(),
            base_path: self.base_path.clone(),
            public_url: placement.public_url.clone(),
            skills: self.skills.clone(),
            capabilities: self.capabilities.clone(),
            default_input_modes: self.default_input_modes.clone(),
            default_output_modes: self.default_output_modes.clone(),
            provider: self.provider.clone(),
            documentation_url: self.documentation_url.clone(),
            icon_url: self.icon_url.clone(),
            card: self.card.clone(),
            header_mappings: self.headers.clone(),
            mux: Some(placement.mux.clone()),
            grpc_routes: placement.grpc_routes.clone(),
            handler_options: self.handler_options.clone(),
            serve_agent_card: self.serve_agent_card,
            ..ServerConfig::default()
        }
    }

    /// Chooses the bindings the agent answers on. Without it the agent speaks
    /// [`Transport::JsonRpc`], which every A2A client is required to support.
    #[must_use]
    pub fn transports(mut self, transports: impl IntoIterator<Item = Transport>) -> Self {
        self.transports = transports.into_iter().collect();
        self
    }

    /// Gives the agent a listen address of its own rather than the runtime's shared one.
    #[must_use]
    pub fn addr(mut self, addr: impl Into<String>) -> Self {
        self.addr = Some(addr.into());
        self
    }

    /// Overrides where JSON-RPC mounts and REST is served beneath. Defaults to
    /// [`super::DEFAULT_BASE_PATH`].
    #[must_use]
    pub fn base_path(mut self, path: impl Into<String>) -> Self {
        self.base_path = path.into();
        self
    }

    /// Declares the optional protocol features the agent supports.
    #[must_use]
    pub fn capabilities(mut self, capabilities: Capabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    /// Serves a card the caller built instead of one assembled from the runtime's identity
    /// and the registered skills.
    ///
    /// For a card that is signed or came from a registry this is the only correct behaviour —
    /// rebuilding it would invalidate the signature.
    #[must_use]
    pub fn card(mut self, card: AgentCard) -> Self {
        self.card = Some(card);
        self
    }

    /// Forwards HTTP headers into gRPC metadata for the executor.
    #[must_use]
    pub fn headers(mut self, mappings: impl IntoIterator<Item = HeaderMapping>) -> Self {
        self.headers = mappings.into_iter().collect();
        self
    }

    /// Chooses whether this agent asks for the public card path.
    ///
    /// Asking is not the same as getting: the well-known path admits one card per host, so
    /// on a shared listener the first agent to mount takes it. Turn it off for an agent that
    /// should never claim it, whatever the mounting order turns out to be.
    #[must_use]
    pub fn serve_agent_card(mut self, serve: bool) -> Self {
        self.serve_agent_card = serve;
        self
    }

    /// Names the organisation behind the agent on its card.
    #[must_use]
    pub fn provider(mut self, provider: Provider) -> Self {
        self.provider = Some(provider);
        self
    }

    /// Points the card at the agent's documentation.
    #[must_use]
    pub fn documentation_url(mut self, url: impl Into<String>) -> Self {
        self.documentation_url = Some(url.into());
        self
    }

    /// Gives the card an icon.
    #[must_use]
    pub fn icon_url(mut self, url: impl Into<String>) -> Self {
        self.icon_url = Some(url.into());
        self
    }

    /// The content types the agent's skills accept unless a skill says otherwise. Without it
    /// the card declares [`super::DEFAULT_MODE`].
    #[must_use]
    pub fn default_input_modes(mut self, modes: impl IntoIterator<Item = String>) -> Self {
        self.default_input_modes = modes.into_iter().collect();
        self
    }

    /// The content types the agent's skills produce unless a skill says otherwise. Without
    /// it the card declares [`super::DEFAULT_MODE`].
    #[must_use]
    pub fn default_output_modes(mut self, modes: impl IntoIterator<Item = String>) -> Self {
        self.default_output_modes = modes.into_iter().collect();
        self
    }

    /// Passes settings through to the SDK's request handler: task store, push notifications,
    /// layers.
    #[must_use]
    pub fn handler_options(mut self, options: HandlerOptions) -> Self {
        self.handler_options = options;
        self
    }
}
