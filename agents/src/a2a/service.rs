//! The agent as a registrable service: what it is, and what a caller may adjust.

use std::sync::Arc;

use a2a::AgentCard;
use a2a_server::{AgentExecutor, DefaultRequestHandler, InMemoryTaskStore, TaskStore};

use crate::shared::HeaderMapping;

use super::{Capabilities, DEFAULT_BASE_PATH, Skill, Transport};

/// An agent registered on a [`crate::Runtime`].
///
/// Its identity comes from the runtime rather than being restated here, so its card and the
/// process's other protocols cannot disagree about what this service is. What the card
/// cannot get from the runtime is what the agent can actually do, which is what skills are.
pub struct Service {
    pub(super) handler: Arc<DefaultRequestHandler>,
    pub(super) skills: Vec<Skill>,
    pub(super) transports: Vec<Transport>,
    pub(super) addr: Option<String>,
    pub(super) base_path: String,
    pub(super) card: Option<AgentCard>,
    pub(super) capabilities: Capabilities,
    pub(super) headers: Vec<HeaderMapping>,
    pub(super) serve_agent_card: bool,
}

impl Service {
    /// An agent and the skills it advertises, backed by an in-memory task store.
    pub fn new(executor: impl AgentExecutor, skills: Vec<Skill>) -> Self {
        Self::with_task_store(executor, InMemoryTaskStore::new(), skills)
    }

    /// [`Service::new`] with a task store of the caller's choosing, for an agent whose tasks
    /// must outlive the process.
    pub fn with_task_store(
        executor: impl AgentExecutor,
        task_store: impl TaskStore,
        skills: Vec<Skill>,
    ) -> Self {
        Self {
            handler: Arc::new(DefaultRequestHandler::new(executor, task_store)),
            skills,
            transports: vec![Transport::JsonRpc],
            addr: None,
            base_path: DEFAULT_BASE_PATH.to_string(),
            card: None,
            capabilities: Capabilities::default(),
            headers: Vec::new(),
            serve_agent_card: true,
        }
    }

    /// Chooses the bindings the agent answers on. Without it the agent speaks
    /// [`Transport::JsonRpc`].
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

    /// Overrides where JSON-RPC mounts and REST is served beneath.
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
}
