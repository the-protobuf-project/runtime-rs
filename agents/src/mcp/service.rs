//! The tool server as a registrable service, alone or in a group.
//!
//! Go registers generated `ServeFooMCP` functions, each run against a config the runtime
//! builds. A generated Rust handler is a value rather than a function, so [`Service`] holds
//! the handler factory and the settings Go's config carries, and [`ServiceGroup`] is Go's
//! `ServiceGroup`: several services on one listener, each at its own path, or — with
//! [`ServiceGroup::combined`] — as one server at one path.

use std::collections::HashMap;
use std::sync::Arc;

use rmcp::ServerHandler;

use crate::shared::HeaderMapping;

use super::config::{Config, ResourceHandler};
use super::handler::BoxedHandler;
use super::hosted::{Hosted, Hosting};
use super::server::TryFactory;
use super::{
    CacheHints, DEFAULT_BASE_PATH, ElicitHook, ExtraProperty, HealthCheck, Interceptors, Transport,
};

/// A tool server registered on a [`crate::Runtime`].
///
/// Everything set on it is applied by the runtime around the handler — extras injected into
/// schemas and lifted out of arguments, the interceptor chain around every tool call, cache
/// hints on list and read results, resource overrides, and the caller's headers handed to
/// in-process gRPC — so a generated handler needs none of it compiled in.
pub struct Service<H: ServerHandler> {
    pub(super) factory: TryFactory<H>,
    pub(super) transports: Vec<Transport>,
    pub(super) addr: Option<String>,
    pub(super) base_path: String,
    pub(super) headers: Vec<HeaderMapping>,
    pub(super) extras: Vec<ExtraProperty>,
    pub(super) interceptors: Interceptors,
    pub(super) cache: CacheHints,
    pub(super) stateless: bool,
    pub(super) health: Option<(String, HealthCheck)>,
    pub(super) resource_handlers: HashMap<String, ResourceHandler>,
    pub(super) elicit_hook: Option<ElicitHook>,
    pub(super) methods: HashMap<String, String>,
}

impl<H: ServerHandler> Service<H> {
    /// A tool server built from `factory`, which is called once per session.
    pub fn new<F>(factory: F) -> Self
    where
        F: Fn() -> H + Send + Sync + 'static,
    {
        Self::try_new(move || Ok(factory()))
    }

    /// A tool server from one already-built handler, cloned per session.
    ///
    /// This is the shape protoc-gen-mcp emits: `FooServiceMcpHandler::new(svc)` takes the
    /// implementation by value, Arc-wraps it internally, and is [`Clone`] — so the handler is
    /// built once and every session gets a clone sharing that one implementation.
    ///
    /// ```no_run
    /// # use agents::mcp;
    /// # #[derive(Clone)] struct FooServiceMcpHandler;
    /// # impl rmcp::ServerHandler for FooServiceMcpHandler {}
    /// # let my_service = FooServiceMcpHandler;
    /// let handler = FooServiceMcpHandler::from(my_service);
    /// let service = mcp::Service::from_handler(handler);
    /// # let _ = service;
    /// ```
    pub fn from_handler(handler: H) -> Self
    where
        H: Clone,
    {
        Self::new(move || handler.clone())
    }

    /// [`Service::new`] for a factory that can fail — a handler opening a file or a
    /// connection as it is built.
    pub fn try_new<F>(factory: F) -> Self
    where
        F: Fn() -> Result<H, std::io::Error> + Send + Sync + 'static,
    {
        Self {
            factory: Arc::new(factory),
            transports: vec![Transport::StreamableHttp],
            addr: None,
            base_path: DEFAULT_BASE_PATH.to_string(),
            headers: Vec::new(),
            extras: Vec::new(),
            interceptors: Interceptors::new(),
            cache: CacheHints::default(),
            stateless: false,
            health: None,
            resource_handlers: HashMap::new(),
            elicit_hook: None,
            methods: HashMap::new(),
        }
    }

    /// Chooses the transports the server answers on. Without it it speaks
    /// [`Transport::StreamableHttp`], which is what a client that is not an IDE launching a
    /// subprocess expects.
    #[must_use]
    pub fn transports(mut self, transports: impl IntoIterator<Item = Transport>) -> Self {
        self.transports = transports.into_iter().collect();
        self
    }

    /// Gives the server a listen address of its own rather than the runtime's shared one.
    #[must_use]
    pub fn addr(mut self, addr: impl Into<String>) -> Self {
        self.addr = Some(addr.into());
        self
    }

    /// Overrides the HTTP path the server mounts under. A generated service carries its own
    /// proto-derived path and passes it here.
    #[must_use]
    pub fn base_path(mut self, path: impl Into<String>) -> Self {
        self.base_path = path.into();
        self
    }

    /// Forwards HTTP headers into gRPC metadata for tool calls, so an MCP call carries the
    /// same credentials and trace ids as the wire RPC behind it — both as forwarded headers
    /// and as the incoming metadata an interceptor reads (see [`crate::shared::CallContext`]).
    #[must_use]
    pub fn headers(mut self, mappings: impl IntoIterator<Item = HeaderMapping>) -> Self {
        self.headers = mappings.into_iter().collect();
        self
    }

    /// Declares extra properties, injected into every tool's schema and lifted back out of
    /// incoming arguments into [`super::ExtraValues`] before the handler sees them.
    #[must_use]
    pub fn extras(mut self, extras: impl IntoIterator<Item = ExtraProperty>) -> Self {
        self.extras = extras.into_iter().collect();
        self
    }

    /// The extra properties this server declares.
    pub fn declared_extras(&self) -> &[ExtraProperty] {
        &self.extras
    }

    /// Installs middleware around every tool call, so an MCP call runs the same validation,
    /// auth and tracing as the wire RPC behind it.
    #[must_use]
    pub fn interceptors(mut self, interceptors: Interceptors) -> Self {
        self.interceptors = interceptors;
        self
    }

    /// The interceptor chain.
    pub fn chain(&self) -> &Interceptors {
        &self.interceptors
    }

    /// Names the RPC behind a tool, so interceptors see `/pkg.Service/Method` as they would on
    /// the wire. A tool with no name given here is reported by its tool name.
    #[must_use]
    pub fn tool_method(mut self, tool: impl Into<String>, full_method: impl Into<String>) -> Self {
        self.methods.insert(tool.into(), full_method.into());
        self
    }

    /// Declares how long clients may treat this server's list and read results as fresh. The
    /// runtime stamps them on every result.
    #[must_use]
    pub fn cache_hints(mut self, hints: CacheHints) -> Self {
        self.cache = hints;
        self
    }

    /// The declared cache hints.
    pub fn declared_cache_hints(&self) -> &CacheHints {
        &self.cache
    }

    /// Serves streamable HTTP without sessions, so any replica answers any request. See
    /// [`super::ServerConfig::stateless`] for what that gives up.
    #[must_use]
    pub fn stateless(mut self) -> Self {
        self.stateless = true;
        self
    }

    /// Mounts a health probe at `path` running `check`; see [`super::health_check_handler`].
    #[must_use]
    pub fn health_check(mut self, path: impl Into<String>, check: HealthCheck) -> Self {
        let mut path = path.into();
        if !path.starts_with('/') {
            path.insert(0, '/');
        }
        self.health = Some((path, check));
        self
    }

    /// Serves a resource's content from `handler` instead of what the handler would answer.
    #[must_use]
    pub fn resource_handler(mut self, uri: impl Into<String>, handler: ResourceHandler) -> Self {
        self.resource_handlers.insert(uri.into(), handler);
        self
    }

    /// Puts `hook` on every tool call's `RequestContext::extensions`.
    #[must_use]
    pub fn elicit_hook(mut self, hook: ElicitHook) -> Self {
        self.elicit_hook = Some(hook);
        self
    }

    /// Applies a [`Config`]: its extras, headers, interceptors, hook and resource handlers,
    /// and its transport and address when set.
    #[must_use]
    pub fn config(mut self, config: Config) -> Self {
        self.extras.extend(config.extra_properties);
        self.headers.extend(config.header_mappings);
        self.interceptors = std::mem::take(&mut self.interceptors).extend(config.interceptors);
        self.resource_handlers.extend(config.resource_handlers);
        if let Some(hook) = config.elicit_hook {
            self.elicit_hook = Some(hook);
        }
        if let Some(transport) = config.transport {
            self.transports = vec![transport];
        }
        if let Some(addr) = config.addr {
            self.addr = Some(addr);
        }
        self
    }

    /// Whether any configured transport needs a listener.
    pub(super) fn serves_http(&self) -> bool {
        self.transports.iter().any(Transport::listens)
    }

    /// The settings the runtime wraps around each handler.
    fn hosting(&self) -> Hosting {
        Hosting {
            extras: self.extras.clone(),
            interceptors: self.interceptors.clone(),
            cache: self.cache.clone(),
            resource_handlers: self.resource_handlers.clone(),
            headers: self.headers.clone(),
            elicit_hook: self.elicit_hook.clone(),
            methods: self.methods.clone(),
        }
    }

    /// This service with its handler type erased, so it can sit in a [`ServiceGroup`].
    ///
    /// The settings move to the boxed service, where the runtime still applies them.
    pub fn boxed(&self) -> Service<BoxedHandler> {
        let hosting = Arc::new(self.hosting());
        let factory = Arc::clone(&self.factory);
        Service {
            factory: Arc::new(move || {
                Ok(BoxedHandler::new(Hosted {
                    inner: factory()?,
                    hosting: Arc::clone(&hosting),
                }))
            }),
            transports: self.transports.clone(),
            addr: self.addr.clone(),
            base_path: self.base_path.clone(),
            headers: self.headers.clone(),
            extras: Vec::new(),
            interceptors: Interceptors::new(),
            cache: CacheHints::default(),
            stateless: self.stateless,
            health: self.health.clone(),
            resource_handlers: HashMap::new(),
            elicit_hook: None,
            methods: HashMap::new(),
        }
    }
}

/// Several tool servers on one listener: Go's `ServiceGroup`.
///
/// Settings made on the group apply to every member, overriding what each member set:
/// transports, address, headers and statelessness are where the group is placed and how it
/// is reached, which members of one group cannot disagree about. Interceptors are the
/// exception — the group's chain runs outside each member's own.
pub struct ServiceGroup {
    pub(super) members: Vec<Service<BoxedHandler>>,
    pub(super) transports: Option<Vec<Transport>>,
    pub(super) addr: Option<String>,
    pub(super) headers: Option<Vec<HeaderMapping>>,
    pub(super) interceptors: Interceptors,
    pub(super) stateless: bool,
    pub(super) combined: bool,
    pub(super) base_path: Option<String>,
    pub(super) health: Option<(String, HealthCheck)>,
}

impl ServiceGroup {
    /// An empty group. A group with no members reports ready with no endpoints and waits,
    /// as Go's does.
    pub fn new() -> Self {
        Self {
            members: Vec::new(),
            transports: None,
            addr: None,
            headers: None,
            interceptors: Interceptors::new(),
            stateless: false,
            combined: false,
            base_path: None,
            health: None,
        }
    }

    /// Adds one service, which keeps its own base path, extras and cache hints.
    #[must_use]
    pub fn with<H: ServerHandler>(mut self, service: Service<H>) -> Self {
        self.members.push(service.boxed());
        self
    }

    /// [`ServiceGroup::with`] for a service whose type is already erased.
    #[must_use]
    pub fn with_boxed(mut self, service: Service<BoxedHandler>) -> Self {
        self.members.push(service);
        self
    }

    /// Chooses the transports every member answers on.
    #[must_use]
    pub fn transports(mut self, transports: impl IntoIterator<Item = Transport>) -> Self {
        self.transports = Some(transports.into_iter().collect());
        self
    }

    /// Gives the group a listen address of its own.
    #[must_use]
    pub fn addr(mut self, addr: impl Into<String>) -> Self {
        self.addr = Some(addr.into());
        self
    }

    /// Forwards these headers for every member.
    #[must_use]
    pub fn headers(mut self, mappings: impl IntoIterator<Item = HeaderMapping>) -> Self {
        self.headers = Some(mappings.into_iter().collect());
        self
    }

    /// Runs `interceptors` around every member's tool calls, outside the member's own chain.
    #[must_use]
    pub fn interceptors(mut self, interceptors: Interceptors) -> Self {
        self.interceptors = interceptors;
        self
    }

    /// Serves every member's streamable HTTP without sessions.
    #[must_use]
    pub fn stateless(mut self) -> Self {
        self.stateless = true;
        self
    }

    /// Serves the group as ONE server at [`ServiceGroup::base_path`], else
    /// [`DEFAULT_BASE_PATH`], so an agent connects once and sees every member's tools. The
    /// server reports the runtime's name and version, and its description as instructions.
    ///
    /// Two members declaring the same tool, prompt, resource or resource template fail the
    /// start with an error naming each clash. To keep the per-service paths while clients move
    /// over, register the same services again in a group without this.
    #[must_use]
    pub fn combined(mut self) -> Self {
        self.combined = true;
        self
    }

    /// Where a combined group mounts.
    #[must_use]
    pub fn base_path(mut self, path: impl Into<String>) -> Self {
        self.base_path = Some(path.into());
        self
    }

    /// Mounts a health probe at `path` running `check`, once for the whole group.
    #[must_use]
    pub fn health_check(mut self, path: impl Into<String>, check: HealthCheck) -> Self {
        let mut path = path.into();
        if !path.starts_with('/') {
            path.insert(0, '/');
        }
        self.health = Some((path, check));
        self
    }

    /// The transports the group answers on.
    pub(super) fn resolved_transports(&self) -> Vec<Transport> {
        self.transports
            .clone()
            .or_else(|| self.members.first().map(|m| m.transports.clone()))
            .unwrap_or_else(|| vec![Transport::StreamableHttp])
    }

    /// Whether any transport needs a listener.
    pub(super) fn serves_http(&self) -> bool {
        self.resolved_transports().iter().any(Transport::listens)
    }
}

impl Default for ServiceGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ServiceGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceGroup")
            .field("members", &self.members.len())
            .field("combined", &self.combined)
            .field("stateless", &self.stateless)
            .finish_non_exhaustive()
    }
}
