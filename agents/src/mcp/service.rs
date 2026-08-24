//! The tool server as a registrable service.

use std::sync::Arc;

use rmcp::ServerHandler;

use crate::shared::HeaderMapping;

use super::{CacheHints, DEFAULT_BASE_PATH, ExtraProperty, Interceptors, Transport};

/// Builds one MCP handler per session.
///
/// It is a factory rather than a single handler because Streamable HTTP gives each session
/// its own service instance, which is what lets one tool server hold per-session state
/// without sessions seeing each other's.
type HandlerFactory<H> = Arc<dyn Fn() -> Result<H, std::io::Error> + Send + Sync>;

/// A tool server registered on a [`crate::Runtime`].
pub struct Service<H: ServerHandler> {
    pub(super) factory: HandlerFactory<H>,
    pub(super) transports: Vec<Transport>,
    pub(super) addr: Option<String>,
    pub(super) base_path: String,
    pub(super) headers: Vec<HeaderMapping>,
    pub(super) extras: Vec<ExtraProperty>,
    pub(super) interceptors: Interceptors,
    pub(super) cache: CacheHints,
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
    /// proto-derived path and does not need this.
    #[must_use]
    pub fn base_path(mut self, path: impl Into<String>) -> Self {
        self.base_path = path.into();
        self
    }

    /// Forwards HTTP headers into gRPC metadata for tool calls, so an MCP call carries the
    /// same credentials and trace ids as the wire RPC behind it.
    #[must_use]
    pub fn headers(mut self, mappings: impl IntoIterator<Item = HeaderMapping>) -> Self {
        self.headers = mappings.into_iter().collect();
        self
    }

    /// Declares extra properties, injected into every tool's schema and lifted back out of
    /// incoming arguments before the request is decoded.
    #[must_use]
    pub fn extras(mut self, extras: impl IntoIterator<Item = ExtraProperty>) -> Self {
        self.extras = extras.into_iter().collect();
        self
    }

    /// The extra properties this server declares, for generated code building its tool table.
    pub fn declared_extras(&self) -> &[ExtraProperty] {
        &self.extras
    }

    /// Installs middleware around tool dispatch, so an MCP call runs the same validation,
    /// auth and tracing as the wire RPC behind it.
    #[must_use]
    pub fn interceptors(mut self, interceptors: Interceptors) -> Self {
        self.interceptors = interceptors;
        self
    }

    /// The interceptor chain, for a handler dispatching through it.
    pub fn chain(&self) -> &Interceptors {
        &self.interceptors
    }

    /// Declares how long clients may treat this server's results as fresh.
    #[must_use]
    pub fn cache_hints(mut self, hints: CacheHints) -> Self {
        self.cache = hints;
        self
    }

    /// The declared cache hints.
    pub fn declared_cache_hints(&self) -> &CacheHints {
        &self.cache
    }

    /// Whether any configured transport needs a listener.
    pub(super) fn serves_http(&self) -> bool {
        self.transports.iter().any(Transport::listens)
    }
}
