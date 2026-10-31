//! Per-handler settings in one value a caller can build up and hand over.
//!
//! Go states these as functional options folded into a `Config` by `ApplyOptions`, which
//! generated `RegisterFooMCPHandler` code then reads. Chained `with_*` methods are the same
//! idea, [`Config::default`] is what applying none gives, and [`crate::mcp::Service::config`]
//! is where a hosted service reads it — so generated Rust code never has to.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rmcp::ErrorData as McpError;
use rmcp::model::{ReadResourceRequestParams, ReadResourceResult};
use rmcp::service::{RequestContext, RoleServer};

use super::{ElicitHook, ExtraProperty, Interceptors, Transport};
use crate::shared::HeaderMapping;

/// Serves one resource's content in place of the generated placeholder. The resource's
/// metadata — title, media type, annotations — still comes from the proto.
pub type ResourceHandler = Arc<
    dyn Fn(
            ReadResourceRequestParams,
            RequestContext<RoleServer>,
        ) -> Pin<Box<dyn Future<Output = Result<ReadResourceResult, McpError>> + Send>>
        + Send
        + Sync,
>;

/// Runtime configuration for MCP handlers.
#[derive(Clone, Default)]
pub struct Config {
    /// Injected into tool schemas and lifted out of arguments.
    pub extra_properties: Vec<ExtraProperty>,
    /// HTTP headers forwarded into gRPC metadata.
    pub header_mappings: Vec<HeaderMapping>,
    /// The wire protocol.
    pub transport: Option<Transport>,
    /// The listen address for HTTP transports.
    pub addr: Option<String>,
    /// Called before an elicitation, to change its fields at runtime.
    pub elicit_hook: Option<ElicitHook>,
    /// Wrapped around every tool call.
    pub interceptors: Interceptors,
    /// Overrides, by resource URI, the placeholder for a declared resource.
    pub resource_handlers: HashMap<String, ResourceHandler>,
}

impl Config {
    /// Serves a resource's content from `handler` instead of the generated placeholder,
    /// keyed by the resource's URI.
    #[must_use]
    pub fn with_resource_handler(
        mut self,
        uri: impl Into<String>,
        handler: ResourceHandler,
    ) -> Self {
        self.resource_handlers.insert(uri.into(), handler);
        self
    }

    /// Adds extra properties to tool schemas.
    #[must_use]
    pub fn with_extra_properties(
        mut self,
        properties: impl IntoIterator<Item = ExtraProperty>,
    ) -> Self {
        self.extra_properties.extend(properties);
        self
    }

    /// Forwards HTTP headers into gRPC metadata.
    #[must_use]
    pub fn with_header_mappings(
        mut self,
        mappings: impl IntoIterator<Item = HeaderMapping>,
    ) -> Self {
        self.header_mappings.extend(mappings);
        self
    }

    /// Sets the transport.
    #[must_use]
    pub fn with_transport(mut self, transport: Transport) -> Self {
        self.transport = Some(transport);
        self
    }

    /// Sets the listen address for HTTP transports.
    #[must_use]
    pub fn with_addr(mut self, addr: impl Into<String>) -> Self {
        self.addr = Some(addr.into());
        self
    }

    /// Sets the hook called before every elicitation.
    #[must_use]
    pub fn with_elicit_hook(mut self, hook: ElicitHook) -> Self {
        self.elicit_hook = Some(hook);
        self
    }

    /// Sets the interceptors wrapped around every tool call.
    #[must_use]
    pub fn with_interceptors(mut self, interceptors: Interceptors) -> Self {
        self.interceptors = interceptors;
        self
    }
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("extra_properties", &self.extra_properties)
            .field("header_mappings", &self.header_mappings)
            .field("transport", &self.transport)
            .field("addr", &self.addr)
            .field("interceptors", &self.interceptors)
            .field("resource_handlers", &self.resource_handlers.len())
            .finish_non_exhaustive()
    }
}

/// The handler configured for `uri`, or `fallback` when none is.
pub fn resource_handler_for(
    config: Option<&Config>,
    uri: &str,
    fallback: ResourceHandler,
) -> ResourceHandler {
    config
        .and_then(|config| config.resource_handlers.get(uri).cloned())
        .unwrap_or(fallback)
}
