//! A type-erased [`ServerHandler`], so handlers of different types can live in one list.
//!
//! rmcp's handler trait returns `impl Future`, which makes it impossible to put behind
//! `dyn`. A group of generated services — each its own `FooServiceMcpHandler<T>` type — and
//! the combined server built from them both need exactly that, so [`DynHandler`] restates
//! the methods a tool server answers with boxed futures, and [`BoxedHandler`] turns one back
//! into a [`ServerHandler`].

use std::borrow::Cow;
use std::sync::Arc;

use futures::future::BoxFuture;
use rmcp::ErrorData as McpError;
use rmcp::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CompleteRequestParams, CompleteResult,
    GetPromptRequestParams, GetPromptResponse, InitializeRequestParams, InitializeResult,
    ListPromptsResult, ListResourceTemplatesResult, ListResourcesResult, ListToolsResult,
    PaginatedRequestParams, ProtocolVersion, ReadResourceRequestParams, ReadResourceResponse,
    ServerConfig as ServerInfo, Tool,
};
use rmcp::service::{NotificationContext, RequestContext, RoleServer};

/// The object-safe face of a [`ServerHandler`]. Every handler is one, by the blanket impl.
pub trait DynHandler: Send + Sync + 'static {
    /// [`ServerHandler::get_info`].
    fn dyn_get_info(&self) -> ServerInfo;
    /// [`ServerHandler::supported_protocol_versions`].
    fn dyn_supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]>;
    /// [`ServerHandler::initialize`].
    fn dyn_initialize(
        &self,
        request: InitializeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> BoxFuture<'_, Result<InitializeResult, McpError>>;
    /// [`ServerHandler::on_initialized`].
    fn dyn_on_initialized(&self, context: NotificationContext<RoleServer>) -> BoxFuture<'_, ()>;
    /// [`ServerHandler::complete`].
    fn dyn_complete(
        &self,
        request: CompleteRequestParams,
        context: RequestContext<RoleServer>,
    ) -> BoxFuture<'_, Result<CompleteResult, McpError>>;
    /// [`ServerHandler::get_prompt`].
    fn dyn_get_prompt(
        &self,
        request: GetPromptRequestParams,
        context: RequestContext<RoleServer>,
    ) -> BoxFuture<'_, Result<GetPromptResponse, McpError>>;
    /// [`ServerHandler::list_prompts`].
    fn dyn_list_prompts(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> BoxFuture<'_, Result<ListPromptsResult, McpError>>;
    /// [`ServerHandler::list_resources`].
    fn dyn_list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> BoxFuture<'_, Result<ListResourcesResult, McpError>>;
    /// [`ServerHandler::list_resource_templates`].
    fn dyn_list_resource_templates(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> BoxFuture<'_, Result<ListResourceTemplatesResult, McpError>>;
    /// [`ServerHandler::read_resource`].
    fn dyn_read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> BoxFuture<'_, Result<ReadResourceResponse, McpError>>;
    /// [`ServerHandler::call_tool`].
    fn dyn_call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> BoxFuture<'_, Result<CallToolResponse, McpError>>;
    /// [`ServerHandler::list_tools`].
    fn dyn_list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> BoxFuture<'_, Result<ListToolsResult, McpError>>;
    /// [`ServerHandler::get_tool`].
    fn dyn_get_tool(&self, name: &str) -> Option<Tool>;
}

impl<H: ServerHandler> DynHandler for H {
    fn dyn_get_info(&self) -> ServerInfo {
        self.get_info()
    }
    fn dyn_supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        ServerHandler::supported_protocol_versions(self)
    }
    fn dyn_initialize(
        &self,
        request: InitializeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> BoxFuture<'_, Result<InitializeResult, McpError>> {
        Box::pin(self.initialize(request, context))
    }
    fn dyn_on_initialized(&self, context: NotificationContext<RoleServer>) -> BoxFuture<'_, ()> {
        Box::pin(self.on_initialized(context))
    }
    fn dyn_complete(
        &self,
        request: CompleteRequestParams,
        context: RequestContext<RoleServer>,
    ) -> BoxFuture<'_, Result<CompleteResult, McpError>> {
        Box::pin(self.complete(request, context))
    }
    fn dyn_get_prompt(
        &self,
        request: GetPromptRequestParams,
        context: RequestContext<RoleServer>,
    ) -> BoxFuture<'_, Result<GetPromptResponse, McpError>> {
        Box::pin(self.get_prompt(request, context))
    }
    fn dyn_list_prompts(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> BoxFuture<'_, Result<ListPromptsResult, McpError>> {
        Box::pin(self.list_prompts(request, context))
    }
    fn dyn_list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> BoxFuture<'_, Result<ListResourcesResult, McpError>> {
        Box::pin(self.list_resources(request, context))
    }
    fn dyn_list_resource_templates(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> BoxFuture<'_, Result<ListResourceTemplatesResult, McpError>> {
        Box::pin(self.list_resource_templates(request, context))
    }
    fn dyn_read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> BoxFuture<'_, Result<ReadResourceResponse, McpError>> {
        Box::pin(self.read_resource(request, context))
    }
    fn dyn_call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> BoxFuture<'_, Result<CallToolResponse, McpError>> {
        Box::pin(self.call_tool(request, context))
    }
    fn dyn_list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> BoxFuture<'_, Result<ListToolsResult, McpError>> {
        Box::pin(self.list_tools(request, context))
    }
    fn dyn_get_tool(&self, name: &str) -> Option<Tool> {
        self.get_tool(name)
    }
}

/// A [`DynHandler`] behind an `Arc`, usable wherever rmcp wants a [`ServerHandler`].
#[derive(Clone)]
pub struct BoxedHandler(pub(crate) Arc<dyn DynHandler>);

impl BoxedHandler {
    /// Erases `handler`'s type.
    pub fn new(handler: impl ServerHandler) -> Self {
        Self(Arc::new(handler))
    }
}

impl std::fmt::Debug for BoxedHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoxedHandler").finish_non_exhaustive()
    }
}

impl ServerHandler for BoxedHandler {
    fn get_info(&self) -> ServerInfo {
        self.0.dyn_get_info()
    }
    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        self.0.dyn_supported_protocol_versions()
    }
    async fn initialize(
        &self,
        request: InitializeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<InitializeResult, McpError> {
        self.0.dyn_initialize(request, context).await
    }
    async fn on_initialized(&self, context: NotificationContext<RoleServer>) {
        self.0.dyn_on_initialized(context).await;
    }
    async fn complete(
        &self,
        request: CompleteRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CompleteResult, McpError> {
        self.0.dyn_complete(request, context).await
    }
    async fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<GetPromptResponse, McpError> {
        self.0.dyn_get_prompt(request, context).await
    }
    async fn list_prompts(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListPromptsResult, McpError> {
        self.0.dyn_list_prompts(request, context).await
    }
    async fn list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        self.0.dyn_list_resources(request, context).await
    }
    async fn list_resource_templates(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, McpError> {
        self.0.dyn_list_resource_templates(request, context).await
    }
    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, McpError> {
        self.0.dyn_read_resource(request, context).await
    }
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, McpError> {
        self.0.dyn_call_tool(request, context).await
    }
    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        self.0.dyn_list_tools(request, context).await
    }
    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.0.dyn_get_tool(name)
    }
}
