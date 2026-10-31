//! The SDK types generated code and tests name, so they need only this crate.
//!
//! Go re-exports the go-sdk's types as aliases for the same reason: a program written against
//! the runtime should not have to pin the SDK itself and keep the two versions in step. Here
//! the SDK is [`rmcp`], and the whole crate is also re-exported as [`crate::mcp::rmcp`].

pub use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ClientCapabilities, CompleteResult,
    ContentBlock, ElicitRequestParams, ElicitResult, ElicitationAction, ElicitationSchema,
    GetPromptRequestParams, GetPromptResponse, GetPromptResult, Icon, Implementation, InputRequest,
    InputRequests, InputRequiredResult, InputResponses, ListToolsResult, ProgressNotificationParam,
    ProgressToken, Prompt, PromptArgument, PromptMessage, ReadResourceRequestParams,
    ReadResourceResponse, ReadResourceResult, Resource, ResourceContents, ResourceTemplate, Role,
    ServerCapabilities, ServerConfig as ServerInfo, Tool, ToolAnnotations,
};
pub use rmcp::service::{Peer, RequestContext, RoleServer};
pub use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService,
};
pub use rmcp::{ErrorData as McpError, ServerHandler, ServiceExt};
