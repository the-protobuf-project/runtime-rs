//! What the runtime wraps around a handler it hosts.
//!
//! A generated `FooServiceMcpHandler` dispatches its own tools and knows nothing of extras,
//! interceptors, cache hints, header mappings or resource overrides. Go threads those through
//! `Config` into generated registration code, and through receiving middleware on the SDK's
//! server. rmcp has no middleware seam, so [`Hosted`] is that seam: a [`ServerHandler`] that
//! applies the service's settings and delegates to the handler inside it.
//!
//! It also performs the SEP-2322 round trip for clients older than 2026-07-28, which Go's SDK
//! does in its own server middleware: a handler that returns an input-required result is
//! answered through an ordinary elicitation request and called again, so it needs one path.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rmcp::ErrorData as McpError;
use rmcp::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, CompleteRequestParams, CompleteResult,
    GetPromptRequestParams, GetPromptResponse, InitializeRequestParams, InitializeResult,
    InputRequest, InputRequests, InputResponses, ListPromptsResult, ListResourceTemplatesResult,
    ListResourcesResult, ListToolsResult, PaginatedRequestParams, ProtocolVersion,
    ReadResourceRequestParams, ReadResourceResponse, ServerConfig as ServerInfo, Tool,
};
use rmcp::service::{NotificationContext, RequestContext, RoleServer};
use serde_json::{Map, Value};
use tonic::metadata::MetadataMap;

use super::config::ResourceHandler;
use super::{
    CacheHint, CacheHints, ElicitHook, ExtraProperty, ExtraValues, Interceptors, ToolHandler,
    extract_extras, prepare_tool_with_extras, structured_result,
};
use crate::shared::{
    CallContext, ForwardedHeaders, HeaderMapping, current_call_context, incoming_headers,
    with_call_context,
};

/// How many input-required rounds one legacy tool call may take before it is refused, so a
/// handler that keeps asking cannot hold a request forever.
const MAX_ELICITATION_ROUNDS: usize = 8;

/// The settings a hosted handler runs with.
#[derive(Clone, Default)]
pub(crate) struct Hosting {
    pub(crate) extras: Vec<ExtraProperty>,
    pub(crate) interceptors: Interceptors,
    pub(crate) cache: CacheHints,
    pub(crate) resource_handlers: HashMap<String, ResourceHandler>,
    pub(crate) headers: Vec<HeaderMapping>,
    pub(crate) elicit_hook: Option<ElicitHook>,
    pub(crate) methods: HashMap<String, String>,
}

/// A handler with its service's settings applied.
#[derive(Clone)]
pub(crate) struct Hosted<H> {
    pub(crate) inner: H,
    pub(crate) hosting: Arc<Hosting>,
}

/// The caller's headers, for in-process gRPC made during this request.
///
/// rmcp records each HTTP request's head on the request it hands the handler, so this reads
/// it per request rather than per session — which is what keeps a stateful session from
/// passing one caller's credentials off as the next one's.
///
/// A layer with no mappings of its own keeps whatever an outer layer — a group's — already
/// established, rather than clearing it.
fn call_context(context: &RequestContext<RoleServer>, mappings: &[HeaderMapping]) -> CallContext {
    if mappings.is_empty()
        && let Some(outer) = current_call_context()
    {
        return outer;
    }
    let parts = context.extensions.get::<http::request::Parts>();
    CallContext {
        incoming: incoming_headers(&MetadataMap::new(), mappings, parts.map(|p| &p.headers)),
        forwarded: parts.and_then(|p| p.extensions.get::<ForwardedHeaders>().cloned()),
    }
}

/// Whether the client speaks a protocol older than 2026-07-28, which cannot receive an
/// input-required result.
fn is_legacy(context: &RequestContext<RoleServer>) -> bool {
    context
        .protocol_version()
        .is_none_or(|version| version < ProtocolVersion::V_2026_07_28)
}

/// Answers a handler's input requests over ordinary server-to-client requests.
async fn fulfil(
    context: &RequestContext<RoleServer>,
    requests: InputRequests,
) -> Result<InputResponses, McpError> {
    let mut answers = InputResponses::new();
    for (id, request) in requests {
        let InputRequest::Elicitation(elicit) = request else {
            return Err(McpError::invalid_request(
                format!("mcp: input request {id:?} is not an elicitation"),
                None,
            ));
        };
        tracing::debug!(%id, "mcp: performing an elicitation round trip for a legacy client");
        let answer = context
            .peer
            .create_elicitation(elicit.params)
            .await
            .map_err(|err| McpError::internal_error(format!("mcp: elicitation: {err}"), None))?;
        let answer = serde_json::to_value(answer)
            .map_err(|err| McpError::internal_error(format!("mcp: elicitation: {err}"), None))?;
        answers.insert(id, answer);
    }
    Ok(answers)
}

/// Arguments as the object a request carries.
fn as_object(arguments: Value) -> Option<Map<String, Value>> {
    match arguments {
        Value::Object(map) => Some(map),
        _ => None,
    }
}

impl<H: ServerHandler> Hosted<H> {
    /// One dispatch of a tool call: extras lifted out, then the interceptor chain around the
    /// handler.
    async fn dispatch(
        &self,
        mut request: CallToolRequestParams,
        mut context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, McpError> {
        let hosting = &self.hosting;
        let arguments = request
            .arguments
            .take()
            .map_or_else(|| Value::Object(Map::new()), Value::Object);
        let (arguments, found) = extract_extras(arguments, &hosting.extras);
        if !found.is_empty() {
            let keyed = hosting
                .extras
                .iter()
                .filter_map(|extra| {
                    let value = found.get(&extra.name)?;
                    Some((extra.key().to_string(), value.clone()))
                })
                .collect();
            context.extensions.insert(ExtraValues(keyed));
        }

        if hosting.interceptors.is_empty() {
            request.arguments = as_object(arguments);
            return self.inner.call_tool(request, context).await;
        }

        let method = hosting
            .methods
            .get(request.name.as_ref())
            .cloned()
            .unwrap_or_else(|| request.name.to_string());
        // The chain sees and may rewrite the result as JSON; the handler's own response is
        // kept beside it so a non-JSON one (an input-required result) is never lost.
        let original: Arc<Mutex<Option<CallToolResponse>>> = Arc::default();
        let slot = Arc::clone(&original);
        let inner = &self.inner;
        let handler: ToolHandler<'_> = Box::new(move |arguments| {
            Box::pin(async move {
                request.arguments = as_object(arguments);
                let response = inner.call_tool(request, context).await?;
                let value = match &response {
                    CallToolResponse::Complete(result) => {
                        serde_json::to_value(result).unwrap_or(Value::Null)
                    }
                    _ => Value::Null,
                };
                *slot.lock().expect("mcp: response slot poisoned") = Some(response);
                Ok(value)
            })
        });
        let value = hosting
            .interceptors
            .invoke(&method, arguments, handler)
            .await?;
        let response = original.lock().expect("mcp: response slot poisoned").take();
        Ok(match response {
            Some(CallToolResponse::Complete(result)) => {
                serde_json::from_value(value).unwrap_or(result).into()
            }
            Some(other) => other,
            // An interceptor answered without calling the tool.
            None => serde_json::from_value::<CallToolResult>(value.clone())
                .unwrap_or_else(|_| structured_result(&value))
                .into(),
        })
    }
}

/// Stamps a list hint on a result that has the two cache fields.
macro_rules! stamp_list {
    ($result:expr, $hint:expr) => {
        match $hint {
            Some(CacheHint { ttl_ms, scope }) => {
                $result.with_ttl_ms(ttl_ms).with_cache_scope(scope.into())
            }
            None => $result,
        }
    };
}

impl<H: ServerHandler> ServerHandler for Hosted<H> {
    fn get_info(&self) -> ServerInfo {
        self.inner.get_info()
    }

    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        ServerHandler::supported_protocol_versions(&self.inner)
    }

    async fn initialize(
        &self,
        request: InitializeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<InitializeResult, McpError> {
        self.inner.initialize(request, context).await
    }

    async fn on_initialized(&self, context: NotificationContext<RoleServer>) {
        self.inner.on_initialized(context).await;
    }

    async fn complete(
        &self,
        request: CompleteRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CompleteResult, McpError> {
        self.inner.complete(request, context).await
    }

    async fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<GetPromptResponse, McpError> {
        let call = call_context(&context, &self.hosting.headers);
        with_call_context(call, self.inner.get_prompt(request, context)).await
    }

    async fn list_prompts(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListPromptsResult, McpError> {
        let result = self.inner.list_prompts(request, context).await?;
        Ok(stamp_list!(result, self.hosting.cache.list))
    }

    async fn list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        let result = self.inner.list_resources(request, context).await?;
        Ok(stamp_list!(result, self.hosting.cache.list))
    }

    async fn list_resource_templates(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, McpError> {
        let result = self.inner.list_resource_templates(request, context).await?;
        Ok(stamp_list!(result, self.hosting.cache.list))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, McpError> {
        let uri = request.uri.clone();
        let call = call_context(&context, &self.hosting.headers);
        let response = with_call_context(call, async {
            match self.hosting.resource_handlers.get(&uri) {
                Some(handler) => handler(request, context).await.map(Into::into),
                None => self.inner.read_resource(request, context).await,
            }
        })
        .await?;
        Ok(match response {
            ReadResourceResponse::Complete(result) => {
                self.hosting.cache.apply_to_read(&uri, result).into()
            }
            other => other,
        })
    }

    async fn call_tool(
        &self,
        mut request: CallToolRequestParams,
        mut context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, McpError> {
        if let Some(hook) = &self.hosting.elicit_hook {
            context.extensions.insert(Arc::clone(hook));
        }
        let call = call_context(&context, &self.hosting.headers);
        with_call_context(call, async move {
            for _ in 0..MAX_ELICITATION_ROUNDS {
                match self.dispatch(request.clone(), context.clone()).await? {
                    CallToolResponse::InputRequired(pending) if is_legacy(&context) => {
                        let answers =
                            fulfil(&context, pending.input_requests.unwrap_or_default()).await?;
                        let mut responses = request.input_responses.take().unwrap_or_default();
                        responses.extend(answers);
                        request.input_responses = Some(responses);
                        request.request_state = pending.request_state;
                    }
                    response => return Ok(response),
                }
            }
            Err(McpError::internal_error(
                format!(
                    "mcp: tool {:?} asked for input too many times",
                    request.name
                ),
                None,
            ))
        })
        .await
    }

    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        let mut result = self.inner.list_tools(request, context).await?;
        if !self.hosting.extras.is_empty() {
            result.tools = std::mem::take(&mut result.tools)
                .into_iter()
                .map(|tool| prepare_tool_with_extras(tool, &self.hosting.extras))
                .collect();
        }
        Ok(stamp_list!(result, self.hosting.cache.list))
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.inner
            .get_tool(name)
            .map(|tool| prepare_tool_with_extras(tool, &self.hosting.extras))
    }
}
