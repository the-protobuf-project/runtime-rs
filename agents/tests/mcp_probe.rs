//! A hand-written handler the hosting tests drive: it reports back what the runtime did to
//! each call, so a test can assert on extras, headers and hooks from the outside.

#![allow(dead_code)]

use agents::mcp::{self, ElicitField, ElicitHook, Elicitation, EnumCompletions, ExtraValues};
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CompleteRequestParams, CompleteResult,
    GetPromptRequestParams, GetPromptResponse, Implementation, ListPromptsResult,
    ListResourcesResult, ListToolsResult, PaginatedRequestParams, Prompt,
    ReadResourceRequestParams, ReadResourceResponse, Resource, ServerCapabilities,
    ServerConfig as ServerInfo,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::{ErrorData as McpError, ServerHandler};
use serde_json::{Value, json};

/// Answers tools named after itself:
///
/// - `<name>` echoes its arguments and what the runtime put around the call;
/// - `<name>_ask` raises an [`mcp::ask`] question and books once it is answered;
/// - `<name>_confirm` runs the SEP-2322 handshake with [`mcp::run_elicitation`].
#[derive(Clone)]
pub struct Probe {
    pub name: &'static str,
}

impl Probe {
    pub fn new(name: &'static str) -> Self {
        Self { name }
    }

    fn fields() -> Vec<ElicitField> {
        vec![ElicitField::new("confirm", "Really?", "boolean").required()]
    }
}

impl ServerHandler for Probe {
    fn get_info(&self) -> ServerInfo {
        let mut capabilities = ServerCapabilities::builder()
            .enable_prompts()
            .enable_resources()
            .enable_tools()
            .build();
        capabilities.completions = Some(Default::default());
        ServerInfo::new(capabilities).with_server_info(Implementation::new(self.name, "1.0.0"))
    }

    async fn list_tools(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        let schema = r#"{"type":"object","properties":{"word":{"type":"string"}}}"#;
        Ok(ListToolsResult::with_all_items(vec![
            mcp::create_tool(self.name, "echo", schema),
            mcp::create_tool(&format!("{}_ask", self.name), "ask", schema),
            mcp::create_tool(&format!("{}_confirm", self.name), "confirm", schema),
        ]))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, McpError> {
        let arguments = Value::Object(request.arguments.clone().unwrap_or_default());
        let name = request.name.to_string();
        if name == self.name {
            let auth = agents::shared::current_call_context().and_then(|call| {
                call.incoming
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string)
            });
            let extras = context
                .extensions
                .get::<ExtraValues>()
                .map(|extras| Value::Object(extras.0.clone()));
            return Ok(mcp::structured_result(&json!({
                "service": self.name,
                "args": arguments,
                "extras": extras,
                "auth": auth,
                "hook": context.extensions.get::<ElicitHook>().is_some(),
            }))
            .into());
        }
        if name == format!("{}_ask", self.name) {
            let (merged, declined) = mcp::merge_answers(&request, arguments);
            if declined {
                return Ok(mcp::text_result("declined").into());
            }
            if merged.get("confirm").is_some() {
                return Ok(mcp::text_result(format!("booked {merged}")).into());
            }
            let status = mcp::ask(tonic::Code::FailedPrecondition, "Book it?", &Self::fields());
            return Ok(mcp::ask_pending(&context, &status).expect("an ask status"));
        }
        if name == format!("{}_confirm", self.name) {
            return match mcp::run_elicitation(&request, "Sure?", &Self::fields()) {
                Ok(Elicitation::Answered(answer)) => Ok(mcp::text_result(format!(
                    "{:?} {}",
                    answer.action,
                    answer.content.unwrap_or_default()
                ))
                .into()),
                Ok(Elicitation::Pending(pending)) => Ok(pending.into()),
                Err(err) => Err(McpError::invalid_params(err, None)),
            };
        }
        Err(McpError::invalid_params(
            format!("unknown tool {name}"),
            None,
        ))
    }

    async fn list_resources(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        Ok(ListResourcesResult::with_all_items(vec![Resource::new(
            format!("probe://{}", self.name),
            self.name,
        )]))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, McpError> {
        Ok(mcp::default_resource_result(&request.uri).into())
    }

    async fn list_prompts(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListPromptsResult, McpError> {
        Ok(ListPromptsResult::with_all_items(vec![Prompt::new(
            format!("{}_prompt", self.name),
            Some("a prompt"),
            None,
        )]))
    }

    async fn get_prompt(
        &self,
        _: GetPromptRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<GetPromptResponse, McpError> {
        Ok(mcp::default_prompt_result(self.name).into())
    }

    async fn complete(
        &self,
        request: CompleteRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<CompleteResult, McpError> {
        let completions = EnumCompletions::new(
            [(
                format!("{}_prompt:level", self.name),
                vec!["High".to_string(), "Low".to_string()],
            )]
            .into_iter()
            .collect(),
        );
        Ok(completions.complete_request(&request))
    }
}
