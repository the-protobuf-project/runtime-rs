//! One MCP server carrying several services, so an agent connects once and sees every
//! service's tools.
//!
//! Go builds one SDK server and lets every generated registration add to it. rmcp has no
//! shared registry to add to — each generated handler *is* a server — so [`CombinedServer`]
//! routes instead: tools and prompts by name, resources by URI, to whichever service declared
//! them, and list results are the services' lists end to end.
//!
//! Two services declaring the same tool, prompt, resource or resource template is an error
//! naming each clash, as in Go: the SDK would let the second silently answer for the first,
//! and on a combined server that is one service's tool answering for another's. Each service
//! is inventoried exactly as Go does it — over an in-memory session, the way a client reads
//! it — because that is the only view of a handler's declarations that cannot drift from what
//! it serves.

use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use rmcp::ErrorData as McpError;
use rmcp::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CompleteRequestParams, CompleteResult,
    GetPromptRequestParams, GetPromptResponse, Implementation, ListPromptsResult,
    ListResourceTemplatesResult, ListResourcesResult, ListToolsResult, PaginatedRequestParams,
    ReadResourceRequestParams, ReadResourceResponse, ServerCapabilities,
    ServerConfig as ServerInfo,
};
use rmcp::service::{RequestContext, RoleServer};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use super::handler::BoxedHandler;

/// Builds one session's instance of a member service.
pub type HandlerFactory = Arc<dyn Fn() -> Result<BoxedHandler, io::Error> + Send + Sync>;

/// What one service declares, as a client reads it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Inventory {
    /// Tool names.
    pub tools: Vec<String>,
    /// Prompt names.
    pub prompts: Vec<String>,
    /// Resource URIs.
    pub resources: Vec<String>,
    /// Resource template URI templates.
    pub resource_templates: Vec<String>,
}

impl Inventory {
    /// Every declaration, named as an error message names it.
    fn names(&self) -> Vec<String> {
        let tools = self.tools.iter().map(|n| format!("tool {n:?}"));
        let prompts = self.prompts.iter().map(|n| format!("prompt {n:?}"));
        let resources = self.resources.iter().map(|n| format!("resource {n:?}"));
        let templates = self
            .resource_templates
            .iter()
            .map(|n| format!("resource template {n:?}"));
        tools
            .chain(prompts)
            .chain(resources)
            .chain(templates)
            .collect()
    }
}

/// Combining services failed: one could not be inventoried, or several clash.
#[derive(Debug, thiserror::Error)]
pub enum CombineError {
    /// A service could not be built or read back.
    #[error("mcp: inventorying service #{index}: {message}")]
    Inventory {
        /// The service's position.
        index: usize,
        /// What went wrong.
        message: String,
    },
    /// Names two services both declare, one line per clash, as Go's `errors.Join` prints.
    #[error("{}", .0.join("\n"))]
    Collisions(Vec<String>),
}

/// Lists what `handler` declares by asking it over an in-memory session.
///
/// # Errors
///
/// The session failed or answered with something that is not a list.
pub async fn inventory(handler: BoxedHandler) -> Result<Inventory, String> {
    let (server_io, client_io) = tokio::io::duplex(64 * 1024);
    let server = tokio::spawn(async move {
        if let Ok(running) = rmcp::serve_server(handler, server_io).await {
            let _ = running.waiting().await;
        }
    });
    let result = Probe::new(client_io).run().await;
    server.abort();
    result
}

/// The client half of an inventory session: just enough JSON-RPC over newline-delimited
/// JSON to read the four lists.
struct Probe {
    reader: BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>,
    writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
    next_id: u64,
}

impl Probe {
    fn new(io: tokio::io::DuplexStream) -> Self {
        let (reader, writer) = tokio::io::split(io);
        Self {
            reader: BufReader::new(reader),
            writer,
            next_id: 0,
        }
    }

    async fn send(&mut self, message: Value) -> Result<(), String> {
        let mut line = message.to_string();
        line.push('\n');
        self.writer
            .write_all(line.as_bytes())
            .await
            .map_err(|err| err.to_string())
    }

    async fn request(&mut self, method: &str, params: Value) -> Result<Value, String> {
        self.next_id += 1;
        let id = self.next_id;
        self.send(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
            .await?;
        loop {
            let mut line = String::new();
            let read = self
                .reader
                .read_line(&mut line)
                .await
                .map_err(|err| err.to_string())?;
            if read == 0 {
                return Err(format!("{method}: session closed"));
            }
            let Ok(message) = serde_json::from_str::<Value>(&line) else {
                continue;
            };
            if message["id"] != json!(id) {
                continue;
            }
            if let Some(error) = message.get("error") {
                return Err(format!("{method}: {error}"));
            }
            return Ok(message["result"].clone());
        }
    }

    /// Every page of one list, by `field` of each item.
    async fn list(
        &mut self,
        method: &str,
        items: &str,
        field: &str,
    ) -> Result<Vec<String>, String> {
        let mut names = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let params = match &cursor {
                Some(cursor) => json!({"cursor": cursor}),
                None => json!({}),
            };
            let page = self.request(method, params).await?;
            for item in page[items].as_array().into_iter().flatten() {
                if let Some(name) = item[field].as_str() {
                    names.push(name.to_string());
                }
            }
            match page["nextCursor"].as_str() {
                Some(next) if !next.is_empty() => cursor = Some(next.to_string()),
                _ => return Ok(names),
            }
        }
    }

    async fn run(mut self) -> Result<Inventory, String> {
        let initialized = self
            .request(
                "initialize",
                json!({
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "inventory", "version": "1"},
                }),
            )
            .await?;
        self.send(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}))
            .await?;

        // List only what the server advertises: asking for a kind it has none of is an
        // error, not an empty list.
        let capabilities = &initialized["capabilities"];
        let mut inventory = Inventory::default();
        if capabilities.get("tools").is_some() {
            inventory.tools = self.list("tools/list", "tools", "name").await?;
        }
        if capabilities.get("prompts").is_some() {
            inventory.prompts = self.list("prompts/list", "prompts", "name").await?;
        }
        if capabilities.get("resources").is_some() {
            inventory.resources = self.list("resources/list", "resources", "uri").await?;
            inventory.resource_templates = self
                .list(
                    "resources/templates/list",
                    "resourceTemplates",
                    "uriTemplate",
                )
                .await?;
        }
        Ok(inventory)
    }
}

/// Reports every name two inventories declare.
fn check_collisions(inventories: &[Inventory]) -> Result<(), CombineError> {
    let mut owner: HashMap<String, usize> = HashMap::new();
    let mut clashes = Vec::new();
    for (index, inventory) in inventories.iter().enumerate() {
        for name in inventory.names() {
            if let Some(first) = owner.get(&name) {
                clashes.push(format!(
                    "mcp: {name} is declared by both service #{first} and service #{index}; \
                     every name on a combined server must be unique"
                ));
                continue;
            }
            owner.insert(name, index);
        }
    }
    if clashes.is_empty() {
        Ok(())
    } else {
        Err(CombineError::Collisions(clashes))
    }
}

/// Which member answers what.
struct Routing {
    info: ServerInfo,
    tools: HashMap<String, usize>,
    prompts: HashMap<String, usize>,
    resources: HashMap<String, usize>,
}

/// Several services as one server: Go's `NewCombinedServer` and `CombineServices`.
#[derive(Clone)]
pub struct CombinedServer {
    factories: Vec<HandlerFactory>,
    routing: Arc<Routing>,
}

impl std::fmt::Debug for CombinedServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CombinedServer")
            .field("services", &self.factories.len())
            .field("tools", &self.routing.tools.len())
            .finish_non_exhaustive()
    }
}

impl CombinedServer {
    /// Combines `factories` into one server named `name` at `version`, with `instructions`
    /// as what it tells a client it is for. No one service's identity speaks for the rest.
    ///
    /// # Errors
    ///
    /// A service could not be built or inventoried, or two declare the same name.
    pub async fn new(
        name: &str,
        version: &str,
        instructions: &str,
        factories: Vec<HandlerFactory>,
    ) -> Result<Self, CombineError> {
        let mut inventories = Vec::with_capacity(factories.len());
        let mut capabilities = ServerCapabilities::default();
        for (index, factory) in factories.iter().enumerate() {
            let handler = factory().map_err(|err| CombineError::Inventory {
                index,
                message: err.to_string(),
            })?;
            merge_capabilities(&mut capabilities, handler.get_info().capabilities);
            let found = inventory(handler)
                .await
                .map_err(|message| CombineError::Inventory { index, message })?;
            inventories.push(found);
        }
        check_collisions(&inventories)?;

        let mut routing = Routing {
            info: {
                let mut info = ServerInfo::new(capabilities)
                    .with_server_info(Implementation::new(name.to_string(), version.to_string()));
                if !instructions.is_empty() {
                    info = info.with_instructions(instructions.to_string());
                }
                info
            },
            tools: HashMap::new(),
            prompts: HashMap::new(),
            resources: HashMap::new(),
        };
        for (index, inventory) in inventories.into_iter().enumerate() {
            routing
                .tools
                .extend(inventory.tools.into_iter().map(|n| (n, index)));
            routing
                .prompts
                .extend(inventory.prompts.into_iter().map(|n| (n, index)));
            routing
                .resources
                .extend(inventory.resources.into_iter().map(|n| (n, index)));
        }
        tracing::debug!(
            services = factories.len(),
            tools = routing.tools.len(),
            "mcp: combined server built"
        );
        Ok(Self {
            factories,
            routing: Arc::new(routing),
        })
    }

    /// [`CombinedServer::new`] for handlers already built, each shared by every session.
    ///
    /// # Errors
    ///
    /// As [`CombinedServer::new`].
    pub async fn from_handlers(
        name: &str,
        version: &str,
        instructions: &str,
        handlers: Vec<BoxedHandler>,
    ) -> Result<Self, CombineError> {
        let factories = handlers
            .into_iter()
            .map(|handler| Arc::new(move || Ok(handler.clone())) as HandlerFactory)
            .collect();
        Self::new(name, version, instructions, factories).await
    }

    /// One session's handler, with a fresh instance of every service.
    ///
    /// # Errors
    ///
    /// A service's factory failed.
    pub fn session(&self) -> Result<CombinedHandler, io::Error> {
        let members = self
            .factories
            .iter()
            .map(|factory| factory())
            .collect::<Result<_, _>>()?;
        Ok(CombinedHandler {
            members,
            routing: Arc::clone(&self.routing),
        })
    }
}

/// Takes each capability from the first service that has it.
fn merge_capabilities(into: &mut ServerCapabilities, from: ServerCapabilities) {
    if into.tools.is_none() {
        into.tools = from.tools;
    }
    if into.prompts.is_none() {
        into.prompts = from.prompts;
    }
    if into.resources.is_none() {
        into.resources = from.resources;
    }
    if into.completions.is_none() {
        into.completions = from.completions;
    }
    if into.logging.is_none() {
        into.logging = from.logging;
    }
}

/// One session of a [`CombinedServer`].
#[derive(Clone)]
pub struct CombinedHandler {
    members: Vec<BoxedHandler>,
    routing: Arc<Routing>,
}

impl std::fmt::Debug for CombinedHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CombinedHandler")
            .field("services", &self.members.len())
            .finish_non_exhaustive()
    }
}

impl CombinedHandler {
    fn member(
        &self,
        index: Option<&usize>,
        kind: &str,
        name: &str,
    ) -> Result<&BoxedHandler, McpError> {
        index
            .and_then(|index| self.members.get(*index))
            .ok_or_else(|| McpError::invalid_params(format!("unknown {kind}: {name}"), None))
    }
}

impl ServerHandler for CombinedHandler {
    fn get_info(&self) -> ServerInfo {
        self.routing.info.clone()
    }

    /// Asks each service in turn and answers with the first that offers any value. Each
    /// knows only its own prompts and answers an empty list for the rest, so at most one has
    /// anything to say.
    async fn complete(
        &self,
        request: CompleteRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CompleteResult, McpError> {
        let mut first_error = None;
        for member in &self.members {
            match member.complete(request.clone(), context.clone()).await {
                Ok(result) if !result.completion.values.is_empty() => return Ok(result),
                Ok(_) => {}
                Err(err) => {
                    first_error.get_or_insert(err);
                }
            }
        }
        match first_error {
            Some(err) => Err(err),
            None => Ok(CompleteResult::default()),
        }
    }

    async fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<GetPromptResponse, McpError> {
        let name = request.name.clone();
        self.member(self.routing.prompts.get(&name), "prompt", &name)?
            .get_prompt(request, context)
            .await
    }

    async fn list_prompts(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListPromptsResult, McpError> {
        let mut all = Vec::new();
        for member in &self.members {
            if member.get_info().capabilities.prompts.is_some() {
                all.extend(member.list_prompts(None, context.clone()).await?.prompts);
            }
        }
        Ok(ListPromptsResult::with_all_items(all))
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        let mut all = Vec::new();
        for member in &self.members {
            if member.get_info().capabilities.resources.is_some() {
                all.extend(
                    member
                        .list_resources(None, context.clone())
                        .await?
                        .resources,
                );
            }
        }
        Ok(ListResourcesResult::with_all_items(all))
    }

    async fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourceTemplatesResult, McpError> {
        let mut all = Vec::new();
        for member in &self.members {
            if member.get_info().capabilities.resources.is_some() {
                all.extend(
                    member
                        .list_resource_templates(None, context.clone())
                        .await?
                        .resource_templates,
                );
            }
        }
        Ok(ListResourceTemplatesResult::with_all_items(all))
    }

    /// A declared URI goes to its service; anything else may match a template, so each
    /// service is asked in turn and the first to answer wins.
    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, McpError> {
        if let Some(index) = self.routing.resources.get(&request.uri) {
            return self.members[*index].read_resource(request, context).await;
        }
        let mut last =
            McpError::resource_not_found(format!("unknown resource: {}", request.uri), None);
        for member in &self.members {
            match member.read_resource(request.clone(), context.clone()).await {
                Ok(response) => return Ok(response),
                Err(err) => last = err,
            }
        }
        Err(last)
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, McpError> {
        let name = request.name.to_string();
        self.member(self.routing.tools.get(&name), "tool", &name)?
            .call_tool(request, context)
            .await
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        let mut all = Vec::new();
        for member in &self.members {
            if member.get_info().capabilities.tools.is_some() {
                all.extend(member.list_tools(None, context.clone()).await?.tools);
            }
        }
        Ok(ListToolsResult::with_all_items(all))
    }
}
