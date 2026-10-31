//! MCP on a hybrid server: a tool that calls a gRPC service through the server's loopback
//! runs the server's interceptor chain with the MCP caller's headers, and the combined
//! endpoint and path options mount what Go's do.

use std::net::TcpListener;
use std::sync::Arc;

use agents::mcp::rmcp::model::{
    CallToolRequestParams, CallToolResponse, Implementation, ListToolsResult,
    PaginatedRequestParams, ServerCapabilities, ServerConfig as ServerInfo,
};
use agents::mcp::rmcp::service::{RequestContext, RoleServer};
use agents::mcp::rmcp::{ErrorData as McpError, ServerHandler};
use agents::mcp::{self, HeaderMapping};
use grpc::options::{Endpoint, McpTransport, Options};
use grpc::{HybridServer, Loopback};
use serde_json::{Value, json};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_client::HealthClient;

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
        .expect("a free port")
}

/// A tool that asks the server's health service, through the loopback, whether it serves.
#[derive(Clone)]
struct Checker {
    name: &'static str,
    loopback: Loopback,
}

impl ServerHandler for Checker {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(self.name, "1.0.0"))
    }

    async fn list_tools(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        Ok(ListToolsResult::with_all_items(vec![mcp::create_tool(
            &format!("{}-check", self.name),
            "checks health",
            r#"{"type":"object"}"#,
        )]))
    }

    async fn call_tool(
        &self,
        _: CallToolRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, McpError> {
        let mut client = HealthClient::new(self.loopback.clone());
        let result = match client
            .check(HealthCheckRequest {
                service: String::new(),
            })
            .await
        {
            Ok(reply) => mcp::text_result(format!("status {}", reply.get_ref().status)),
            Err(status) => mcp::handle_error(&status),
        };
        Ok(result.into())
    }
}

/// A server whose interceptor demands `authorization: Bearer good`, with two MCP services.
fn server(grpc_port: u16, mcp_port: u16) -> HybridServer {
    let mut options = Options {
        service_name: "hybrid-mcp".into(),
        version: "2.0.0".into(),
        description: "tools over gRPC".into(),
        grpc: Endpoint::new("127.0.0.1", grpc_port),
        enable_health: true,
        enable_mcp: true,
        ..Default::default()
    };
    options.mcp.host = "127.0.0.1".into();
    options.mcp.port = mcp_port;
    options.mcp.transport = Some(McpTransport::StreamableHttp);
    HybridServer::new(options)
        .with_unary_interceptor(Arc::new(|request: tonic::Request<()>| {
            match request.metadata().get("authorization") {
                Some(value) if value == "Bearer good" => Ok(request),
                _ => Err(tonic::Status::unauthenticated("no credentials")),
            }
        }))
        .with_mcp_service(|loopback| {
            mcp::Service::from_handler(Checker {
                name: "alpha",
                loopback,
            })
            .base_path("/alpha/mcp")
        })
        .with_mcp_service(|loopback| {
            mcp::Service::from_handler(Checker {
                name: "beta",
                loopback,
            })
            .base_path("/beta/mcp")
        })
}

/// Initializes a session at `url` and calls `tool`, sending `auth` when given.
async fn call(url: &str, tool: &str, auth: Option<&str>) -> Value {
    let http = reqwest::Client::new();
    let post = |body: Value, session: Option<String>| {
        let mut request = http
            .post(url)
            .header("accept", "application/json, text/event-stream")
            .json(&body);
        if let Some(auth) = auth {
            request = request.header("authorization", auth);
        }
        if let Some(session) = session {
            request = request.header("mcp-session-id", session);
        }
        request.send()
    };
    let init = post(
        json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-06-18", "capabilities": {},
            "clientInfo": {"name": "t", "version": "1"}
        }}),
        None,
    )
    .await
    .expect("initialize");
    assert!(init.status().is_success(), "{}", init.status());
    let session = init
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let _ = init.text().await;
    let _ = post(
        json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
        session.clone(),
    )
    .await;
    let response = post(
        json!({"jsonrpc": "2.0", "id": 2, "method": "tools/call",
               "params": {"name": tool, "arguments": {}}}),
        session,
    )
    .await
    .expect("call");
    let text = response.text().await.unwrap_or_default();
    let payload = text
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .find(|p| !p.trim().is_empty())
        .unwrap_or(&text);
    serde_json::from_str(payload).unwrap_or(Value::Null)
}

fn text(response: &Value) -> String {
    response["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_default()
        .to_string()
}

#[tokio::test]
async fn the_server_interceptor_chain_runs_for_mcp_tools_with_the_callers_headers() {
    let (grpc_port, mcp_port) = (free_port(), free_port());
    let mut server = server(grpc_port, mcp_port);
    server.start().await.expect("start");
    let url = format!("http://127.0.0.1:{mcp_port}/alpha/mcp");

    let allowed = call(&url, "alpha-check", Some("Bearer good")).await;
    assert_eq!(text(&allowed), "status 1", "{allowed}");

    let refused = call(&url, "alpha-check", None).await;
    assert_eq!(refused["result"]["isError"], true, "{refused}");
    let payload: Value = serde_json::from_str(&text(&refused)).expect("an error payload");
    assert_eq!(payload["code"], "Unauthenticated");

    let endpoints = server.agent_endpoints();
    assert_eq!(endpoints.len(), 2, "{endpoints:?}");
    server.stop().await.expect("stop");
}

#[tokio::test]
async fn the_combined_endpoint_serves_every_service_and_can_replace_the_paths() {
    let (grpc_port, mcp_port) = (free_port(), free_port());
    let mut server = server(grpc_port, mcp_port)
        .with_mcp_endpoint("")
        .without_mcp_service_paths()
        .with_mcp_header_mappings([HeaderMapping::new("authorization", "authorization")]);
    server.start().await.expect("start");

    let combined = format!("http://127.0.0.1:{mcp_port}/mcp");
    for tool in ["alpha-check", "beta-check"] {
        let response = call(&combined, tool, Some("Bearer good")).await;
        assert_eq!(text(&response), "status 1", "{tool}: {response}");
    }

    let gone = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{mcp_port}/alpha/mcp"))
        .json(&json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}))
        .send()
        .await
        .expect("request");
    assert_eq!(gone.status(), reqwest::StatusCode::NOT_FOUND);

    let endpoints = server.agent_endpoints();
    assert_eq!(endpoints.len(), 1);
    assert_eq!(endpoints[0].detail, "/mcp");
    server.stop().await.expect("stop");
}

#[tokio::test]
async fn stateless_mcp_issues_no_session() {
    let (grpc_port, mcp_port) = (free_port(), free_port());
    let mut server = server(grpc_port, mcp_port).with_stateless_mcp();
    server.start().await.expect("start");

    let init = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{mcp_port}/beta/mcp"))
        .header("accept", "application/json, text/event-stream")
        .json(
            &json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
                "protocolVersion": "2025-06-18", "capabilities": {},
                "clientInfo": {"name": "t", "version": "1"}
            }}),
        )
        .send()
        .await
        .expect("initialize");
    assert!(init.status().is_success());
    assert!(init.headers().get("mcp-session-id").is_none());
    server.stop().await.expect("stop");
}
