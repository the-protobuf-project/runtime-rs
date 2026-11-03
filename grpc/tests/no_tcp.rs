//! A local daemon's shape: gRPC on a Unix socket, REST off, and MCP on a Unix socket or on
//! stdio, whose tool reaches a gRPC service in-process — and not one TCP port open.
//!
//! The check counts the process's listening TCP sockets, after proving the count can see one.
//! Tests here serialise on a lock, because that proof briefly opens a TCP listener of its own.
//! The stdio case runs the server in a child process — this test binary again, under an
//! environment variable — since stdio is the process's own pipes.

#![cfg(unix)]

#[path = "common/local.rs"]
mod local;

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agents::mcp;
use agents::mcp::rmcp::model::{
    CallToolRequestParams, CallToolResponse, Implementation, ListToolsResult,
    PaginatedRequestParams, ServerCapabilities, ServerConfig as ServerInfo,
};
use agents::mcp::rmcp::service::{RequestContext, RoleServer};
use agents::mcp::rmcp::{ErrorData as McpError, ServerHandler};
use grpc::banner::BannerOutput;
use grpc::options::{Endpoint, McpTransport, Options};
use grpc::{HybridServer, Loopback};
use local::{assert_tcp_listeners_are_visible, channel, socket, tcp_listeners};
use rmcp::ServiceExt;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::transport::{StreamableHttpClientTransport, UnixSocketHttpClient};
use serde_json::{Value, json};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_client::HealthClient;

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Set in the child: the gRPC socket path it serves on.
const CHILD: &str = "TPP_GRPC_NO_TCP_STDIO_CHILD";

/// A tool that asks the health service, through the loopback, whether the server serves.
#[derive(Clone)]
struct Checker(Loopback);

impl ServerHandler for Checker {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("checker", "1.0.0"))
    }

    async fn list_tools(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        Ok(ListToolsResult::with_all_items(vec![mcp::create_tool(
            "check",
            "checks health",
            r#"{"type":"object"}"#,
        )]))
    }

    async fn call_tool(
        &self,
        _: CallToolRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, McpError> {
        let reply = HealthClient::new(self.0.clone())
            .check(HealthCheckRequest::default())
            .await;
        let result = match reply {
            Ok(reply) => mcp::text_result(format!("status {}", reply.get_ref().status)),
            Err(status) => mcp::handle_error(&status),
        };
        Ok(result.into())
    }
}

/// gRPC on `grpc`, REST off, MCP on `mcp_host` over `transport`.
fn local_server(grpc: &Path, mcp_host: String, transport: McpTransport) -> HybridServer {
    let mut options = Options {
        service_name: "no-tcp".into(),
        grpc: Endpoint::unix(grpc),
        enable_http: false,
        enable_health: true,
        enable_mcp: true,
        ..Default::default()
    };
    options.mcp.host = mcp_host;
    options.mcp.transport = Some(transport);
    HybridServer::new(options)
        .with_banner(BannerOutput::Off)
        .with_mcp_service(|loopback| {
            mcp::Service::from_handler(Checker(loopback)).base_path("/mcp")
        })
}

#[tokio::test]
async fn grpc_and_mcp_on_unix_sockets_open_no_tcp_port() {
    let _serial = SERIAL.lock().await;
    assert_tcp_listeners_are_visible();
    let (_dir, grpc) = socket("grpc");
    let mcp_path = grpc.with_file_name("mcp.sock");
    let mut server = local_server(
        &grpc,
        format!("unix:{}", mcp_path.display()),
        McpTransport::StreamableHttp,
    );
    server.start().await.expect("the server starts");
    assert_eq!(
        tcp_listeners(std::process::id()),
        0,
        "no TCP listener is open"
    );

    HealthClient::new(channel(&grpc).await.expect("gRPC over the socket"))
        .check(HealthCheckRequest::default())
        .await
        .expect("gRPC answers on the socket");

    let url = "http://localhost/mcp";
    let http = UnixSocketHttpClient::new(mcp_path.to_str().expect("utf-8"), url);
    let transport = StreamableHttpClientTransport::with_client(
        http,
        StreamableHttpClientTransportConfig::with_uri(url),
    );
    let client = ().serve(transport).await.expect("MCP initialises over its socket");
    let result = client
        .call_tool(CallToolRequestParams::new("check"))
        .await
        .expect("tools/call");
    assert!(format!("{result:?}").contains("status 1"), "{result:?}");
    client.cancel().await.expect("closes");

    assert_eq!(tcp_listeners(std::process::id()), 0, "still none");
    server.stop().await.expect("stops");
}

/// The child half of the stdio test: serves until killed. Passes at once in a normal run.
#[tokio::test]
async fn stdio_child() {
    let Ok(grpc) = std::env::var(CHILD) else {
        return;
    };
    let mut server = local_server(Path::new(&grpc), String::new(), McpTransport::Stdio);
    server.start().await.expect("the child's server starts");
    tokio::time::sleep(Duration::from_secs(60)).await;
}

#[test]
fn grpc_on_a_unix_socket_with_mcp_on_stdio_opens_no_tcp_port() {
    let _serial = SERIAL.blocking_lock();
    assert_tcp_listeners_are_visible();
    let (_dir, grpc) = socket("stdio");
    let mut child = Command::new(std::env::current_exe().expect("this test binary"))
        .args(["--exact", "stdio_child", "--nocapture", "--test-threads=1"])
        .env(CHILD, &grpc)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("the child starts");
    let pid = child.id();
    let mut input = child.stdin.take().expect("stdin");
    let mut output = BufReader::new(child.stdout.take().expect("stdout"));
    let child = Arc::new(Mutex::new(child));
    let watchdog = Arc::clone(&child);
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(30));
        let _ = watchdog.lock().map(|mut child| child.kill());
    });

    // The test harness writes its own text, and the first reply shares a line with its
    // `test stdio_child ... ` prefix, so each line is read from its first brace.
    let mut reply = |id: u64| -> Value {
        let mut line = String::new();
        loop {
            line.clear();
            let read = output.read_line(&mut line).expect("the child's stdout");
            assert!(read > 0, "the child closed stdout before answering {id}");
            let json = line.find('{').map_or("", |start| &line[start..]);
            if let Ok(message) = serde_json::from_str::<Value>(json)
                && message["id"] == id
            {
                return message;
            }
        }
    };
    let mut send = |message: Value| {
        writeln!(input, "{message}").expect("the child's stdin");
        input.flush().expect("flushed");
    };

    send(
        json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
            "protocolVersion": "2025-06-18", "capabilities": {},
            "clientInfo": {"name": "t", "version": "1"}
        }}),
    );
    assert!(reply(1)["result"].is_object(), "initialised over stdio");
    send(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
    send(json!({"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                "params": {"name": "check", "arguments": {}}}));
    let called = reply(2);
    assert_eq!(
        called["result"]["content"][0]["text"], "status 1",
        "the tool reached gRPC in-process: {called}"
    );
    assert!(grpc.exists(), "the child serves gRPC on its socket");
    let listeners = tcp_listeners(pid);

    let mut child = child.lock().expect("unpoisoned");
    let _ = child.kill();
    let _ = child.wait();
    assert_eq!(listeners, 0, "the child opened no TCP listener");
}
