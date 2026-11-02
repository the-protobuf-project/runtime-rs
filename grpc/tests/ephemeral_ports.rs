//! Port 0 is a free port the operating system chooses, on every listener, and what comes up
//! reports where it actually is.

use std::sync::Arc;

use agents::mcp;
use agents::mcp::rmcp::ServerHandler;
use grpc::options::{Endpoint, McpTransport, Options};
use grpc::{GatewayTarget, HybridServer};
use std::sync::Mutex;

#[derive(Clone)]
struct Empty;
impl ServerHandler for Empty {}

fn all_ephemeral() -> Options {
    let mut options = Options {
        service_name: "ports".into(),
        grpc: Endpoint::new("127.0.0.1", 0),
        http: Endpoint::new("127.0.0.1", 0),
        enable_http: true,
        enable_mcp: true,
        ..Default::default()
    };
    options.mcp.host = "127.0.0.1".into();
    options.mcp.transport = Some(McpTransport::StreamableHttp);
    options
}

#[tokio::test]
async fn zero_is_not_the_default_port() {
    let mut server = HybridServer::new(all_ephemeral());
    server.start().await.expect("the server starts");
    let (grpc, http) = (server.grpc_addr().unwrap(), server.http_addr().unwrap());
    assert_ne!(grpc.port(), 65020, "not gRPC's default");
    assert_ne!(http.port(), 6502, "not HTTP's default");
    assert_ne!(grpc.port(), 0);
    assert_ne!(http.port(), 0);
    server.stop().await.expect("the server stops");
}

#[tokio::test]
async fn two_servers_on_port_zero_do_not_collide() {
    let mut first = HybridServer::new(all_ephemeral());
    let mut second = HybridServer::new(all_ephemeral());
    first.start().await.expect("the first starts");
    second.start().await.expect("the second starts");
    assert_ne!(first.grpc_addr(), second.grpc_addr());
    first.stop().await.expect("stops");
    second.stop().await.expect("stops");
}

#[tokio::test]
async fn mcp_on_port_zero_reports_the_address_that_answers() {
    let mut server = HybridServer::new(all_ephemeral())
        .with_mcp_service(|_| mcp::Service::from_handler(Empty).base_path("/tools/mcp"));
    server.start().await.expect("the server starts");

    let endpoints = server.agent_endpoints();
    let url = &endpoints.first().expect("an MCP endpoint").url;
    assert!(!url.contains(":0/"), "port 0 cannot be dialled: {url}");
    let answered = reqwest::Client::new()
        .post(url)
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await
        .expect("the reported address is listening");
    assert_ne!(answered.status(), reqwest::StatusCode::NOT_FOUND, "{url}");
    server.stop().await.expect("the server stops");
}

#[tokio::test]
async fn a_dialling_gateway_is_given_the_bound_port() {
    let seen = Arc::new(Mutex::new(None::<String>));
    let recorder = Arc::clone(&seen);
    let mut server = HybridServer::new(all_ephemeral()).with_http_gateways(Arc::new(
        move |router, target: &GatewayTarget| {
            *recorder.lock().unwrap() = Some(target.endpoint.clone());
            Ok(router)
        },
    ));
    server.start().await.expect("the server starts");
    let bound = server.grpc_addr().expect("gRPC bound").to_string();
    assert_eq!(seen.lock().unwrap().as_deref(), Some(bound.as_str()));
    server.stop().await.expect("the server stops");
}

#[tokio::test]
async fn mcp_still_follows_a_fixed_http_port() {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
        .expect("a free port");
    let mut options = all_ephemeral();
    options.http.port = port;
    let mut server = HybridServer::new(options);
    server.start().await.expect("the server starts");
    assert_eq!(server.options().mcp.port, port + 1);
    server.stop().await.expect("the server stops");
}
