//! Which `Host` an MCP server answers, end to end over TCP: loopback only by default on
//! Streamable HTTP, a name `allowed_hosts` lists once set, and the same check on SSE.

#![cfg(feature = "mcp")]

mod common;
mod mcp_probe;

use agents::Runtime;
use agents::mcp::{self, Transport};
use common::{config, free_port};
use mcp_probe::Probe;
use tokio_util::sync::CancellationToken;

const INIT: &str = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}"#;

async fn serve(service: mcp::Service<Probe>) -> (Runtime, CancellationToken, u16) {
    let port = free_port().await;
    let runtime = Runtime::new(config(port)).register(service);
    let cancel = CancellationToken::new();
    runtime.start(cancel.clone()).await.expect("start");
    (runtime, cancel, port)
}

async fn post(port: u16, host: &str) -> u16 {
    reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/mcp"))
        .header("host", host)
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .body(INIT)
        .send()
        .await
        .expect("send")
        .status()
        .as_u16()
}

#[tokio::test]
async fn streamable_http_answers_loopback_and_refuses_another_name_by_default() {
    let (runtime, cancel, port) = serve(mcp::Service::from_handler(Probe::new("probe"))).await;
    assert_eq!(post(port, &format!("localhost:{port}")).await, 200);
    assert_eq!(post(port, "evil.example.net").await, 403);
    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_name_allowed_hosts_lists_is_answered() {
    let service = mcp::Service::from_handler(Probe::new("probe"))
        .allowed_hosts(["localhost", "bacnet.example.com"]);
    let (runtime, cancel, port) = serve(service).await;
    assert_eq!(post(port, "bacnet.example.com").await, 200);
    assert_eq!(post(port, "evil.example.net").await, 403);
    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn sse_refuses_a_name_allowed_hosts_does_not_list() {
    let service = mcp::Service::from_handler(Probe::new("probe"))
        .transports([Transport::Sse])
        .allowed_hosts(["localhost"]);
    let (runtime, cancel, port) = serve(service).await;
    let get = |host: &'static str| {
        reqwest::Client::new()
            .get(format!("http://127.0.0.1:{port}/mcp/"))
            .header("host", host)
            .header("accept", "text/event-stream")
            .send()
    };
    assert_eq!(
        get("evil.example.net")
            .await
            .expect("send")
            .status()
            .as_u16(),
        403
    );
    assert_eq!(get("localhost").await.expect("send").status().as_u16(), 200);
    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");
}
