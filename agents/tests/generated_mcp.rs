//! The generated-code contract: a real protoc-gen-mcp handler, hosted by the runtime.
//!
//! `agents::mcp` exists so a generated `FooServiceMcpHandler` can be placed alongside other
//! protocols instead of binding a listener of its own. The fixture beside this file is plugin
//! output verbatim, so if the generated shape ever moves — the factory signature, the
//! `ServerHandler` surface, the proto-derived base path — this test stops compiling, which is
//! the point.

#![cfg(feature = "mcp")]

mod common;
// Generated code, vendored verbatim: it is held to the plugin's output, not to this repo's
// lints, and the helpers this test does not reach are still part of what it must compile.
#[allow(dead_code, unused_imports, clippy::all)]
#[path = "fixtures/counter_service_mcp.rs"]
mod counter;
mod mcp_client;

use std::sync::atomic::{AtomicU64, Ordering};

use agents::{Config, Identity, Protocol, Runtime, mcp};
use common::free_port;
use counter::{
    COUNTER_SERVICE_MCP_DEFAULT_BASE_PATH, CounterServiceMcpHandler, CounterServiceMcpServer,
    McpProgressSink,
};
use mcp_client::McpClient;
use rmcp::ErrorData as McpError;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

/// The implementation a user of the plugin writes by hand. The generated handler wraps it.
#[derive(Default)]
struct Counter {
    count: AtomicU64,
}

#[async_trait::async_trait]
impl CounterServiceMcpServer for Counter {
    async fn count(&self, args: Value, progress: McpProgressSink) -> Result<Value, McpError> {
        let to = args.get("to").and_then(Value::as_u64).unwrap_or(3);
        for i in 1..=to {
            progress.send(i as f64, Some(to as f64), None).await;
        }
        let total = self.count.fetch_add(to, Ordering::SeqCst) + to;
        Ok(json!({ "value": total }))
    }
}

/// A runtime hosting the generated handler at its proto-derived path.
fn runtime_at(port: u16) -> Runtime {
    // The generated handler takes the implementation by value, Arc-wraps it, and is Clone —
    // so it is built once here and cloned per session by `from_handler`.
    let handler = CounterServiceMcpHandler::new(Counter::default());
    Runtime::new(Config {
        identity: Identity {
            name: "counter-service".into(),
            version: "9.9.9".into(),
            ..Default::default()
        },
        host: "127.0.0.1".into(),
        port,
        ..Default::default()
    })
    .register(
        mcp::Service::from_handler(handler)
            // The proto-derived path, so this endpoint matches the Go and C++ servers
            // generated from the same proto rather than drifting a segment away.
            .base_path(COUNTER_SERVICE_MCP_DEFAULT_BASE_PATH),
    )
}

#[tokio::test]
async fn a_generated_handler_answers_a_full_mcp_conversation() {
    let port = free_port().await;
    let runtime = runtime_at(port);
    let cancel = CancellationToken::new();
    runtime.start(cancel.clone()).await.expect("start");

    let mut client = McpClient::new(format!(
        "http://127.0.0.1:{port}{COUNTER_SERVICE_MCP_DEFAULT_BASE_PATH}"
    ));

    let info = client.initialize().await;
    assert_eq!(
        info["result"]["serverInfo"]["name"], "CounterService",
        "identity comes from the proto, not the runtime: {info}"
    );

    // The tools the plugin baked into the handler must reach the wire.
    let tools = client.call("tools/list", json!({})).await;
    let names: Vec<&str> = tools["result"]["tools"]
        .as_array()
        .expect("a tools array")
        .iter()
        .filter_map(|t| t["name"].as_str())
        .collect();
    assert!(
        names.iter().any(|n| n.to_lowercase().contains("count")),
        "generated tool missing from tools/list: {names:?}"
    );

    // And calling one must reach the hand-written implementation behind it.
    let called = client
        .call(
            "tools/call",
            json!({"name": names[0], "arguments": {"to": 4}}),
        )
        .await;
    assert!(
        called["result"].is_object(),
        "tools/call did not reach the implementation: {called}"
    );

    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn the_generated_base_path_is_where_it_mounts() {
    let port = free_port().await;
    let runtime = runtime_at(port);
    let cancel = CancellationToken::new();
    runtime.start(cancel.clone()).await.expect("start");

    let reported = runtime.endpoints();
    let endpoint = reported
        .iter()
        .find(|e| e.protocol == Protocol::Mcp)
        .expect("an MCP endpoint");
    assert!(
        endpoint
            .url
            .ends_with(COUNTER_SERVICE_MCP_DEFAULT_BASE_PATH),
        "reported {} should end with the proto-derived path",
        endpoint.url
    );

    // Nothing answers off the generated path, so a client reading the wrong one finds out.
    let stray = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/mcp"))
        .json(&json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}))
        .send()
        .await
        .expect("stray request");
    assert_eq!(stray.status(), reqwest::StatusCode::NOT_FOUND);

    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");
}
