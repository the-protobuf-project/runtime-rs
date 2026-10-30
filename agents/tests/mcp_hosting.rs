//! What the runtime does around a hosted handler, seen from a client over HTTP: extras,
//! interceptors, cache hints, resource overrides, header forwarding, statelessness, health,
//! SSE, combined endpoints, and both kinds of elicitation.

#![cfg(feature = "mcp")]

mod common;
mod mcp_client;
mod mcp_probe;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use agents::mcp::{
    self, CacheHint, CacheHints, ExtraProperty, HealthState, Interceptors, ServiceGroup,
    ToolHandler, ToolInterceptor, Transport,
};
use agents::shared::HeaderMapping;
use agents::{Protocol, Runtime};
use common::{config, free_port};
use mcp_client::{McpClient, tool_text};
use mcp_probe::Probe;
use rmcp::ErrorData as McpError;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

/// Counts calls, records the method it saw, and refuses any call asking it to.
struct Gate {
    calls: Arc<AtomicUsize>,
    method: Arc<std::sync::Mutex<String>>,
}

impl ToolInterceptor for Gate {
    fn intercept<'a>(
        &'a self,
        method: &'a str,
        arguments: Value,
        next: ToolHandler<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<Value, McpError>> + Send + 'a>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.method.lock().unwrap() = method.to_string();
            if arguments["word"] == "deny" {
                return Err(McpError::invalid_params("denied by interceptor", None));
            }
            next(arguments).await
        })
    }
}

/// Starts `runtime` and returns its cancel token.
async fn start(runtime: &Runtime) -> CancellationToken {
    let cancel = CancellationToken::new();
    runtime.start(cancel.clone()).await.expect("start");
    cancel
}

#[tokio::test]
async fn extras_interceptors_and_cache_hints_are_applied_around_the_handler() {
    let port = free_port().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let method = Arc::new(std::sync::Mutex::new(String::new()));
    let mut hints = CacheHints {
        list: Some(CacheHint::public(60_000)),
        ..Default::default()
    };
    hints
        .resources
        .insert("probe://probe".into(), CacheHint::private(5_000));
    let service = mcp::Service::from_handler(Probe::new("probe"))
        .extras([ExtraProperty::new("api_key", "the key")
            .required()
            .with_context_key("key")])
        .interceptors(Interceptors::new().with(Arc::new(Gate {
            calls: Arc::clone(&calls),
            method: Arc::clone(&method),
        })))
        .tool_method("probe", "/probe.v1.ProbeService/Echo")
        .cache_hints(hints);
    let runtime = Runtime::new(config(port)).register(service);
    let cancel = start(&runtime).await;

    let mut client = McpClient::new(format!("http://127.0.0.1:{port}/mcp"));
    client.initialize().await;

    let tools = client.call("tools/list", json!({})).await;
    let echo = &tools["result"]["tools"][0];
    assert_eq!(
        echo["inputSchema"]["properties"]["api_key"]["type"], "string",
        "{tools}"
    );
    assert_eq!(echo["inputSchema"]["required"], json!(["api_key"]));
    assert_eq!(tools["result"]["ttlMs"], 60_000, "{tools}");

    let called = client
        .call(
            "tools/call",
            json!({"name": "probe", "arguments": {"word": "hi", "api_key": "s3cret"}}),
        )
        .await;
    let seen = tool_text(&called);
    assert_eq!(
        seen["args"],
        json!({"word": "hi"}),
        "the extra is lifted out: {called}"
    );
    assert_eq!(seen["extras"], json!({"key": "s3cret"}));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(*method.lock().unwrap(), "/probe.v1.ProbeService/Echo");

    let denied = client
        .call(
            "tools/call",
            json!({"name": "probe", "arguments": {"word": "deny"}}),
        )
        .await;
    assert!(
        denied["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("denied by interceptor"),
        "{denied}"
    );

    let read = client
        .call("resources/read", json!({"uri": "probe://probe"}))
        .await;
    assert_eq!(read["result"]["ttlMs"], 5_000, "{read}");
    assert_eq!(read["result"]["cacheScope"], "private", "{read}");

    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_resource_handler_overrides_the_placeholder() {
    let port = free_port().await;
    let service = mcp::Service::from_handler(Probe::new("probe")).resource_handler(
        "probe://probe",
        Arc::new(|request, _| {
            Box::pin(async move {
                Ok(rmcp::model::ReadResourceResult::new(vec![
                    rmcp::model::ResourceContents::text("overridden", request.uri),
                ]))
            })
        }),
    );
    let runtime = Runtime::new(config(port)).register(service);
    let cancel = start(&runtime).await;

    let mut client = McpClient::new(format!("http://127.0.0.1:{port}/mcp"));
    client.initialize().await;
    let read = client
        .call("resources/read", json!({"uri": "probe://probe"}))
        .await;
    assert_eq!(
        read["result"]["contents"][0]["text"], "overridden",
        "{read}"
    );

    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn mapped_headers_reach_the_call_as_incoming_metadata() {
    let port = free_port().await;
    let hook: mcp::ElicitHook = Arc::new(|_, fields| Box::pin(async move { Ok(fields) }));
    let service = mcp::Service::from_handler(Probe::new("probe"))
        .headers(agents::shared::default_header_mappings())
        .elicit_hook(hook);
    let runtime = Runtime::new(config(port)).register(service);
    let cancel = start(&runtime).await;

    let mut client = McpClient::new(format!("http://127.0.0.1:{port}/mcp"))
        .header("Authorization", "Bearer abc");
    client.initialize().await;
    let called = client
        .call("tools/call", json!({"name": "probe", "arguments": {}}))
        .await;
    let seen = tool_text(&called);
    assert_eq!(seen["auth"], "Bearer abc", "{called}");
    assert_eq!(seen["hook"], true, "the elicit hook rides on the call");

    // A second caller on its own session sees only its own headers.
    let mut other = McpClient::new(format!("http://127.0.0.1:{port}/mcp"));
    other.initialize().await;
    let called = other
        .call("tools/call", json!({"name": "probe", "arguments": {}}))
        .await;
    assert_eq!(tool_text(&called)["auth"], Value::Null, "{called}");

    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_stateless_server_issues_no_session() {
    let port = free_port().await;
    let runtime = Runtime::new(config(port))
        .register(mcp::Service::from_handler(Probe::new("probe")).stateless());
    let cancel = start(&runtime).await;

    let mut client = McpClient::new(format!("http://127.0.0.1:{port}/mcp"));
    let info = client.initialize().await;
    assert!(info["result"].is_object(), "{info}");
    assert_eq!(
        client.session(),
        None,
        "no Mcp-Session-Id in stateless mode"
    );

    // The standalone GET stream needs a session, so it is refused.
    let get = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{port}/mcp"))
        .header("accept", "text/event-stream")
        .send()
        .await
        .expect("get");
    assert_eq!(get.status(), reqwest::StatusCode::METHOD_NOT_ALLOWED);

    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn the_health_route_answers_in_ping_shape() {
    let port = free_port().await;
    let healthy = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let flag = Arc::clone(&healthy);
    let check: mcp::HealthCheck = Arc::new(move || {
        let serving = flag.load(Ordering::SeqCst);
        Box::pin(async move {
            if serving {
                HealthState::Serving
            } else {
                HealthState::NotServing
            }
        })
    });
    let runtime = Runtime::new(config(port))
        .register(mcp::Service::from_handler(Probe::new("probe")).health_check("health", check));
    let cancel = start(&runtime).await;
    let http = reqwest::Client::new();
    let url = format!("http://127.0.0.1:{port}/health");

    let ok = http.get(&url).send().await.expect("get");
    assert_eq!(ok.status(), reqwest::StatusCode::OK);
    assert_eq!(
        ok.json::<Value>().await.unwrap(),
        json!({"jsonrpc": "2.0", "id": "health", "result": {}})
    );

    healthy.store(false, Ordering::SeqCst);
    let down = http.get(&url).send().await.expect("get");
    assert_eq!(down.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let body = down.json::<Value>().await.unwrap();
    assert_eq!(body["error"]["code"], -32000);
    assert_eq!(body["error"]["message"], "not serving");

    let post = http.post(&url).send().await.expect("post");
    assert_eq!(post.status(), reqwest::StatusCode::METHOD_NOT_ALLOWED);

    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_group_mounts_each_service_at_its_own_path() {
    let port = free_port().await;
    let group = ServiceGroup::new()
        .with(mcp::Service::from_handler(Probe::new("alpha")).base_path("/alpha/mcp"))
        .with(mcp::Service::from_handler(Probe::new("beta")).base_path("/beta/mcp"));
    let runtime = Runtime::new(config(port)).register(group);
    let cancel = start(&runtime).await;

    let endpoints = runtime.endpoints();
    assert_eq!(endpoints.len(), 2, "{endpoints:?}");
    assert!(endpoints.iter().all(|e| e.protocol == Protocol::Mcp));

    for name in ["alpha", "beta"] {
        let mut client = McpClient::new(format!("http://127.0.0.1:{port}/{name}/mcp"));
        let info = client.initialize().await;
        assert_eq!(info["result"]["serverInfo"]["name"], name);
    }

    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_combined_group_serves_every_service_at_one_path() {
    let port = free_port().await;
    let group = ServiceGroup::new()
        .with(mcp::Service::from_handler(Probe::new("alpha")))
        .with(mcp::Service::from_handler(Probe::new("beta")))
        .headers([HeaderMapping::new("authorization", "authorization")])
        .combined();
    let runtime = Runtime::new(config(port)).register(group);
    let cancel = start(&runtime).await;
    assert_eq!(runtime.endpoints()[0].detail, "/mcp");

    let mut client = McpClient::new(format!("http://127.0.0.1:{port}/mcp"))
        .header("authorization", "Bearer combined");
    let info = client.initialize().await;
    assert_eq!(
        info["result"]["serverInfo"]["name"], "test-service",
        "the runtime's identity, not one service's: {info}"
    );
    assert_eq!(info["result"]["instructions"], "a service under test");

    let tools = client.call("tools/list", json!({})).await;
    let names: Vec<&str> = tools["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|t| t["name"].as_str())
        .collect();
    assert!(
        names.contains(&"alpha") && names.contains(&"beta"),
        "{names:?}"
    );

    let called = client
        .call("tools/call", json!({"name": "beta", "arguments": {}}))
        .await;
    let seen = tool_text(&called);
    assert_eq!(seen["service"], "beta", "routed to its own service");
    assert_eq!(seen["auth"], "Bearer combined");

    let resources = client.call("resources/list", json!({})).await;
    assert_eq!(
        resources["result"]["resources"].as_array().unwrap().len(),
        2
    );

    // Completions: the first service with an answer wins.
    let complete = client
        .call(
            "completion/complete",
            json!({
                "ref": {"type": "ref/prompt", "name": "beta_prompt"},
                "argument": {"name": "level", "value": "h"},
            }),
        )
        .await;
    assert_eq!(
        complete["result"]["completion"]["values"],
        json!(["High"]),
        "{complete}"
    );
    let prompt = client
        .call("prompts/get", json!({"name": "alpha_prompt"}))
        .await;
    assert_eq!(prompt["result"]["description"], "alpha", "{prompt}");

    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_combined_group_with_clashing_names_fails_the_start() {
    let port = free_port().await;
    let group = ServiceGroup::new()
        .with(mcp::Service::from_handler(Probe::new("same")))
        .with(mcp::Service::from_handler(Probe::new("same")))
        .combined();
    let runtime = Runtime::new(config(port)).register(group);
    let err = runtime
        .start(CancellationToken::new())
        .await
        .expect_err("a clash must fail the start");
    let message = err.to_string();
    assert!(
        message.contains(r#"tool "same" is declared by both service #0 and service #1"#),
        "{message}"
    );
    assert!(message.contains(r#"resource "probe://same""#), "{message}");
}

#[tokio::test]
async fn an_ask_without_elicitation_support_becomes_a_tool_error() {
    let port = free_port().await;
    let runtime =
        Runtime::new(config(port)).register(mcp::Service::from_handler(Probe::new("probe")));
    let cancel = start(&runtime).await;

    let mut client = McpClient::new(format!("http://127.0.0.1:{port}/mcp"));
    client.initialize().await;
    let called = client
        .call("tools/call", json!({"name": "probe_ask", "arguments": {}}))
        .await;
    assert_eq!(called["result"]["isError"], true, "{called}");
    assert_eq!(tool_text(&called), "Book it? (confirm)");

    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_legacy_client_answers_an_ask_through_an_elicitation_round_trip() {
    let port = free_port().await;
    let runtime =
        Runtime::new(config(port)).register(mcp::Service::from_handler(Probe::new("probe")));
    let cancel = start(&runtime).await;

    let mut client = McpClient::new(format!("http://127.0.0.1:{port}/mcp"));
    client.initialize_with(json!({"elicitation": {}})).await;

    let booked = client
        .call_answering(
            "tools/call",
            json!({"name": "probe_ask", "arguments": {"word": "room"}}),
            json!({"action": "accept", "content": {"confirm": true}}),
        )
        .await;
    let text = tool_text(&booked);
    assert!(
        text.as_str().unwrap_or_default().starts_with("booked"),
        "{booked}"
    );
    assert!(
        text.as_str().unwrap().contains("\"confirm\":true"),
        "{booked}"
    );

    let declined = client
        .call_answering(
            "tools/call",
            json!({"name": "probe_confirm", "arguments": {}}),
            json!({"action": "decline"}),
        )
        .await;
    assert!(
        tool_text(&declined)
            .as_str()
            .unwrap_or_default()
            .starts_with("Decline"),
        "{declined}"
    );

    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn the_legacy_sse_transport_serves_a_session() {
    use futures::StreamExt;

    let port = free_port().await;
    let runtime = Runtime::new(config(port))
        .register(mcp::Service::from_handler(Probe::new("probe")).transports([Transport::Sse]));
    let cancel = start(&runtime).await;
    assert_eq!(runtime.endpoints()[0].transport, "sse");

    let http = reqwest::Client::new();
    let stream = http
        .get(format!("http://127.0.0.1:{port}/mcp/"))
        .header("accept", "text/event-stream")
        .send()
        .await
        .expect("open");
    assert_eq!(stream.status(), reqwest::StatusCode::OK);
    let mut events = stream.bytes_stream();
    let mut buffer = String::new();

    // Reads until an event whose data satisfies `wanted`.
    async fn next_data(
        events: &mut (impl futures::Stream<Item = reqwest::Result<bytes::Bytes>> + Unpin),
        buffer: &mut String,
        event: &str,
    ) -> String {
        loop {
            while let Some(end) = buffer.find("\n\n") {
                let block: String = buffer.drain(..end + 2).collect();
                if block.contains(&format!("event: {event}"))
                    && let Some(data) = block.lines().find_map(|l| l.strip_prefix("data: "))
                {
                    return data.to_string();
                }
            }
            let chunk = events.next().await.expect("stream open").expect("chunk");
            buffer.push_str(&String::from_utf8_lossy(&chunk));
        }
    }

    let endpoint = next_data(&mut events, &mut buffer, "endpoint").await;
    assert!(endpoint.starts_with("/mcp/?sessionId="), "{endpoint}");
    let post = format!("http://127.0.0.1:{port}{endpoint}");
    let accepted = http
        .post(&post)
        .json(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "sse-test", "version": "1"}
            }
        }))
        .send()
        .await
        .expect("post");
    assert_eq!(accepted.status(), reqwest::StatusCode::ACCEPTED);
    let message: Value =
        serde_json::from_str(&next_data(&mut events, &mut buffer, "message").await).unwrap();
    assert_eq!(
        message["result"]["serverInfo"]["name"], "probe",
        "{message}"
    );

    let unknown = http
        .post(format!("http://127.0.0.1:{port}/mcp/?sessionId=nope"))
        .json(&json!({"jsonrpc": "2.0", "method": "notifications/initialized"}))
        .send()
        .await
        .expect("post");
    assert_eq!(unknown.status(), reqwest::StatusCode::NOT_FOUND);

    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn stdio_is_reported_with_no_url_and_no_detail() {
    let port = free_port().await;
    // Stdio alongside HTTP, so the test never touches the process's own pipes beyond a
    // listener that is cancelled straight away.
    let service = mcp::Service::from_handler(Probe::new("probe"))
        .transports([Transport::StreamableHttp, Transport::Stdio]);
    let runtime = Runtime::new(config(port)).register(service);
    let cancel = start(&runtime).await;
    let stdio = runtime
        .endpoints()
        .into_iter()
        .find(|e| e.transport == "stdio")
        .expect("a stdio endpoint");
    assert_eq!(stdio.url, "");
    assert_eq!(stdio.detail, "");
    cancel.cancel();
    let _ = runtime.shutdown().await;
}

#[tokio::test]
async fn start_server_serves_one_server_until_cancelled() {
    let port = free_port().await;
    let config = mcp::ServerConfig {
        name: "standalone".into(),
        addr: format!("127.0.0.1:{port}"),
        generated_base_path: "/gen/mcp".into(),
        ..Default::default()
    };
    let cancel = CancellationToken::new();
    let serving = {
        let cancel = cancel.clone();
        tokio::spawn(
            async move { mcp::start_server(cancel, &config, || Probe::new("probe")).await },
        )
    };

    // The server binds in the background; a refused connection panics the probe task, which
    // is simply retried.
    let url = format!("http://127.0.0.1:{port}/gen/mcp");
    let mut info = Value::Null;
    for _ in 0..50 {
        let mut client = McpClient::new(&url);
        if let Ok(answer) = tokio::spawn(async move { client.initialize().await }).await {
            info = answer;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(info["result"]["serverInfo"]["name"], "probe", "{info}");

    cancel.cancel();
    serving.await.expect("join").expect("a clean drain");
}
