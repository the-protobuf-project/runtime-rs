//! The pure pieces ported from Go, each held to what the Go function returns: transports,
//! endpoints, base paths, asks, elicitation, progress, metadata, streams, invoke, and the
//! inventory a combined server is checked with.

#![cfg(feature = "mcp")]

mod mcp_probe;

use std::sync::Arc;

use agents::mcp::{
    self, ElicitField, Elicitation, InProcessServerStream, Interceptors, ProgressMessage,
    ServerConfig, Transport,
};
use agents::shared::{ForwardedHeaders, HeaderMapping, forward_metadata, incoming_headers};
use mcp_probe::Probe;
use rmcp::model::{CallToolRequestParams, NumberOrString, ProgressToken};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

#[test]
fn transports_parse_exactly_as_go_names_them() {
    assert_eq!(
        mcp::parse_transports(" stdio , streamable-http,,sse ").unwrap(),
        vec![Transport::Stdio, Transport::StreamableHttp, Transport::Sse]
    );
    assert!(mcp::parse_transports("").unwrap().is_empty());
    // No aliases and no case folding: Go refuses these, so this does too.
    assert!("http".parse::<Transport>().is_err());
    assert!("SSE".parse::<Transport>().is_err());
}

#[test]
fn the_endpoint_follows_go_server_endpoint() {
    let none = |_: &str| None;
    let config = ServerConfig::default();
    let endpoint = mcp::server_endpoint_with(&config, none);
    assert_eq!(endpoint.url, "http://localhost:8080/mcp");
    assert_eq!(endpoint.transport, "streamable-http");

    let config = ServerConfig {
        addr: "10.0.0.1:9000".into(),
        base_path: "/custom".into(),
        generated_base_path: "/gen/mcp".into(),
        transports: vec![Transport::Stdio, Transport::Sse],
        ..Default::default()
    };
    let endpoint = mcp::server_endpoint_with(&config, none);
    assert_eq!(endpoint.url, "http://10.0.0.1:9000/gen/mcp");
    assert_eq!(endpoint.transport, "sse");

    let env = |name: &str| match name {
        "MCP_SERVER_HOST" => Some("public.example".to_string()),
        "MCP_SERVER_PORT" => Some("443".to_string()),
        "MCP_SERVER_TLS" => Some("true".to_string()),
        _ => None,
    };
    let endpoint = mcp::server_endpoint_with(&ServerConfig::default(), env);
    assert_eq!(endpoint.url, "https://public.example:443/mcp");
    assert_eq!(endpoint.protocol, "https");

    let stdio = ServerConfig {
        transport: Some(Transport::Stdio),
        ..Default::default()
    };
    let endpoint = mcp::server_endpoint_with(&stdio, none);
    assert_eq!(
        (endpoint.protocol.as_str(), endpoint.url.as_str()),
        ("stdio", "")
    );
}

#[test]
fn base_paths_resolve_as_go_resolves_them() {
    let empty = ServerConfig::default();
    assert_eq!(mcp::resolve_base_path(&empty, "/gen"), "/gen");
    let set = ServerConfig {
        base_path: "/mine".into(),
        ..Default::default()
    };
    assert_eq!(mcp::resolve_base_path(&set, "/gen"), "/mine");
    assert_eq!(mcp::prefer_generated_base_path("/gen", &set), "/gen");
}

#[test]
fn an_ask_status_carries_its_question() {
    let fields = vec![ElicitField::new("nights", "How many nights?", "integer").required()];
    let status = mcp::ask(tonic::Code::InvalidArgument, "How long?", &fields);
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    let (message, schema) = mcp::asked(&status).expect("an ask");
    assert_eq!(message, "How long?");
    assert_eq!(schema["properties"]["nights"]["type"], "integer");
    assert_eq!(schema["required"], json!(["nights"]));

    assert!(mcp::asked(&tonic::Status::not_found("x")).is_none());
    let id = mcp::ask_id("How long?");
    assert!(id.starts_with("ask:") && id.len() == 4 + 12, "{id}");
    assert_eq!(
        id,
        mcp::ask_id("How long?"),
        "the same question, the same id"
    );
}

#[test]
fn answers_to_an_ask_are_merged_and_a_decline_stops_the_call() {
    let id = mcp::ask_id("Book it?");
    let mut request = CallToolRequestParams::new("book");
    request.input_responses = Some(
        [(
            id.clone(),
            json!({"action": "accept", "content": {"confirm": true}}),
        )]
        .into_iter()
        .collect(),
    );
    let (merged, declined) = mcp::merge_answers(&request, json!({"room": "deluxe"}));
    assert!(!declined);
    assert_eq!(merged, json!({"room": "deluxe", "confirm": true}));

    request.input_responses = Some([(id, json!({"action": "cancel"}))].into_iter().collect());
    let (_, declined) = mcp::merge_answers(&request, json!({}));
    assert!(declined);
}

#[test]
fn run_elicitation_asks_first_and_reads_the_answer_on_the_retry() {
    let fields = vec![ElicitField::new("confirm", "Sure?", "boolean").required()];
    let mut request = CallToolRequestParams::new("delete");
    let Ok(Elicitation::Pending(pending)) = mcp::run_elicitation(&request, "Sure?", &fields) else {
        panic!("the first call asks");
    };
    let requests = pending.input_requests.expect("input requests");
    assert!(requests.contains_key(mcp::ELICIT_REQUEST_ID));

    request.input_responses = Some(
        [(
            mcp::ELICIT_REQUEST_ID.to_string(),
            json!({"action": "accept", "content": {"confirm": true}}),
        )]
        .into_iter()
        .collect(),
    );
    let Ok(Elicitation::Answered(answer)) = mcp::run_elicitation(&request, "Sure?", &fields) else {
        panic!("the retry is answered");
    };
    assert_eq!(answer.content, Some(json!({"confirm": true})));
}

#[test]
fn only_missing_fields_are_asked_for() {
    let fields = vec![
        ElicitField::new("a", "", "string"),
        ElicitField::new("b", "", "string"),
        ElicitField::new("c", "", "string"),
        ElicitField::new("d", "", "boolean"),
    ];
    let missing =
        mcp::missing_elicit_fields(&json!({"a": "x", "b": "", "c": null, "d": false}), &fields);
    let names: Vec<&str> = missing.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, vec!["b", "c"]);
}

struct Chunk {
    token: &'static str,
}

impl ProgressMessage for Chunk {
    fn progress(&self) -> f64 {
        2.0
    }
    fn message(&self) -> &str {
        "halfway"
    }
    fn total(&self) -> f64 {
        4.0
    }
    fn token_string(&self) -> &str {
        self.token
    }
    fn meta(&self) -> serde_json::Map<String, Value> {
        [("step".to_string(), json!(2))].into_iter().collect()
    }
}

#[test]
fn a_chunk_token_wins_over_the_request_token() {
    let fallback = ProgressToken(NumberOrString::Number(7));
    let params = mcp::progress_params(fallback.clone(), &Chunk { token: "mine" });
    assert_eq!(
        params.progress_token,
        ProgressToken(NumberOrString::String("mine".into()))
    );
    assert_eq!(params.total, Some(4.0));
    assert_eq!(params.message.as_deref(), Some("halfway"));
    assert_eq!(params.meta.expect("meta").0.0["step"], 2);

    let params = mcp::progress_params(fallback.clone(), &Chunk { token: "" });
    assert_eq!(params.progress_token, fallback);
}

#[test]
fn the_progress_token_rides_in_metadata() {
    let token = ProgressToken(NumberOrString::Number(42));
    let mut metadata = tonic::metadata::MetadataMap::new();
    mcp::with_progress_token(&mut metadata, Some(&token));
    assert_eq!(metadata.get(mcp::GRPC_PROGRESS_TOKEN_KEY).unwrap(), "42");
    mcp::with_progress_token(&mut metadata, None);
    assert_eq!(metadata.get("mcp-progress-token").unwrap(), "42");

    let incoming = mcp::with_incoming_progress_token(Some(&token)).expect("metadata");
    assert_eq!(incoming.len(), 1);
    assert!(mcp::with_incoming_progress_token(None).is_none());
}

#[test]
fn mapped_headers_replace_stale_incoming_metadata() {
    let mappings = vec![
        HeaderMapping::new("Authorization", "authorization"),
        HeaderMapping::new("X-Bad", "grpc-timeout"),
    ];
    let mut existing = tonic::metadata::MetadataMap::new();
    existing.insert("authorization", "stale".parse().unwrap());
    existing.insert("keep", "me".parse().unwrap());

    let mut headers = http::HeaderMap::new();
    headers.insert("authorization", "fresh".parse().unwrap());
    headers.insert("x-bad", "1".parse().unwrap());
    let merged = incoming_headers(&existing, &mappings, Some(&headers));
    assert_eq!(merged.get("authorization").unwrap(), "fresh");
    assert_eq!(merged.get("keep").unwrap(), "me");
    assert!(
        merged.get("grpc-timeout").is_none(),
        "grpc- keys are the transport's"
    );

    let cleared = incoming_headers(&existing, &mappings, None);
    assert!(
        cleared.get("authorization").is_none(),
        "nothing stale survives"
    );
}

#[test]
fn forwarded_metadata_drops_grpc_keys_and_prefers_http() {
    let mut incoming = tonic::metadata::MetadataMap::new();
    incoming.insert("grpc-timeout", "1S".parse().unwrap());
    incoming.insert("authorization", "from-grpc".parse().unwrap());
    incoming.insert("x-tenant", "acme".parse().unwrap());
    let forwarded = ForwardedHeaders(
        [("authorization".to_string(), "from-http".to_string())]
            .into_iter()
            .collect(),
    );
    let out = forward_metadata(Some(&incoming), Some(&forwarded)).expect("metadata");
    assert!(out.get("grpc-timeout").is_none());
    assert_eq!(out.get("authorization").unwrap(), "from-http");
    assert_eq!(out.get("x-tenant").unwrap(), "acme");
    assert!(forward_metadata(None, None).is_none());
}

#[tokio::test]
async fn an_in_process_stream_carries_chunks_and_ends() {
    let mut stream = InProcessServerStream::<u32>::new(CancellationToken::new());
    let sender = stream.sender().unwrap();
    stream.close();
    tokio::spawn(async move {
        for i in 0..20 {
            sender.send(i).await.unwrap();
        }
    });
    let mut seen = Vec::new();
    while let Some(i) = stream.recv().await {
        seen.push(i);
    }
    assert_eq!(seen, (0..20).collect::<Vec<_>>());

    let cancel = CancellationToken::new();
    let stream = InProcessServerStream::<u32>::new(cancel.clone());
    stream.send(1).await.expect("room before cancelling");
    cancel.cancel();
    assert_eq!(
        stream.send(99).await.unwrap_err().code(),
        tonic::Code::Cancelled,
        "a cancelled stream refuses"
    );
}

#[tokio::test]
async fn a_pumped_stream_ends_with_its_error() {
    let source = futures::stream::iter(vec![Ok(1u32), Err(tonic::Status::aborted("boom")), Ok(3)]);
    let mut pumped =
        InProcessServerStream::<Result<u32, tonic::Status>>::pump(source, CancellationToken::new());
    assert_eq!(pumped.recv().await.unwrap().unwrap(), 1);
    assert_eq!(
        pumped.recv().await.unwrap().unwrap_err().code(),
        tonic::Code::Aborted
    );
    assert!(pumped.recv().await.is_none(), "nothing after the error");
}

#[tokio::test]
async fn invoke_unary_runs_the_chain_or_goes_direct() {
    let direct = mcp::invoke_unary(
        None,
        "/x.Y/Z",
        json!(1),
        Box::new(|v| Box::pin(async move { Ok(v) })),
    )
    .await
    .unwrap();
    assert_eq!(direct, json!(1));

    let chain = mcp::chain_unary_interceptors([Interceptors::new(), Interceptors::new()]);
    assert!(chain.is_empty());
    let via = mcp::invoke_unary(
        Some(&chain),
        "/x.Y/Z",
        json!(2),
        Box::new(|v| Box::pin(async move { Ok(v) })),
    )
    .await
    .unwrap();
    assert_eq!(via, json!(2));
}

#[tokio::test]
async fn the_inventory_reads_what_a_handler_declares() {
    let inventory = mcp::inventory(mcp::BoxedHandler::new(Probe::new("inv")))
        .await
        .expect("inventory");
    assert_eq!(inventory.tools, vec!["inv", "inv_ask", "inv_confirm"]);
    assert_eq!(inventory.prompts, vec!["inv_prompt"]);
    assert_eq!(inventory.resources, vec!["probe://inv"]);
    assert!(inventory.resource_templates.is_empty());
}

#[tokio::test]
async fn combined_handlers_are_checked_for_clashes() {
    let err = mcp::CombinedServer::from_handlers(
        "c",
        "1",
        "",
        vec![
            mcp::BoxedHandler::new(Probe::new("x")),
            mcp::BoxedHandler::new(Probe::new("x")),
        ],
    )
    .await
    .expect_err("clash");
    assert!(
        matches!(err, mcp::CombineError::Collisions(ref c) if c.len() == 5),
        "{err}"
    );

    let ok = mcp::CombinedServer::from_handlers(
        "c",
        "1",
        "",
        vec![
            mcp::BoxedHandler::new(Probe::new("x")),
            mcp::BoxedHandler::new(Probe::new("y")),
        ],
    )
    .await;
    assert!(ok.is_ok());
}

#[test]
fn a_config_fills_in_resource_handlers() {
    let fallback: mcp::ResourceHandler =
        Arc::new(|r, _| Box::pin(async move { Ok(mcp::default_resource_result(&r.uri)) }));
    let custom: mcp::ResourceHandler =
        Arc::new(|r, _| Box::pin(async move { Ok(mcp::default_resource_result(&r.uri)) }));
    let config = mcp::Config::default().with_resource_handler("a://x", Arc::clone(&custom));
    assert!(Arc::ptr_eq(
        &mcp::resource_handler_for(Some(&config), "a://x", Arc::clone(&fallback)),
        &custom
    ));
    assert!(Arc::ptr_eq(
        &mcp::resource_handler_for(Some(&config), "a://y", Arc::clone(&fallback)),
        &fallback
    ));
    assert!(Arc::ptr_eq(
        &mcp::resource_handler_for(None, "a://x", Arc::clone(&fallback)),
        &fallback
    ));
}
