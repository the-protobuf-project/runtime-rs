//! Agent2Agent on a running HybridServer: the gRPC binding on the server's own gRPC port, and
//! JSON-RPC plus the card on the A2A port, from `with_a2a_agent` and `with_a2a_services`.

use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use agents::a2a::{self, pb};
use grpc::options::{A2aOptions, A2aTransport, Endpoint, Options};
use grpc::{A2aServerConfig, HybridServer};

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
        .expect("a free port")
}

/// Options for a server with A2A on, both transports, and every port pinned.
fn options(grpc_port: u16, a2a_port: u16) -> Options {
    Options {
        service_name: "a2a-hybrid".into(),
        description: "an agent on a hybrid server".into(),
        version: "3.0.0".into(),
        grpc: Endpoint::new("127.0.0.1", grpc_port),
        enable_a2a: true,
        a2a: A2aOptions {
            host: "127.0.0.1".into(),
            port: a2a_port,
            transports: vec![A2aTransport::JsonRpc, A2aTransport::Grpc],
            ..Default::default()
        },
        ..Default::default()
    }
}

fn echo() -> impl a2a::Executor {
    a2a::TextAgent::new(|text: String| async move { Ok(format!("you said: {text}")) })
}

#[tokio::test]
async fn with_a2a_agent_serves_grpc_jsonrpc_and_the_card() {
    let (grpc_port, a2a_port) = (free_port(), free_port());
    let mut server = HybridServer::new(options(grpc_port, a2a_port))
        .with_a2a_agent(echo(), vec![a2a::skill("echo", "Echo", "Repeats")]);
    server.start().await.expect("the server starts");

    // The gRPC binding answers on the server's own gRPC port, beside every other service.
    let mut client =
        pb::a2a_service_client::A2aServiceClient::connect(format!("http://127.0.0.1:{grpc_port}"))
            .await
            .expect("dial the gRPC port");
    let reply = client
        .send_message(pb::SendMessageRequest {
            message: Some(pb::Message {
                message_id: "m-1".into(),
                role: pb::Role::User as i32,
                parts: vec![pb::Part {
                    content: Some(pb::part::Content::Text("over grpc".into())),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .expect("send_message over gRPC")
        .into_inner();
    let Some(pb::send_message_response::Payload::Message(message)) = reply.payload else {
        panic!("a one-shot reply is a message: {reply:?}");
    };
    assert_eq!(
        message.parts[0].content,
        Some(pb::part::Content::Text("you said: over grpc".into()))
    );

    // The card carries the server's identity and both bindings.
    let http = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{a2a_port}");
    let card: serde_json::Value = http
        .get(format!("{base}{}", a2a::AGENT_CARD_PATH))
        .send()
        .await
        .expect("card")
        .json()
        .await
        .expect("card json");
    assert_eq!(card["name"], "a2a-hybrid");
    assert_eq!(card["version"], "3.0.0");
    assert_eq!(card["skills"][0]["id"], "echo");
    let interfaces = card["supportedInterfaces"].as_array().expect("interfaces");
    assert_eq!(interfaces[0]["url"], format!("{base}/a2a"));
    assert_eq!(interfaces[1]["protocolBinding"], "GRPC");
    assert_eq!(interfaces[1]["url"], format!("127.0.0.1:{a2a_port}"));

    // JSON-RPC answers at the configured base path.
    let rpc: serde_json::Value = http
        .post(format!("{base}/a2a"))
        .header("A2A-Version", "1.0")
        .json(&serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "SendMessage",
            "params": {"message": {"messageId": "m-2", "role": "ROLE_USER", "parts": [{"text": "over http"}]}},
        }))
        .send()
        .await
        .expect("jsonrpc")
        .json()
        .await
        .expect("jsonrpc json");
    assert!(rpc.to_string().contains("you said: over http"), "{rpc}");

    // Both bindings are reported for the banner.
    let transports: Vec<String> = server
        .agent_endpoints()
        .into_iter()
        .map(|e| e.transport)
        .collect();
    assert_eq!(transports, ["jsonrpc", "grpc"]);

    server.stop().await.expect("the server stops");
}

#[tokio::test]
async fn with_a2a_services_hands_the_function_a_config_built_from_the_options() {
    let (grpc_port, a2a_port) = (free_port(), free_port());
    let seen: Arc<Mutex<Option<A2aServerConfig>>> = Arc::default();
    let record = Arc::clone(&seen);
    let agent: grpc::A2aAgent = Arc::new(echo());

    let mut opts = options(grpc_port, a2a_port);
    opts.a2a.transports = vec![A2aTransport::JsonRpc];
    opts.a2a.base_path = "/agents/echo".into();
    let mut server = HybridServer::new(opts).with_a2a_services([a2a::service_fn(
        move |cancel, cfg: A2aServerConfig| {
            *record.lock().unwrap() = Some(cfg.clone());
            a2a::start_server(cancel, cfg, Arc::clone(&agent))
        },
    )]);
    server.start().await.expect("the server starts");

    let cfg = seen.lock().unwrap().clone().expect("the function ran");
    assert_eq!(cfg.name, "a2a-hybrid");
    assert_eq!(cfg.description, "an agent on a hybrid server");
    assert_eq!(cfg.addr, format!("127.0.0.1:{a2a_port}"));
    assert_eq!(cfg.base_path, "/agents/echo");
    assert_eq!(cfg.transports, [a2a::Transport::JsonRpc]);
    assert!(cfg.grpc_routes.is_some(), "the server's own gRPC routes");

    let status = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{a2a_port}/agents/echo"))
        .header("A2A-Version", "1.0")
        .json(&serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "SendMessage",
            "params": {"message": {"messageId": "m", "role": "ROLE_USER", "parts": [{"text": "x"}]}},
        }))
        .send()
        .await
        .expect("jsonrpc")
        .status();
    assert!(status.is_success(), "{status}");

    server.stop().await.expect("the server stops");
}

#[tokio::test]
async fn a2a_services_are_not_served_when_a2a_is_disabled() {
    let (grpc_port, a2a_port) = (free_port(), free_port());
    let mut opts = options(grpc_port, a2a_port);
    opts.enable_a2a = false;
    let mut server = HybridServer::new(opts).with_a2a_agent(echo(), vec![]);
    server.start().await.expect("the server starts");

    assert!(server.agent_endpoints().is_empty());
    assert!(
        reqwest::get(format!(
            "http://127.0.0.1:{a2a_port}{}",
            a2a::AGENT_CARD_PATH
        ))
        .await
        .is_err(),
        "nothing listens on the A2A port"
    );

    server.stop().await.expect("the server stops");
}
