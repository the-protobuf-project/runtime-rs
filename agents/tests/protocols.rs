//! The two protocols on one runtime: an agent and a tool server sharing a listener, each
//! under its own base path, with one identity between them.

#![cfg(all(feature = "mcp", feature = "a2a"))]

use a2a::{A2AError, StreamResponse};
use a2a_server::{AgentExecutor, ExecutorContext};
use agents::{Config, Identity, Protocol, Runtime, a2a as a2a_svc, mcp as mcp_svc};
use futures::stream::BoxStream;
use rmcp::ServerHandler;
use tokio_util::sync::CancellationToken;

/// An agent that does nothing, which is enough to prove it mounted and answers.
struct EchoAgent;

#[async_trait::async_trait]
impl AgentExecutor for EchoAgent {
    fn execute(
        &self,
        _ctx: ExecutorContext,
    ) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
        Box::pin(futures::stream::empty())
    }

    fn cancel(
        &self,
        _ctx: ExecutorContext,
    ) -> BoxStream<'static, Result<StreamResponse, A2AError>> {
        Box::pin(futures::stream::empty())
    }
}

/// A tool server with no tools — the default [`ServerHandler`] is a complete MCP server.
#[derive(Clone, Default)]
struct Tools;

impl ServerHandler for Tools {}

async fn free_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

#[tokio::test]
async fn an_agent_and_a_tool_server_share_one_listener() {
    let port = free_port().await;

    let runtime = Runtime::new(Config {
        identity: Identity {
            name: "hybrid-service".into(),
            description: "serves both protocols".into(),
            version: "2.1.0".into(),
        },
        host: "127.0.0.1".into(),
        port,
        ..Default::default()
    })
    .register(
        a2a_svc::Service::new(
            EchoAgent,
            vec![a2a_svc::skill("echo", "Echo", "Repeats the input")],
        )
        .transports([a2a_svc::Transport::JsonRpc, a2a_svc::Transport::Rest]),
    )
    .register(mcp_svc::Service::new(Tools::default));

    let cancel = CancellationToken::new();
    runtime.start(cancel.clone()).await.expect("start");

    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    // The card is what a client fetches first, and it must carry the runtime's identity
    // rather than anything the agent restated.
    let card: serde_json::Value = client
        .get(format!("{base}{}", a2a_svc::AGENT_CARD_PATH))
        .send()
        .await
        .expect("card request")
        .json()
        .await
        .expect("card json");

    assert_eq!(card["name"], "hybrid-service");
    assert_eq!(card["version"], "2.1.0");
    assert_eq!(card["skills"][0]["id"], "echo");

    // Both transports appear on the card, so a client knows what it can speak.
    let bindings: Vec<String> = card["supportedInterfaces"]
        .as_array()
        .expect("interfaces")
        .iter()
        .map(|i| {
            i["protocolBinding"]
                .as_str()
                .unwrap_or_default()
                .to_string()
        })
        .collect();
    assert!(bindings.contains(&"JSONRPC".to_string()), "{bindings:?}");
    assert!(bindings.contains(&"HTTP+JSON".to_string()), "{bindings:?}");

    // JSON-RPC answers at the base path. A malformed version is rejected by the protocol
    // rather than 404'd by the router, which is what proves it mounted.
    let rpc = client
        .post(format!("{base}/a2a"))
        .json(&serde_json::json!({"jsonrpc": "1.0", "id": 1, "method": "message/send"}))
        .send()
        .await
        .expect("jsonrpc request");
    assert_ne!(
        rpc.status(),
        reqwest::StatusCode::NOT_FOUND,
        "JSON-RPC must be mounted at /a2a"
    );

    // MCP is on the same port under its own base path. Without the negotiation headers the
    // transport refuses the request, which again is the transport answering, not the router.
    let mcp = client
        .get(format!("{base}/mcp"))
        .send()
        .await
        .expect("mcp request");
    assert_ne!(
        mcp.status(),
        reqwest::StatusCode::NOT_FOUND,
        "MCP must be mounted at /mcp"
    );

    let endpoints = runtime.endpoints();
    assert!(
        endpoints.iter().any(|e| e.protocol == Protocol::A2a),
        "{endpoints:?}"
    );
    assert!(
        endpoints.iter().any(|e| e.protocol == Protocol::Mcp),
        "{endpoints:?}"
    );

    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn the_grpc_transport_refuses_rather_than_silently_not_serving() {
    let runtime = Runtime::new(Config {
        identity: Identity {
            name: "grpc-agent".into(),
            ..Default::default()
        },
        host: "127.0.0.1".into(),
        port: free_port().await,
        grpc_routes: Some(agents::GrpcRoutes::new()),
        ..Default::default()
    })
    .register(a2a_svc::Service::new(EchoAgent, vec![]).transports([a2a_svc::Transport::Grpc]));

    let err = runtime.start(CancellationToken::new()).await.unwrap_err();
    let message = err.to_string();
    assert!(message.contains("gRPC transport"), "{message}");
}

#[tokio::test]
async fn transports_parse_from_an_environment_style_list() {
    assert_eq!(
        a2a_svc::parse_transports("jsonrpc, grpc ,nonsense,rest"),
        vec![
            a2a_svc::Transport::JsonRpc,
            a2a_svc::Transport::Grpc,
            a2a_svc::Transport::Rest
        ]
    );
    assert!(a2a_svc::parse_transports("nothing,valid").is_empty());
}
