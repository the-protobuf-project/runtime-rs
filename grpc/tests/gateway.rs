//! A REST request through a `transcode` gateway reaches a real tonic service in-process,
//! through the same interceptor chain a gRPC call runs.
//!
//! The service is the built-in `grpc.health.v1.Health`, so the test needs no generated
//! code of its own: the route table below is written in the form `protoc-gen-http` emits.

use std::net::TcpListener;

use grpc::HybridServer;
use grpc::options::{Endpoint, Options};
use serde::Serialize;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_client::HealthClient;
use transcode::codec::{CodecEntry, CodecRegistry, Framing, JsonCodec};
use transcode::error::Result;
use transcode::gateway::{Call, Dispatcher, Gateway, Reply};
use transcode::middleware::MethodPattern;
use transcode::route::{Capture, Match, MethodInfo, Route, RouteTable};

const DOMAIN: &str = "health.example.com";

static V1_HEALTH: &[Match] = &[Match::Literal("v1"), Match::Literal("health")];
static NONE: &[Capture] = &[];
static ROUTES: &[Route] = &[Route {
    method: "GET",
    segments: V1_HEALTH,
    verb: "",
    captures: NONE,
    template: "/v1/health",
    handler: 0,
}];
static METHODS: &[MethodInfo] = &[MethodInfo {
    name: "Check",
    full_name: "grpc.health.v1.Health.Check",
    service: "grpc.health.v1.Health",
    pattern: MethodPattern::Get,
    mutating: false,
    server_stream: false,
}];
static CODECS: &[CodecEntry] = &[CodecEntry {
    name: "json",
    media_types: &["application/json"],
    framing: Framing::JsonArray,
    index: 0,
}];

#[derive(Serialize)]
struct Status {
    status: String,
}

/// Calls the health service over the loopback, as generated dispatch would.
struct Health {
    client: HealthClient<grpc::Loopback>,
}

impl Dispatcher for Health {
    async fn dispatch(&self, call: &Call<'_>) -> Result<Reply> {
        let request = call.request(HealthCheckRequest {
            service: String::new(),
        });
        let response = call.rpc(self.client.clone().check(request).await)?;
        let status = tonic_health::pb::health_check_response::ServingStatus::try_from(
            response.into_inner().status,
        )
        .map(|s| s.as_str_name().to_string())
        .unwrap_or_default();
        call.reply_with(&JsonCodec::new(), http::StatusCode::OK, &Status { status })
    }
}

/// A port nothing is listening on, found by binding and releasing one.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
        .expect("a free port")
}

/// Rejects any call carrying `x-deny`, to prove the chain runs on the loopback.
fn deny(request: tonic::Request<()>) -> std::result::Result<tonic::Request<()>, tonic::Status> {
    if request.metadata().contains_key("x-deny") {
        return Err(tonic::Status::permission_denied(
            "Denied by the interceptor.",
        ));
    }
    Ok(request)
}

#[tokio::test]
async fn rest_reaches_grpc_through_the_loopback_and_its_interceptors() {
    let (grpc_port, http_port) = (free_port(), free_port());
    let mut server = HybridServer::new(Options {
        service_name: "health-test".into(),
        grpc: Endpoint::new("127.0.0.1", grpc_port),
        http: Endpoint::new("127.0.0.1", http_port),
        enable_http: true,
        enable_health: true,
        ..Default::default()
    })
    .with_unary_interceptor(std::sync::Arc::new(deny))
    .with_gateway(|loopback| {
        Gateway::new(
            RouteTable::with_methods(ROUTES, METHODS),
            CodecRegistry::new(CODECS),
            Health {
                client: HealthClient::new(loopback),
            },
            DOMAIN,
        )
    });
    server.start().await.expect("the server starts");

    let base = format!("http://127.0.0.1:{http_port}");
    let client = reqwest::Client::new();

    let ok = client
        .get(format!("{base}/v1/health"))
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 200);
    let body: serde_json::Value = ok.json().await.unwrap();
    assert_eq!(
        body["status"], "SERVING",
        "the health service answered: {body}"
    );

    // The header becomes metadata, the loopback runs the chain, and the rejection comes
    // back as the AIP-193 envelope with the status the interceptor chose.
    let denied = client
        .get(format!("{base}/v1/health"))
        .header("grpc-metadata-x-deny", "1")
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 403);
    let body: serde_json::Value = denied.json().await.unwrap();
    assert_eq!(body["error"]["status"], "PERMISSION_DENIED");

    // A path no gateway claims still gets the gateway's envelope, not a bare 404.
    let missing = client
        .get(format!("{base}/v1/nothing"))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
    let body: serde_json::Value = missing.json().await.unwrap();
    assert_eq!(body["error"]["status"], "NOT_FOUND");

    // The server's own health route sits beside the gateway.
    let health = client.get(format!("{base}/health")).send().await.unwrap();
    assert_eq!(health.status(), 200);

    server.stop().await.expect("the server stops");
    assert!(
        client.get(format!("{base}/health")).send().await.is_err(),
        "the listener is closed after stop"
    );
}
