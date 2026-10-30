//! A CORS policy on a running server answers preflights before routing and decorates every
//! HTTP response, `/health` included; an invalid one stops the server from starting.

use std::net::TcpListener;
use std::time::Duration;

use grpc::options::{Endpoint, Options};
use grpc::{CorsPolicy, Error, HybridServer};

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
        .expect("a free port")
}

fn server(http_port: u16, policy: CorsPolicy) -> HybridServer {
    HybridServer::new(Options {
        service_name: "cors-test".into(),
        grpc: Endpoint::new("127.0.0.1", free_port()),
        http: Endpoint::new("127.0.0.1", http_port),
        enable_http: true,
        enable_health: true,
        ..Default::default()
    })
    .with_cors(policy)
}

fn policy() -> CorsPolicy {
    CorsPolicy {
        allowed_origins: vec!["https://app.example".into()],
        exposed_headers: vec!["X-Request-Id".into()],
        max_age: Duration::from_secs(60),
        ..Default::default()
    }
}

#[tokio::test]
async fn the_policy_wraps_every_http_response() {
    let http_port = free_port();
    let mut server = server(http_port, policy());
    server.start().await.expect("the server starts");
    let base = format!("http://127.0.0.1:{http_port}");
    let client = reqwest::Client::new();

    // A path nothing routes still gets its preflight answered: it never reaches routing.
    let preflight = client
        .request(reqwest::Method::OPTIONS, format!("{base}/v1/anything"))
        .header("Origin", "https://app.example")
        .header("Access-Control-Request-Method", "POST")
        .send()
        .await
        .unwrap();
    assert_eq!(preflight.status(), 204);
    assert_eq!(
        preflight.headers()["access-control-allow-origin"],
        "https://app.example"
    );
    assert_eq!(preflight.headers()["access-control-max-age"], "60");

    let refused = client
        .request(reqwest::Method::OPTIONS, format!("{base}/health"))
        .header("Origin", "https://evil.example")
        .header("Access-Control-Request-Method", "GET")
        .send()
        .await
        .unwrap();
    assert_eq!(refused.status(), 403);
    assert!(
        !refused
            .headers()
            .contains_key("access-control-allow-origin")
    );

    let health = client
        .get(format!("{base}/health"))
        .header("Origin", "https://app.example")
        .send()
        .await
        .unwrap();
    assert_eq!(health.status(), 200);
    assert_eq!(
        health.headers()["access-control-allow-origin"],
        "https://app.example"
    );
    assert_eq!(
        health.headers()["access-control-expose-headers"],
        "X-Request-Id"
    );
    assert_eq!(health.headers()["vary"], "Origin");

    server.stop().await.expect("the server stops");
}

#[tokio::test]
async fn the_policy_also_covers_a_replacement_handler() {
    let http_port = free_port();
    let mut server = server(http_port, policy()).with_http_handler(
        axum::Router::new().route("/mine", axum::routing::get(|| async { "mine" })),
    );
    server.start().await.expect("the server starts");

    let response = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{http_port}/mine"))
        .header("Origin", "https://APP.example")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.headers()["access-control-allow-origin"],
        "https://APP.example"
    );
    server.stop().await.expect("the server stops");
}

#[tokio::test]
async fn an_invalid_policy_fails_start() {
    let wildcard_with_credentials = CorsPolicy {
        allowed_origins: vec!["*".into()],
        allow_credentials: true,
        ..Default::default()
    };
    for invalid in [CorsPolicy::default(), wildcard_with_credentials] {
        let mut server = server(free_port(), invalid);
        let err = server.start().await.expect_err("an invalid policy");
        assert!(matches!(err, Error::InvalidCors(_)), "{err}");
    }
}
