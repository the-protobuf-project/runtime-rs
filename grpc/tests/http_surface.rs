//! The HTTP surface: what it answers, and that one router serves every transport.

use axum::Router;
use axum::routing::get;
use grpc::http::{Surface, h1};
use tokio_util::sync::CancellationToken;

/// Stands in for a generated transcode router: absolute paths from `google.api.http`, mounted
/// by merging so they land exactly where the API declares them.
fn generated_routes() -> Router {
    Router::new()
        .route("/v1/artists", get(|| async { "artists" }))
        .route("/v1/artists/{id}", get(|| async { "artist" }))
}

async fn free_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().expect("addr").port();
    drop(listener);
    port
}

#[tokio::test]
async fn a_generated_router_keeps_the_paths_the_api_declares() {
    let port = free_port().await;
    let surface = Surface::new().mount(generated_routes());
    let cancel = CancellationToken::new();

    let task = h1::serve(
        surface.router(),
        &format!("127.0.0.1:{port}"),
        None,
        cancel.clone(),
    )
    .await
    .expect("serve");

    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    // The generated routes are reachable at their declared paths, unprefixed.
    let listed = client
        .get(format!("{base}/v1/artists"))
        .send()
        .await
        .expect("request");
    assert_eq!(listed.status(), 200);
    assert_eq!(listed.text().await.unwrap(), "artists");

    // And the surface's own health endpoint still answers alongside them.
    let health = client
        .get(format!("{base}/health"))
        .send()
        .await
        .expect("request");
    assert_eq!(health.status(), 200);
    let body: serde_json::Value = health.json().await.expect("json");
    assert_eq!(body["status"], "ok");

    cancel.cancel();
    let _ = task.await;
}

#[tokio::test]
async fn an_unmounted_path_is_a_404_rather_than_a_hang() {
    let port = free_port().await;
    let cancel = CancellationToken::new();
    let task = h1::serve(
        Surface::new().router(),
        &format!("127.0.0.1:{port}"),
        None,
        cancel.clone(),
    )
    .await
    .expect("serve");

    let response = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{port}/v1/nothing"))
        .send()
        .await
        .expect("request");
    assert_eq!(response.status(), 404);

    cancel.cancel();
    let _ = task.await;
}

#[tokio::test]
async fn an_address_already_in_use_fails_at_bind() {
    let held = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = held.local_addr().expect("addr").to_string();

    let err = h1::serve(
        Surface::new().router(),
        &addr,
        None,
        CancellationToken::new(),
    )
    .await
    .expect_err("the port is taken");

    // Reported to the caller, not from a background task nobody is watching.
    assert!(err.to_string().contains("failed to listen"), "{err}");
}

#[tokio::test]
async fn cancelling_releases_the_port() {
    let port = free_port().await;
    let cancel = CancellationToken::new();
    let task = h1::serve(
        Surface::new().router(),
        &format!("127.0.0.1:{port}"),
        None,
        cancel.clone(),
    )
    .await
    .expect("serve");

    cancel.cancel();
    let _ = task.await;

    tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .expect("port released");
}
