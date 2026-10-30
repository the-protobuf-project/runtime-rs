//! Draining: what shutdown releases, and what it tolerates being called on.

mod common;

use agents::{Protocol, Runtime};
use tokio_util::sync::CancellationToken;

use common::*;

#[tokio::test]
async fn shutdown_releases_the_port() {
    let port = free_port().await;
    let runtime =
        Runtime::new(config(port)).register(Fake::mounting(Protocol::Mcp, "/mcp", "tools"));

    let cancel = CancellationToken::new();
    runtime.start(cancel.clone()).await.expect("start");
    assert!(
        tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
    );

    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");

    // Rebinding proves the drain finished rather than merely being asked for.
    tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .expect("port released by shutdown");
}

#[tokio::test]
async fn shutdown_is_safe_on_a_runtime_that_never_started() {
    let runtime = Runtime::new(config(0));
    runtime.shutdown().await.expect("first");
    runtime.shutdown().await.expect("second");
}
