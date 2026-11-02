//! Where the runtime puts services: sharing one listener, splitting onto their own, and
//! mounting onto a host's router instead of binding at all.

mod common;

use agents::{Config, Mux, Protocol, Runtime};
use axum::Router;
use axum::routing::get;
use tokio_util::sync::CancellationToken;

use common::*;

#[tokio::test]
async fn two_protocols_naming_no_address_share_one_port() {
    let port = free_port().await;
    let runtime = Runtime::new(config(port))
        .register(Fake::mounting(Protocol::Mcp, "/mcp", "tools"))
        .register(Fake::mounting(Protocol::A2a, "/a2a", "agent"));

    let cancel = CancellationToken::new();
    runtime.start(cancel.clone()).await.expect("start");

    let client = reqwest::Client::new();
    for (path, expected) in [("/mcp", "tools"), ("/a2a", "agent")] {
        let body = client
            .get(format!("http://127.0.0.1:{port}{path}"))
            .send()
            .await
            .expect("request")
            .text()
            .await
            .expect("body");
        assert_eq!(body, expected, "{path} answered on the shared listener");
    }

    assert_eq!(runtime.endpoints().len(), 2);

    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_service_naming_an_address_gets_a_port_of_its_own() {
    let shared = free_port().await;
    let own = free_port().await;

    let runtime = Runtime::new(config(shared))
        .register(Fake::mounting(Protocol::Mcp, "/mcp", "tools"))
        .register(Fake::mounting(Protocol::A2a, "/a2a", "agent").at(&format!("127.0.0.1:{own}")));

    let cancel = CancellationToken::new();
    runtime.start(cancel.clone()).await.expect("start");

    let client = reqwest::Client::new();
    let tools = client
        .get(format!("http://127.0.0.1:{shared}/mcp"))
        .send()
        .await
        .expect("shared request");
    assert!(tools.status().is_success());

    let agent = client
        .get(format!("http://127.0.0.1:{own}/a2a"))
        .send()
        .await
        .expect("own request");
    assert!(agent.status().is_success());

    // Each is only on its own listener, which is the point of naming an address.
    let crossed = client
        .get(format!("http://127.0.0.1:{shared}/a2a"))
        .send()
        .await
        .expect("crossed request");
    assert_eq!(crossed.status(), reqwest::StatusCode::NOT_FOUND);

    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_host_router_is_mounted_onto_rather_than_bound() {
    let port = free_port().await;
    let host_mux = Mux::from_router(Router::new().route("/host", get(|| async { "host" })));

    let runtime = Runtime::new(Config {
        mux: Some(host_mux.clone()),
        ..config(port)
    })
    .register(Fake::mounting(Protocol::Mcp, "/mcp", "tools"));

    let cancel = CancellationToken::new();
    runtime.start(cancel.clone()).await.expect("start");

    // The runtime bound nothing: the host owns that server.
    assert!(
        tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_err(),
        "runtime must not bind a port it was given a router for"
    );

    // ...but the service did mount onto the host's router.
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .expect("host binds its own listener");
    let serving = tokio::spawn(async move {
        let _ = axum::serve(listener, host_mux.router()).await;
    });

    let client = reqwest::Client::new();
    for (path, expected) in [("/host", "host"), ("/mcp", "tools")] {
        let body = client
            .get(format!("http://127.0.0.1:{port}{path}"))
            .send()
            .await
            .expect("request")
            .text()
            .await
            .expect("body");
        assert_eq!(body, expected);
    }

    serving.abort();
    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_service_on_port_zero_is_placed_on_and_reports_the_port_it_got() {
    let runtime = Runtime::new(config(free_port().await))
        .register(Fake::mounting(Protocol::Mcp, "/mcp", "tools").at("127.0.0.1:0"));

    let cancel = CancellationToken::new();
    runtime.start(cancel.clone()).await.expect("start");

    let bound = runtime
        .bound_addr("127.0.0.1:0")
        .expect("the listener for port 0 reports where it bound");
    let port: u16 = bound
        .rsplit_once(':')
        .and_then(|(_, port)| port.parse().ok())
        .expect("a port");
    assert_ne!(port, 0, "an ephemeral port, not port 0: {bound}");

    let endpoints = runtime.endpoints();
    assert_eq!(endpoints.len(), 1);
    assert_eq!(
        endpoints[0].url,
        format!("http://127.0.0.1:{port}/mcp"),
        "the service was placed on the bound address, not the one it asked for"
    );

    let body = reqwest::get(&endpoints[0].url)
        .await
        .expect("the advertised address answers")
        .text()
        .await
        .expect("body");
    assert_eq!(body, "tools");

    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");
    assert_eq!(
        runtime.bound_addr("127.0.0.1:0"),
        None,
        "gone after shutdown"
    );
}

#[tokio::test]
async fn a_service_on_a_fixed_port_is_reported_exactly_as_configured() {
    let port = free_port().await;
    let addr = format!("127.0.0.1:{port}");
    let runtime = Runtime::new(config(free_port().await))
        .register(Fake::mounting(Protocol::Mcp, "/mcp", "tools").at(&addr));

    let cancel = CancellationToken::new();
    runtime.start(cancel.clone()).await.expect("start");
    assert_eq!(runtime.bound_addr(&addr), Some(addr.clone()));
    assert_eq!(runtime.endpoints()[0].url, format!("http://{addr}/mcp"));

    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");
}
