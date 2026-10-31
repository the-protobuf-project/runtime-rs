//! Host and origin restrictions compose across transports and group/member settings.

#![cfg(feature = "mcp")]

mod common;
mod mcp_probe;

use agents::Runtime;
use agents::mcp::{Service, ServiceGroup, Transport};
use common::{config, free_port};
use mcp_probe::Probe;
use tokio_util::sync::CancellationToken;

const INIT: &str = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}"#;
const ORIGIN: &str = "https://app.example.com";

async fn serve(service: impl agents::Service) -> (Runtime, CancellationToken, u16) {
    let port = free_port().await;
    let runtime = Runtime::new(config(port)).register(service);
    let cancel = CancellationToken::new();
    runtime.start(cancel.clone()).await.expect("start");
    (runtime, cancel, port)
}

async fn request(port: u16, sse: bool, post: bool, host: &str, origin: Option<&str>) -> u16 {
    let path = if sse {
        "/mcp/?sessionId=missing"
    } else {
        "/mcp"
    };
    let method = if post {
        reqwest::Method::POST
    } else {
        reqwest::Method::GET
    };
    let mut request = reqwest::Client::new()
        .request(method, format!("http://127.0.0.1:{port}{path}"))
        .header("host", host)
        .header("accept", "application/json, text/event-stream");
    if let Some(origin) = origin {
        request = request.header("origin", origin);
    }
    if post {
        request = request
            .header("content-type", "application/json")
            .body(INIT);
    }
    request.send().await.expect("send").status().as_u16()
}

#[tokio::test]
async fn sse_checks_both_guards_on_get_and_post() {
    let service = Service::from_handler(Probe::new("probe"))
        .transports([Transport::Sse])
        .allowed_hosts(["localhost"])
        .allowed_origins([ORIGIN]);
    let (runtime, cancel, port) = serve(service).await;
    for post in [false, true] {
        assert_eq!(
            request(port, true, post, "localhost", Some("https://evil.test")).await,
            403
        );
        assert_eq!(
            request(port, true, post, "evil.test", Some(ORIGIN)).await,
            403
        );
        let expected = if post { 404 } else { 200 };
        assert_eq!(
            request(port, true, post, "localhost", Some(ORIGIN)).await,
            expected
        );
        assert_eq!(request(port, true, post, "localhost", None).await, expected);
    }
    cancel.cancel();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn group_hosts_preserve_member_origins() {
    for transport in [Transport::StreamableHttp, Transport::Sse] {
        let group = ServiceGroup::new()
            .with(Service::from_handler(Probe::new("probe")).allowed_origins([ORIGIN]))
            .transports([transport])
            .allowed_hosts(["localhost"]);
        let (runtime, cancel, port) = serve(group).await;
        let sse = transport == Transport::Sse;
        assert_eq!(
            request(port, sse, !sse, "localhost", Some(ORIGIN)).await,
            200
        );
        assert_eq!(
            request(port, sse, !sse, "localhost", Some("https://evil.test")).await,
            403
        );
        cancel.cancel();
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn group_origins_preserve_member_hosts() {
    for transport in [Transport::StreamableHttp, Transport::Sse] {
        let group = ServiceGroup::new()
            .with(Service::from_handler(Probe::new("probe")).allowed_hosts(["localhost"]))
            .transports([transport])
            .allowed_origins([ORIGIN]);
        let (runtime, cancel, port) = serve(group).await;
        let sse = transport == Transport::Sse;
        assert_eq!(
            request(port, sse, !sse, "localhost", Some(ORIGIN)).await,
            200
        );
        assert_eq!(
            request(port, sse, !sse, "evil.test", Some(ORIGIN)).await,
            403
        );
        cancel.cancel();
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn an_explicit_empty_group_origin_list_overrides_the_member_list() {
    let group = ServiceGroup::new()
        .with(Service::from_handler(Probe::new("probe")).allowed_origins([ORIGIN]))
        .allowed_origins(std::iter::empty::<String>());
    let (runtime, cancel, port) = serve(group).await;
    assert_eq!(
        request(port, false, true, "localhost", Some("https://other.test")).await,
        200
    );
    cancel.cancel();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn empty_host_lists_are_rejected_for_members_and_groups() {
    for transport in [Transport::StreamableHttp, Transport::Sse] {
        let member = ServiceGroup::new().with(
            Service::from_handler(Probe::new("probe")).allowed_hosts(std::iter::empty::<String>()),
        );
        let group = ServiceGroup::new()
            .with(Service::from_handler(Probe::new("probe")))
            .allowed_hosts(std::iter::empty::<String>());
        for group in [member, group] {
            let runtime =
                Runtime::new(config(free_port().await)).register(group.transports([transport]));
            let error = runtime
                .start(CancellationToken::new())
                .await
                .expect_err("empty hosts");
            assert!(
                error
                    .to_string()
                    .contains("allowed_hosts must not be empty"),
                "{error}"
            );
        }
    }
}
