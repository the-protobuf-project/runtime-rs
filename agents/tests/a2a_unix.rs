//! A2A cards and ready endpoints advertise the Unix socket using an encoded authority.

#![cfg(all(unix, feature = "a2a"))]

mod common;
#[path = "common/socket.rs"]
mod socket;

use agents::Runtime;
use agents::a2a::{self, ServerConfig};
use common::{config, echo_agent, free_port};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn cards_and_ready_endpoints_name_the_bound_unix_socket() {
    let (_directory, path) = socket::socket("a2a #?");
    let service = a2a::Service::new(echo_agent(), vec![])
        .addr(format!("unix:{}", path.display()))
        .serve_agent_card(true);
    let runtime = Runtime::new(config(free_port().await)).register(service);
    let cancel = CancellationToken::new();
    runtime.start(cancel.clone()).await.unwrap();
    let endpoints = runtime.endpoints();
    let endpoint = &endpoints[0];
    assert!(
        endpoint.url.starts_with("http+unix://%2Ftmp%2F"),
        "{}",
        endpoint.url
    );
    assert!(endpoint.url.ends_with("a2a%20%23%3F.sock/a2a"));
    assert_eq!(
        endpoint.detail,
        endpoint.url.replace("/a2a", a2a::AGENT_CARD_PATH)
    );

    let mut stream = tokio::net::UnixStream::connect(&path).await.unwrap();
    stream
        .write_all(
            format!(
                "GET {} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                a2a::AGENT_CARD_PATH
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = String::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        stream.read_to_string(&mut response),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.contains(&endpoint.url), "{response}");
    cancel.cancel();
    runtime.shutdown().await.unwrap();
    assert!(!path.exists());
}

#[test]
fn a_unix_listener_keeps_an_explicit_public_url() {
    let config = ServerConfig {
        addr: "unix:/tmp/a2a.sock".into(),
        public_url: "https://agents.example.com".into(),
        ..Default::default()
    };
    let endpoint = a2a::server_endpoint(&config).unwrap();
    assert_eq!(endpoint.url, "https://agents.example.com/a2a");
    assert_eq!(
        endpoint.card_url,
        format!("https://agents.example.com{}", a2a::AGENT_CARD_PATH)
    );
}

#[test]
fn a_grpc_unix_endpoint_is_a_dial_target() {
    let config = ServerConfig {
        addr: "unix:/tmp/a2a.sock".into(),
        transports: vec![a2a::Transport::Grpc],
        ..Default::default()
    };
    assert_eq!(a2a::server_endpoint(&config).unwrap().url, config.addr);
}
