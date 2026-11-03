//! What MCP and A2A advertise for a named-pipe address. Pure computation, so it runs on every
//! platform: a Unix host can check what a Windows deployment will tell its clients.

#![cfg(all(feature = "mcp", feature = "a2a"))]

use agents::{a2a, mcp};

/// `\\.\pipe\agents`, percent-encoded as a URL authority.
const PIPE_AUTHORITY: &str = "%5C%5C.%5Cpipe%5Cagents";

#[test]
fn mcp_on_a_pipe_is_advertised_as_http_npipe() {
    let config = mcp::ServerConfig {
        addr: "pipe:agents".into(),
        ..Default::default()
    };
    let endpoint = mcp::server_endpoint_with(&config, |_| None);
    assert_eq!(endpoint.protocol, "http+npipe");
    let base = format!("http+npipe://{PIPE_AUTHORITY}");
    assert!(endpoint.url.starts_with(&base), "{}", endpoint.url);
}

#[test]
fn a2a_on_a_pipe_is_advertised_as_http_npipe() {
    let config = a2a::ServerConfig {
        addr: "pipe:agents".into(),
        ..Default::default()
    };
    let endpoint = a2a::server_endpoint(&config).unwrap();
    assert_eq!(endpoint.url, format!("http+npipe://{PIPE_AUTHORITY}/a2a"));
    assert_eq!(
        endpoint.card_url,
        format!("http+npipe://{PIPE_AUTHORITY}{}", a2a::AGENT_CARD_PATH)
    );
}

#[test]
fn a2a_grpc_on_a_pipe_is_a_dial_target() {
    let config = a2a::ServerConfig {
        addr: r"pipe:\\.\pipe\agents".into(),
        transports: vec![a2a::Transport::Grpc],
        ..Default::default()
    };
    assert_eq!(a2a::server_endpoint(&config).unwrap().url, config.addr);
}

#[test]
fn a_pipe_listener_keeps_an_explicit_public_url() {
    let config = a2a::ServerConfig {
        addr: "pipe:agents".into(),
        public_url: "https://agents.example.com".into(),
        ..Default::default()
    };
    let endpoint = a2a::server_endpoint(&config).unwrap();
    assert_eq!(endpoint.url, "https://agents.example.com/a2a");
}
