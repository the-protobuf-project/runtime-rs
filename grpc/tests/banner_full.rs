//! The whole banner: art, build line, table and warnings in one piece.

use grpc::banner::{self, Summary};
use grpc::options::{Endpoint, Options, ServerEnvironment};

fn server() -> (Options, Summary) {
    let opts = Options {
        service_name: "bookstore".into(),
        version: "1.4.2".into(),
        environment: ServerEnvironment::Production,
        grpc: Endpoint::new("0.0.0.0", 50051),
        http: Endpoint::new("0.0.0.0", 8080),
        enable_http: true,
        enable_health: true,
        experimental_http3: true,
        ..Default::default()
    };
    let state = Summary {
        services: vec!["bookstore.v1.BookService".into()],
        tls: true,
        health: true,
        reflection: false,
        otlp_endpoint: "localhost:12005".into(),
        rest_routes: 2,
        agents: Vec::new(),
    };
    (opts, state)
}

#[test]
fn the_banner_carries_every_part() {
    let (opts, state) = server();
    let out = banner::render(&opts, &state);

    // The art, from the service name.
    assert!(out.contains("_.___/"), "missing ASCII art:\n{out}");
    // The build line.
    assert!(out.contains("Version: 1.4.2"), "{out}");
    assert!(out.contains("PRODUCTION"), "{out}");
    assert!(out.contains("Build: #"), "{out}");
    // The table.
    assert!(out.contains("bookstore.v1.BookService"), "{out}");
    assert!(out.contains("HTTP/3 ⚗"), "{out}");
    assert!(out.contains("2 REST Routes  +  /health"), "{out}");
    // And the health warning under it.
    assert!(out.contains("Health check service enabled"), "{out}");
}

#[test]
fn running_without_tls_is_warned_about() {
    let (opts, mut state) = server();
    state.tls = false;
    let out = banner::render(&opts, &state);
    assert!(out.contains("without TLS"), "{out}");
    assert!(out.contains("plaintext"), "{out}");
}

#[test]
fn tls_is_not_warned_about() {
    let (opts, state) = server();
    assert!(!banner::render(&opts, &state).contains("without TLS"));
}

#[test]
fn http3_sits_one_port_above_the_gateway() {
    let (opts, state) = server();
    let rows = banner::rows(&opts, &state);
    let h3 = rows
        .iter()
        .find(|r| r.section.starts_with("HTTP/3"))
        .unwrap();
    assert_eq!(h3.address, "0.0.0.0:8081");
}

#[test]
fn a_disabled_component_takes_no_row() {
    let (mut opts, mut state) = server();
    opts.enable_http = false;
    opts.experimental_http3 = false;
    state.health = false;

    let rows = banner::rows(&opts, &state);
    let sections: std::collections::BTreeSet<&str> =
        rows.iter().map(|r| r.section.as_str()).collect();
    assert_eq!(sections, ["gRPC"].into_iter().collect());
}

#[test]
fn agent_endpoints_are_numbered_within_their_protocol() {
    let (opts, mut state) = server();
    state.agents = vec![
        agents::Endpoint {
            protocol: agents::Protocol::Mcp,
            transport: "streamable-http".into(),
            url: "http://0.0.0.0:8081/mcp".into(),
            detail: "/mcp".into(),
        },
        agents::Endpoint {
            protocol: agents::Protocol::A2a,
            transport: "jsonrpc".into(),
            url: "http://0.0.0.0:8082/a2a".into(),
            detail: "http://0.0.0.0:8082/.well-known/agent-card.json".into(),
        },
    ];

    let rows = banner::rows(&opts, &state);
    let mcp = rows.iter().find(|r| r.section == "MCP").unwrap();
    let a2a = rows.iter().find(|r| r.section == "A2A").unwrap();

    assert_eq!(mcp.component, "Server 1  (streamable-http)");
    assert_eq!(mcp.detail, "/mcp");
    assert_eq!(a2a.component, "Agent 1  (jsonrpc)");
    // The card is what a client fetches first, so the row says where it is.
    assert!(a2a.detail.contains("card "), "{}", a2a.detail);
}
