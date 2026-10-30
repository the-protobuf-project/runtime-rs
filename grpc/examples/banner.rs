//! Prints the startup banner, for eyeballing it without starting a server.
//!
//!     cargo run -p tpp-grpc --example banner

use grpc::banner::{self, Summary};
use grpc::options::{Endpoint, Options, ServerEnvironment};

fn main() {
    let opts = Options {
        service_name: "bookstore".into(),
        version: "1.4.2".into(),
        environment: ServerEnvironment::Production,
        grpc: Endpoint::new("0.0.0.0", 50051),
        http: Endpoint::new("0.0.0.0", 8080),
        mcp: grpc::options::McpOptions {
            host: "0.0.0.0".into(),
            port: 8081,
            ..Default::default()
        },
        a2a: grpc::options::A2aOptions {
            host: "0.0.0.0".into(),
            port: 8082,
            ..Default::default()
        },
        enable_http: true,
        enable_health: true,
        experimental_http3: true,
        ..Default::default()
    };

    let state = Summary {
        services: vec![
            "bookstore.v1.BookService".into(),
            "bookstore.v1.ShelfService".into(),
        ],
        tls: true,
        health: true,
        reflection: true,
        otlp_endpoint: "localhost:12005".into(),
        rest_routes: 2,
        agents: vec![
            agents::Endpoint {
                protocol: agents::Protocol::Mcp,
                transport: "streamable-http".into(),
                url: "http://0.0.0.0:8081/bookstore/v1/mcp".into(),
                detail: "/bookstore/v1/mcp".into(),
            },
            agents::Endpoint {
                protocol: agents::Protocol::A2a,
                transport: "jsonrpc".into(),
                url: "http://0.0.0.0:8082/a2a".into(),
                detail: "/.well-known/agent-card.json".into(),
            },
        ],
    };

    banner::print(&opts, &state);
}
