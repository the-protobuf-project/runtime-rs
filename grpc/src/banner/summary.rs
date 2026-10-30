//! Turning what a server actually brought up into the rows the table prints.

use crate::options::Options;

use super::style::{pluralise, tls_label};
use super::table::Row;

/// What the banner needs to know about a running server.
///
/// It is a struct rather than a borrow of the server because the banner is printed once, from
/// data the server has already resolved — and because a summary that can be built without a
/// server is one that can be tested without binding a port.
#[derive(Debug, Clone, Default)]
pub struct Summary {
    /// The gRPC services registered, already filtered and sorted.
    pub services: Vec<String>,
    /// Whether TLS is configured.
    pub tls: bool,
    /// Whether the health service is registered.
    pub health: bool,
    /// Whether reflection is registered.
    pub reflection: bool,
    /// Where traces and metrics are sent.
    pub otlp_endpoint: String,
    /// How many REST gateway handlers were registered.
    pub rest_routes: usize,
    /// Where the agent protocols came up.
    pub agents: Vec<agents::Endpoint>,
}

/// Builds the summary rows, in the order the Go runtime prints them.
pub fn rows(opts: &Options, summary: &Summary) -> Vec<Row> {
    let mut rows = Vec::new();
    let grpc_addr = opts.grpc.addr();

    for service in &summary.services {
        rows.push(Row::new(
            "gRPC",
            service,
            &grpc_addr,
            tls_label(summary.tls),
        ));
    }
    if summary.health {
        rows.push(Row::new(
            "gRPC",
            "Health Check",
            &grpc_addr,
            "grpc.health.v1",
        ));
    }
    if summary.reflection {
        rows.push(Row::new(
            "gRPC",
            "Reflection",
            &grpc_addr,
            "grpcurl-compatible",
        ));
    }
    rows.push(Row::new(
        "gRPC",
        "OpenTelemetry",
        &grpc_addr,
        format!("OTLP → {}", summary.otlp_endpoint),
    ));

    rows.extend(agent_rows(opts, &summary.agents));

    if opts.enable_http {
        let scheme = if summary.tls { "https" } else { "http" };
        rows.push(Row::new(
            "HTTP/1.1",
            format!(
                "{}  +  /health",
                pluralise(summary.rest_routes, "REST Route")
            ),
            opts.http.addr(),
            scheme,
        ));
    }
    if opts.experimental_http3 {
        rows.push(Row::new(
            "HTTP/3 ⚗",
            "REST Gateway (QUIC)",
            format!("{}:{}", opts.http.host, opts.http.port.saturating_add(1)),
            "TLS 1.3",
        ));
    }

    rows
}

/// One row per agent endpoint, numbered within its protocol.
fn agent_rows(opts: &Options, endpoints: &[agents::Endpoint]) -> Vec<Row> {
    let (mut mcp_seen, mut a2a_seen) = (0usize, 0usize);

    endpoints
        .iter()
        .map(|endpoint| {
            let is_mcp = endpoint.protocol == agents::Protocol::Mcp;
            let (section, label, addr) = if is_mcp {
                mcp_seen += 1;
                (
                    "MCP",
                    "Server",
                    format!("{}:{}", opts.mcp.host, opts.mcp.port),
                )
            } else {
                a2a_seen += 1;
                (
                    "A2A",
                    "Agent",
                    format!("{}:{}", opts.a2a.host, opts.a2a.port),
                )
            };
            let n = if is_mcp { mcp_seen } else { a2a_seen };

            Row::new(
                section,
                format!("{label} {n}  ({})", endpoint.transport),
                addr,
                detail_for(endpoint),
            )
        })
        .collect()
}

/// What to print in the detail column for one agent endpoint.
///
/// MCP reports the base path it mounted under. A2A reports its URL and, when it published one,
/// where the card is — that is what a client fetches first, so the summary says where rather
/// than making someone guess.
fn detail_for(endpoint: &agents::Endpoint) -> String {
    if endpoint.protocol == agents::Protocol::Mcp {
        return if endpoint.detail.is_empty() {
            endpoint.url.clone()
        } else {
            endpoint.detail.clone()
        };
    }

    if endpoint.detail.is_empty() {
        endpoint.url.clone()
    } else {
        format!("{}  card {}", endpoint.url, endpoint.detail)
    }
}
