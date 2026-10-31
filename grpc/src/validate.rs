//! Pre-flight checks on a server's configuration, and the TLS material it runs on.

use crate::error::{Error, Result};
use crate::options::Options;

/// The default port for the HTTP gateway when none is set.
pub const DEFAULT_HTTP_PORT: u16 = 6502;
/// The default port for the gRPC server when none is set.
pub const DEFAULT_GRPC_PORT: u16 = 65020;

/// Fills in defaults and rejects a configuration that cannot be served.
///
/// It mutates `opts`, because the defaults it picks — a gRPC port, and the consecutive
/// MCP and A2A ports below — are what the startup banner and the agent cards then
/// advertise. Resolving them anywhere else would let the printed address and the bound one
/// disagree.
pub fn validate_options(opts: &mut Options, has_certificate: bool) -> Result<()> {
    if opts.grpc.host.is_empty() {
        return Err(Error::EmptyGrpcHost);
    }
    if opts.grpc.port == 0 {
        tracing::debug!(
            port = DEFAULT_GRPC_PORT,
            "validateOptions: applying default gRPC port"
        );
        opts.grpc.port = DEFAULT_GRPC_PORT;
    }

    // Defaulted whether or not HTTP is enabled, because the MCP, A2A and HTTP/3 ports below
    // are derived from it. Go defaults it only with HTTP on, so with HTTP off it derives MCP
    // on port 1; this does not.
    if opts.http.port == 0 {
        tracing::debug!(
            port = DEFAULT_HTTP_PORT,
            "validateOptions: applying default HTTP port"
        );
        opts.http.port = DEFAULT_HTTP_PORT;
    }
    if opts.enable_http && opts.http.host.is_empty() {
        return Err(Error::EmptyHttpHost);
    }

    // MCP over stdio talks on the process's own pipes, so it needs no address at all.
    // As in Go, only a transport that was named and listens gets the defaults: with none
    // named the MCP address is left as given.
    if opts.enable_mcp && opts.mcp.transport.is_some_and(|t| t.listens()) {
        if opts.mcp.host.is_empty() {
            opts.mcp.host = agents::DEFAULT_HOST.to_string();
        }
        if opts.mcp.port == 0 {
            opts.mcp.port = next_port(opts.http.port, "mcp")?;
            tracing::debug!(
                port = opts.mcp.port,
                "validateOptions: applying default MCP port"
            );
        }
    }

    if opts.enable_a2a {
        if opts.a2a.host.is_empty() {
            opts.a2a.host = agents::DEFAULT_HOST.to_string();
        }
        if opts.a2a.port == 0 {
            // One past MCP, which is itself one past HTTP — so the four layers land on
            // consecutive ports without the caller assigning any.
            let base = if opts.mcp.port == 0 {
                next_port(opts.http.port, "a2a")?
            } else {
                opts.mcp.port
            };
            opts.a2a.port = next_port(base, "a2a")?;
            tracing::debug!(
                port = opts.a2a.port,
                "validateOptions: applying default A2A port"
            );
        }
        if opts.a2a.base_path.is_empty() {
            opts.a2a.base_path = crate::options::DEFAULT_A2A_BASE_PATH.to_string();
        }
    }

    if opts.experimental_http3 {
        if !has_certificate {
            return Err(Error::Http3NeedsTls);
        }
        // The gateway's QUIC twin sits one past it, so HTTP/3 needs that port to exist.
        next_port(opts.http.port, "http3")?;
    }

    for (component, port, required) in [
        ("grpc", opts.grpc.port, true),
        ("http", opts.http.port, opts.enable_http),
    ] {
        if required && port == 0 {
            return Err(Error::InvalidPort { component });
        }
    }

    Ok(())
}

/// The port one past `base`, refusing to wrap past the top of the range.
fn next_port(base: u16, component: &'static str) -> Result<u16> {
    base.checked_add(1)
        .filter(|port| *port > 0)
        .ok_or(Error::InvalidPort { component })
}

#[cfg(test)]
mod mcp_port_tests {
    use super::validate_options;
    use crate::options::{McpTransport, Options};

    fn options(transport: Option<McpTransport>) -> Options {
        let mut opts = Options {
            enable_mcp: true,
            ..Default::default()
        };
        opts.grpc.host = "127.0.0.1".into();
        opts.mcp.transport = transport;
        opts
    }

    #[test]
    fn the_mcp_port_is_defaulted_only_for_a_named_listening_transport() {
        let mut opts = options(None);
        validate_options(&mut opts, false).expect("valid");
        assert_eq!(opts.mcp.port, 0, "no transport named, no port chosen");

        let mut opts = options(Some(McpTransport::Stdio));
        validate_options(&mut opts, false).expect("valid");
        assert_eq!(opts.mcp.port, 0, "stdio binds nothing");

        let mut opts = options(Some(McpTransport::StreamableHttp));
        validate_options(&mut opts, false).expect("valid");
        assert_eq!(opts.mcp.port, opts.http.port + 1);
        assert_eq!(opts.mcp.host, agents::DEFAULT_HOST);
    }
}
