//! Pre-flight checks on a server's configuration, and the TLS material it runs on.

use crate::error::{Error, Result};
use crate::options::Options;

/// The default port for the HTTP gateway when none is set.
pub const DEFAULT_HTTP_PORT: u16 = 6502;
/// The default port for the gRPC server when none is set.
pub const DEFAULT_GRPC_PORT: u16 = 65020;

/// Fills in defaults and rejects a configuration that cannot be served.
///
/// It mutates `opts`, because the defaults it picks — the consecutive MCP and A2A ports
/// below — are what the startup banner and the agent cards then advertise. Resolving them
/// anywhere else would let the printed address and the bound one disagree.
///
/// Port 0 on gRPC or HTTP is left alone: it asks the operating system for a free port. The
/// default ports live in [`Options::default`] instead. MCP and A2A still derive theirs from
/// HTTP's when it is fixed; when it is 0 there is nothing to derive from, so they stay 0 and
/// are ephemeral too. `grpc_listener` is false when no gRPC listener will be bound, which
/// leaves no use for a gRPC host.
pub fn validate_options(
    opts: &mut Options,
    has_certificate: bool,
    grpc_listener: bool,
) -> Result<()> {
    if grpc_listener && opts.grpc.host.is_empty() {
        return Err(Error::EmptyGrpcHost);
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
        if opts.mcp.port == 0 && opts.http.port != 0 {
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
        if opts.a2a.port == 0 && (opts.http.port != 0 || opts.mcp.port != 0) {
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
        // HTTP/3 is advertised as one past the gateway, so that port has to be known and
        // has to exist. Binding QUIC to whatever follows an ephemeral port would be a guess.
        if opts.http.port == 0 {
            return Err(Error::InvalidPort { component: "http3" });
        }
        next_port(opts.http.port, "http3")?;
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
        validate_options(&mut opts, false, true).expect("valid");
        assert_eq!(opts.mcp.port, 0, "no transport named, no port chosen");

        let mut opts = options(Some(McpTransport::Stdio));
        validate_options(&mut opts, false, true).expect("valid");
        assert_eq!(opts.mcp.port, 0, "stdio binds nothing");

        let mut opts = options(Some(McpTransport::StreamableHttp));
        validate_options(&mut opts, false, true).expect("valid");
        assert_eq!(opts.mcp.port, opts.http.port + 1);
        assert_eq!(opts.mcp.host, agents::DEFAULT_HOST);
    }

    #[test]
    fn port_zero_stays_zero_so_the_operating_system_chooses() {
        let mut opts = options(Some(McpTransport::StreamableHttp));
        opts.enable_a2a = true;
        opts.grpc.port = 0;
        opts.http.port = 0;
        validate_options(&mut opts, false, true).expect("valid");
        assert_eq!(
            (opts.grpc.port, opts.http.port, opts.mcp.port, opts.a2a.port),
            (0, 0, 0, 0)
        );
    }

    #[test]
    fn an_unnamed_port_is_the_default_one() {
        let opts = Options::default();
        assert_eq!((opts.grpc.port, opts.http.port), (65020, 6502));
    }

    #[test]
    fn http3_cannot_follow_an_ephemeral_http_port() {
        let mut opts = options(None);
        opts.http.port = 0;
        opts.experimental_http3 = true;
        assert!(matches!(
            validate_options(&mut opts, true, true),
            Err(crate::error::Error::InvalidPort { component: "http3" })
        ));
    }

    #[test]
    fn a_grpc_host_is_needed_only_by_a_grpc_listener() {
        let mut opts = options(None);
        opts.grpc.host.clear();
        assert!(validate_options(&mut opts, false, true).is_err());
        assert!(validate_options(&mut opts, false, false).is_ok());
    }
}
