//! Where an agent actually answers, resolved once so the card, the banner and the mount all
//! read from the same place.

use super::config::ServerConfig;
use super::error::Error;
use super::{AGENT_CARD_PATH, DEFAULT_BASE_PATH, DEFAULT_HOST, DEFAULT_PORT, Transport};

/// One resolved place an agent can be reached, as it will appear on the card.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    /// The configured transport this endpoint serves.
    pub transport: Transport,
    /// The name that transport goes by on the wire.
    pub protocol: &'static str,
    /// Where a client connects. For gRPC it is a `host:port` dial target rather than a URL,
    /// which is what a gRPC client expects.
    pub url: String,
    /// Where the public agent card is served, empty when this server does not serve one.
    pub card_url: String,
}

/// The endpoint a client should prefer — the first configured transport.
///
/// For logging where an agent came up before starting it. It returns
/// [`Error::NoTransports`] when the config resolves to none.
pub fn server_endpoint(cfg: &ServerConfig) -> Result<Endpoint, Error> {
    server_endpoints(cfg)
        .into_iter()
        .next()
        .ok_or(Error::NoTransports)
}

/// Every endpoint the config resolves to, in the order they are advertised.
///
/// An agent serving both JSON-RPC and gRPC has two, and a host printing a startup summary
/// wants both lines.
pub fn server_endpoints(cfg: &ServerConfig) -> Vec<Endpoint> {
    let card_url = card_url(cfg);
    cfg.resolved_transports()
        .into_iter()
        .map(|transport| Endpoint {
            transport,
            protocol: transport.protocol_binding(),
            url: public_url_for(cfg, transport),
            card_url: card_url.clone(),
        })
        .collect()
}

/// The base every advertised URL is built from: the public URL, or the listen address.
fn public_base(cfg: &ServerConfig) -> String {
    if cfg.public_url.is_empty() {
        format!("http://{}", host_port_or_default(&cfg.resolved_addr()))
    } else {
        cfg.public_url.clone()
    }
}

/// `base` with its path replaced by `path`, the way Go sets `url.URL.Path` and re-renders.
///
/// A base that does not parse as an absolute URL has `path` appended instead, which is what
/// Go falls back to for the same input.
fn with_path(base: &str, path: &str) -> String {
    match base.parse::<http::Uri>() {
        Ok(uri) if uri.scheme().is_some() && uri.authority().is_some() => {
            let query = uri.query().map(|q| format!("?{q}")).unwrap_or_default();
            format!(
                "{}://{}{path}{query}",
                uri.scheme_str().unwrap_or("http"),
                uri.authority().map(|a| a.as_str()).unwrap_or_default(),
            )
        }
        _ => format!("{base}{path}"),
    }
}

/// Where a client should reach this agent over `transport`.
pub(super) fn public_url_for(cfg: &ServerConfig, transport: Transport) -> String {
    let base = public_base(cfg);
    if transport == Transport::Grpc {
        // A gRPC target is host:port, not a URL. A scheme here is what turns a client's dial
        // into a confusing name-resolution error.
        return match base.parse::<http::Uri>() {
            Ok(uri) if uri.scheme().is_some() => uri
                .authority()
                .map_or(base.clone(), |a| a.as_str().to_string()),
            _ => base,
        };
    }
    // REST routes on paths of its own beneath the base path; JSON-RPC answers at the base
    // path itself. Both are reached through the same prefix.
    with_path(&base, &cfg.resolved_base_path())
}

/// Where this server publishes its card, or empty when it publishes none — a shared-mux agent
/// whose host mounts the card instead.
///
/// An agent that owns its mux always serves the card, which is why "off" only means off
/// when there is a host mux to defer to.
pub(super) fn card_url(cfg: &ServerConfig) -> String {
    if !cfg.serve_agent_card && cfg.mux.is_some() {
        return String::new();
    }
    // The card is at the host root by protocol, not under the agent's base path — clients
    // look for exactly one place and this is it.
    with_path(&public_base(cfg), AGENT_CARD_PATH)
}

/// Makes a path rooted and clean, so `"/"` stays `"/"`, `"a2a/"` becomes `"/a2a"` and
/// `"/a//b/../c/"` becomes `"/a/c"`.
///
/// It is Go's `path.Clean` on a rooted path, so a base path resolves to the same mount in
/// both runtimes. Callers join onto the result without guessing. An empty path is
/// [`DEFAULT_BASE_PATH`].
pub fn normalize_base_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return DEFAULT_BASE_PATH.to_string();
    }
    clean_rooted(trimmed)
}

/// Go's `path.Clean`, for a path treated as rooted: empty and `.` segments vanish, `..`
/// removes the segment before it and cannot climb above the root.
fn clean_rooted(path: &str) -> String {
    let mut segments: Vec<&str> = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                segments.pop();
            }
            named => segments.push(named),
        }
    }
    format!("/{}", segments.join("/"))
}

/// Where the HTTP transports mount, in precedence order: `generated_default`, then
/// [`ServerConfig::generated_base_path`], then [`ServerConfig::base_path`], then
/// [`DEFAULT_BASE_PATH`].
///
/// Generated wins over configured on the same reasoning as the MCP runtime: a path a
/// generator derived from the agent's proto is part of a contract clients already hold, while
/// a hand-set one is a preference. A caller who genuinely needs to move a generated endpoint
/// changes it at the source.
pub fn resolve_base_path(cfg: &ServerConfig, generated_default: &str) -> String {
    [
        generated_default,
        cfg.generated_base_path.as_str(),
        cfg.base_path.as_str(),
    ]
    .into_iter()
    .map(str::trim)
    .find(|candidate| !candidate.is_empty())
    .map_or_else(|| DEFAULT_BASE_PATH.to_string(), normalize_base_path)
}

/// Fills in a host for an address that names only a port, so `":9000"` advertises somewhere a
/// client can actually connect.
///
/// A wildcard bind is an instruction, not a destination: `0.0.0.0` means every interface
/// *here* and nothing at all to a client that dials it, so it is replaced with loopback. An
/// IPv6 host keeps its brackets, as Go's `net.JoinHostPort` writes it.
pub fn host_port_or_default(addr: &str) -> String {
    if addr.is_empty() {
        return format!("{DEFAULT_HOST}:{DEFAULT_PORT}");
    }

    let Some((host, port)) = addr.rsplit_once(':') else {
        return addr.to_string();
    };
    let bracketed = host.starts_with('[') && host.ends_with(']');
    // An unbracketed host with a colon in it is an IPv6 literal with no port, which Go's
    // SplitHostPort rejects — and so returns the address as written.
    if !bracketed && host.contains(':') {
        return addr.to_string();
    }

    let host = match host.trim_matches(['[', ']']) {
        "" | "0.0.0.0" | "::" => DEFAULT_HOST,
        named => named,
    };
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}
