//! Where an agent actually answers, resolved once so the card, the banner and the mount all
//! read from the same place.

use super::Transport;

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

/// Makes a path rooted and unslashed, so `"/"` stays `"/"` and `"a2a/"` becomes `"/a2a"`.
///
/// Callers join onto the result without guessing, which is the whole point of normalising it
/// in one place.
pub fn normalize_base_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return super::DEFAULT_BASE_PATH.to_string();
    }

    let rooted = if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    };

    let collapsed = rooted.trim_end_matches('/');
    if collapsed.is_empty() {
        "/".to_string()
    } else {
        collapsed.to_string()
    }
}

/// Where the HTTP transports mount, in precedence order: a generated path, then a configured
/// one, then [`super::DEFAULT_BASE_PATH`].
///
/// Generated wins over configured on the same reasoning as the MCP runtime: a path a generator
/// derived from the agent's proto is part of a contract clients already hold, while a hand-set
/// one is a preference. A caller who genuinely needs to move a generated endpoint changes it
/// at the source.
pub fn resolve_base_path(generated: Option<&str>, configured: Option<&str>) -> String {
    for candidate in [generated, configured].into_iter().flatten() {
        if !candidate.trim().is_empty() {
            return normalize_base_path(candidate);
        }
    }
    super::DEFAULT_BASE_PATH.to_string()
}

/// Fills in a host for an address that names only a port, so `":9000"` advertises somewhere a
/// client can actually connect.
///
/// A wildcard bind is a instruction, not a destination: `0.0.0.0` means every interface *here*
/// and nothing at all to a client that dials it, so it is replaced with loopback.
pub fn host_port_or_default(addr: &str) -> String {
    if addr.is_empty() {
        return format!("{}:{}", super::DEFAULT_HOST, super::DEFAULT_PORT);
    }

    let Some((host, port)) = addr.rsplit_once(':') else {
        return addr.to_string();
    };

    let host = match host.trim_matches(['[', ']']) {
        "" | "0.0.0.0" | "::" => super::DEFAULT_HOST,
        named => named,
    };
    format!("{host}:{port}")
}
