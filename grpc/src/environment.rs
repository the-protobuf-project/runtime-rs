//! Environment-variable overrides for [`crate::options::Options`].
//!
//! Variables follow `{SERVICENAME}_HYBRID_{COMPONENT}_{SETTING}`, so a service named
//! `vision` takes its gRPC port from `VISION_HYBRID_GRPC_PORT`. This is what lets one
//! binary be repointed across deployment environments without a rebuild.

use std::str::FromStr;

use crate::options::{A2aTransport, Options};

/// Applies every override present in the process environment.
///
/// A variable that is set but unparseable is ignored with a debug log rather than failing
/// the start: a malformed port should not take a service down when the compiled-in default
/// is serviceable.
pub fn apply_env_overrides(opts: &mut Options) {
    let prefix = format!("{}_HYBRID", opts.service_name.to_uppercase());
    tracing::debug!(%prefix, "applyEnvOverrides: scanning env");

    override_host(&prefix, "GRPC", &mut opts.grpc.host);
    override_port(&prefix, "GRPC", &mut opts.grpc.port);
    override_host(&prefix, "HTTP", &mut opts.http.host);
    override_port(&prefix, "HTTP", &mut opts.http.port);
    override_host(&prefix, "MCP", &mut opts.mcp.host);
    override_port(&prefix, "MCP", &mut opts.mcp.port);
    override_host(&prefix, "A2A", &mut opts.a2a.host);
    override_port(&prefix, "A2A", &mut opts.a2a.port);

    if let Ok(raw) = std::env::var(format!("{prefix}_A2A_TRANSPORT")) {
        // Several transports run at once, so this is a list; a value that resolves to
        // nothing leaves the configured setting alone rather than silently dropping the
        // agent to no transport at all.
        let parsed = parse_a2a_transports(&raw);
        if parsed.is_empty() {
            tracing::debug!(%prefix, %raw, "applyEnvOverrides: named no known A2A transport, ignoring");
        } else {
            tracing::debug!(%prefix, %raw, "applyEnvOverrides: overriding A2A transports");
            opts.a2a.transports = parsed;
        }
    }
}

fn override_host(prefix: &str, component: &str, target: &mut String) {
    if let Ok(host) = std::env::var(format!("{prefix}_{component}_HOST"))
        && !host.is_empty()
    {
        tracing::debug!(%prefix, %component, %host, "applyEnvOverrides: overriding host");
        *target = host;
    }
}

fn override_port(prefix: &str, component: &str, target: &mut u16) {
    let Ok(raw) = std::env::var(format!("{prefix}_{component}_PORT")) else {
        return;
    };
    if raw.is_empty() {
        return;
    }
    match raw.parse::<u16>() {
        Ok(port) => {
            tracing::debug!(%prefix, %component, port, "applyEnvOverrides: overriding port");
            *target = port;
        }
        Err(_) => {
            tracing::debug!(%prefix, %component, %raw, "applyEnvOverrides: not a valid port, ignoring");
        }
    }
}

/// Splits a comma-separated transport list, dropping entries that name nothing this server
/// can serve.
pub fn parse_a2a_transports(raw: &str) -> Vec<A2aTransport> {
    raw.split(',')
        .filter_map(|part| A2aTransport::from_str(part).ok())
        .collect()
}

/// The value of an environment variable, or `default` when it is unset or empty.
pub fn from_env_or_default(key: &str, default: &str) -> String {
    match std::env::var(key) {
        Ok(value) if !value.is_empty() => value,
        _ => default.to_string(),
    }
}
