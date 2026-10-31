//! Serving one MCP server on its own, outside a [`crate::Runtime`] someone else built: Go's
//! `MCPServerConfig`, `StartServer`, `ServerEndpoint` and the base-path helpers.
//!
//! Go's `NewMCPServer` builds an SDK server for generated code to register onto. In rmcp the
//! generated handler already is the server, so the Rust counterpart of "build a server from
//! this config" is [`ServerConfig::service`], which hosts a handler with the config's
//! settings; [`start_server`] then serves it.

use std::io;
use std::sync::Arc;

use rmcp::ServerHandler;
use tokio_util::sync::CancellationToken;

use super::{
    DEFAULT_BASE_PATH, HealthCheck, Interceptors, Service, Transport, UnsupportedTransport,
};
use crate::shared::HeaderMapping;
use crate::{Config, Runtime};

/// The listen address Go defaults an HTTP transport to.
pub const DEFAULT_ADDR: &str = ":8080";

/// The health route Go documents as the default when one is wanted.
pub const DEFAULT_HEALTH_CHECK_PATH: &str = "/health";

/// Configuration for serving one MCP server.
#[derive(Clone, Default)]
pub struct ServerConfig {
    /// The server name reported during initialization, for a server this config builds.
    pub name: String,
    /// The server version reported during initialization.
    pub version: String,
    /// A single transport, kept for parity with Go; ignored when `transports` is non-empty.
    pub transport: Option<Transport>,
    /// The transports to serve concurrently. Takes precedence over `transport`.
    pub transports: Vec<Transport>,
    /// The listen address for HTTP transports. Empty means [`DEFAULT_ADDR`].
    pub addr: String,
    /// Serves streamable HTTP without sessions, so any replica answers any request. It gives
    /// up server-to-client requests, the standalone `GET` stream and `DELETE`. SSE and stdio
    /// are unaffected.
    pub stateless: bool,
    /// The HTTP path the server mounts under. Empty means [`DEFAULT_BASE_PATH`].
    pub base_path: String,
    /// The proto-derived default path. When set it wins over `base_path`, as in Go.
    pub generated_base_path: String,
    /// HTTP headers forwarded into gRPC metadata.
    pub header_mappings: Vec<HeaderMapping>,
    /// Where a health probe route is mounted, when `health_check` is set.
    pub health_check_path: String,
    /// The check the health route runs; see [`super::grpc_health_check`].
    pub health_check: Option<HealthCheck>,
    /// Wrapped around every tool call.
    pub interceptors: Interceptors,
}

impl std::fmt::Debug for ServerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerConfig")
            .field("name", &self.name)
            .field("version", &self.version)
            .field("transports", &self.resolved_transports())
            .field("addr", &self.addr)
            .field("stateless", &self.stateless)
            .field("base_path", &self.resolved_base_path())
            .finish_non_exhaustive()
    }
}

impl ServerConfig {
    /// `transports`, else `transport`, else streamable HTTP — Go's `resolveTransports`.
    pub fn resolved_transports(&self) -> Vec<Transport> {
        if !self.transports.is_empty() {
            return self.transports.clone();
        }
        vec![self.transport.unwrap_or_default()]
    }

    /// The path the server mounts at: the generated one, else `base_path`, else the default.
    pub fn resolved_base_path(&self) -> String {
        if !self.generated_base_path.is_empty() {
            self.generated_base_path.clone()
        } else if !self.base_path.is_empty() {
            self.base_path.clone()
        } else {
            DEFAULT_BASE_PATH.to_string()
        }
    }

    /// Hosts `factory`'s handlers as a [`Service`] with this config's settings.
    pub fn service<H, F>(&self, factory: F) -> Service<H>
    where
        H: ServerHandler,
        F: Fn() -> H + Send + Sync + 'static,
    {
        let mut service = Service::new(factory)
            .transports(self.resolved_transports())
            .base_path(self.resolved_base_path())
            .headers(self.header_mappings.clone())
            .interceptors(self.interceptors.clone())
            .addr(listen_addr(&self.addr));
        if self.stateless {
            service = service.stateless();
        }
        if let Some(check) = &self.health_check {
            let path = if self.health_check_path.is_empty() {
                DEFAULT_HEALTH_CHECK_PATH
            } else {
                &self.health_check_path
            };
            service = service.health_check(path, Arc::clone(check));
        }
        service
    }
}

/// `addr` as a bindable `host:port`: Go's `:8080` binds every interface.
fn listen_addr(addr: &str) -> String {
    let addr = if addr.is_empty() { DEFAULT_ADDR } else { addr };
    match addr.strip_prefix(':') {
        Some(port) => format!("{}:{port}", crate::DEFAULT_HOST),
        None => addr.to_string(),
    }
}

/// Splits a comma-separated transport list, as `MCP_TRANSPORT` carries it.
///
/// Like Go's `ParseTransports` it only splits and trims; a name this runtime does not speak
/// is reported by the start, not here. Unlike Go the result is typed, so an unknown name is
/// an error from this call instead.
///
/// # Errors
///
/// A name that is not a transport.
pub fn parse_transports(list: &str) -> Result<Vec<Transport>, UnsupportedTransport> {
    list.split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::parse)
        .collect()
}

/// Where an MCP server answers, as a client should be told.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    /// `stdio`, `http` or `https`.
    pub protocol: String,
    /// `stdio`, `streamable-http` or `sse`.
    pub transport: String,
    /// The full URL; empty for stdio.
    pub url: String,
}

/// The effective base path: `generated_default` unless the config names one.
pub fn resolve_base_path(config: &ServerConfig, generated_default: &str) -> String {
    if config.base_path.is_empty() {
        generated_default.to_string()
    } else {
        config.base_path.clone()
    }
}

/// The generated default even when the config names a path, for when the proto-derived path
/// must win.
pub fn prefer_generated_base_path(generated_default: &str, _config: &ServerConfig) -> String {
    generated_default.to_string()
}

/// The endpoint a config serves, reading `MCP_SERVER_HOST`, `MCP_SERVER_PORT` and
/// `MCP_SERVER_TLS=true` from the environment as Go does — the advertised address past a
/// proxy or a container boundary.
pub fn server_endpoint(config: &ServerConfig) -> Endpoint {
    server_endpoint_with(config, |name| std::env::var(name).ok())
}

/// [`server_endpoint`] reading its overrides from `env` instead of the process environment.
pub fn server_endpoint_with(
    config: &ServerConfig,
    env: impl Fn(&str) -> Option<String>,
) -> Endpoint {
    let transports = config.resolved_transports();
    let has_http = transports.iter().any(Transport::listens);
    if !has_http {
        return Endpoint {
            protocol: "stdio".into(),
            transport: Transport::Stdio.as_str().into(),
            url: String::new(),
        };
    }
    let transport = transports
        .iter()
        .find(|t| t.listens())
        .copied()
        .unwrap_or_default();

    let addr = if config.addr.is_empty() {
        DEFAULT_ADDR
    } else {
        &config.addr
    };
    // HTTP over a Unix socket, written as the `http+unix` URL clients that speak it accept:
    // the socket's path percent-encoded where a host would be.
    if let Some(path) = crate::listen::unix_path(addr) {
        return Endpoint {
            protocol: "http+unix".into(),
            transport: transport.as_str().into(),
            url: format!(
                "http+unix://{}{}",
                crate::listen::unix_authority(path),
                config.resolved_base_path()
            ),
        };
    }
    let mut host = env("MCP_SERVER_HOST").unwrap_or_default();
    let mut port = env("MCP_SERVER_PORT").unwrap_or_default();
    if host.is_empty() || port.is_empty() {
        let (found_host, found_port) = match addr.strip_prefix(':') {
            Some(port) => ("localhost", port),
            None => match addr.split_once(':') {
                Some((h, p)) if !p.is_empty() => (h, p),
                _ => ("localhost", "8080"),
            },
        };
        if host.is_empty() {
            host = found_host.to_string();
        }
        if port.is_empty() {
            port = found_port.to_string();
        }
    }
    let protocol = if env("MCP_SERVER_TLS").as_deref() == Some("true") {
        "https"
    } else {
        "http"
    };
    Endpoint {
        protocol: protocol.into(),
        transport: transport.as_str().into(),
        url: format!("{protocol}://{host}:{port}{}", config.resolved_base_path()),
    }
}

/// Serves `factory`'s handlers per `config` until `cancel` fires, then drains — Go's
/// `StartServer`, for a process that runs one MCP server and nothing else.
///
/// # Errors
///
/// The listener could not be bound, or the server failed to start.
pub async fn start_server<H, F>(
    cancel: CancellationToken,
    config: &ServerConfig,
    factory: F,
) -> crate::Result<()>
where
    H: ServerHandler,
    F: Fn() -> H + Send + Sync + 'static,
{
    let service = config.service(factory);
    let runtime = Runtime::new(Config {
        identity: crate::Identity {
            name: config.name.clone(),
            version: config.version.clone(),
            ..Default::default()
        },
        ..Default::default()
    })
    .register(service);
    tracing::debug!(
        endpoint = %server_endpoint(config).url,
        "mcp: starting a standalone server"
    );
    runtime.serve(cancel).await
}

/// A factory that can fail, as [`Service::try_new`] takes.
pub(crate) type TryFactory<H> = Arc<dyn Fn() -> Result<H, io::Error> + Send + Sync>;
