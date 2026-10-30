//! Everything needed to serve one agent, resolved in one place so the card cannot advertise an
//! endpoint the server is not serving.
//!
//! This is Go's `ServerConfig`, field for field where the ecosystem allows. Where it does not:
//!
//! - `Mux *http.ServeMux` and `GRPCServer *grpc.Server` are [`Mux`] and [`GrpcRoutes`], the
//!   crate's registries that accept registrations by mutation the way Go's objects do.
//! - `HandlerOptions []a2asrv.RequestHandlerOption` and `TransportOptions []a2asrv.TransportOption`
//!   are structs, because the Rust SDK configures its handler through constructor arguments
//!   and builder methods rather than variadic options. [`HandlerOptions`] and
//!   [`TransportOptions`] name what that SDK can actually vary.
//! - `OnReady func(*ServerConfig)` is an [`OnReady`] closure behind an `Arc`, so a config stays
//!   `Clone` — a host replaying one config across restarts needs that.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use a2a_server::{PushConfigStore, RequestHandler, TaskStore};

use crate::mux::{GrpcRoutes, Mux};
use crate::shared::HeaderMapping;

use super::endpoint::resolve_base_path;
use super::{Capabilities, Card, DEFAULT_HOST, DEFAULT_PORT, Provider, Skill, Transport};

/// Called with the resolved config once every transport is mounted and before the listener
/// opens, so a host can log or record where the agent ended up.
pub type OnReady = Arc<dyn Fn(&ServerConfig) + Send + Sync>;

/// Wraps the request handler every transport serves, for behaviour the config does not name:
/// call interceptors, authorisation, concurrency limits.
///
/// Go passes these as `a2asrv.RequestHandlerOption` values. The Rust SDK has no option type
/// for interception — its `InterceptedHandler` is not a handler — so the seam here is the
/// one that composes: a function from one handler to another, applied in order.
pub type HandlerLayer =
    Arc<dyn Fn(Arc<dyn RequestHandler>) -> Arc<dyn RequestHandler> + Send + Sync>;

/// What is passed through to the SDK's request handler.
///
/// The fields are the knobs the Rust SDK's `DefaultRequestHandler` exposes; anything it does
/// not is reachable through [`HandlerOptions::layers`].
#[derive(Clone, Default)]
pub struct HandlerOptions {
    /// Where tasks are kept. `None` means a fresh in-memory store per server, which is right
    /// until a task has to outlive the process.
    pub task_store: Option<Arc<dyn TaskStore>>,
    /// Where push notification configs are kept. Setting one turns push notifications on;
    /// without it the handler answers push calls with "not supported".
    pub push_config_store: Option<Arc<dyn PushConfigStore>>,
    /// Applied to the handler in order, innermost first, after the extended card is attached
    /// — so a layer guarding the extended card sees that call too.
    pub layers: Vec<HandlerLayer>,
}

impl fmt::Debug for HandlerOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HandlerOptions")
            .field("task_store", &self.task_store.is_some())
            .field("push_config_store", &self.push_config_store.is_some())
            .field("layers", &self.layers.len())
            .finish()
    }
}

/// What is passed through to the HTTP transport handlers.
///
/// Go's `a2asrv.TransportOption` covers two things, keep-alive and panic handling. The Rust
/// SDK's routers take no options at all, so only the one this crate can add from outside —
/// recovering from a panic — exists here; the SSE keep-alive interval is fixed by the SDK.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransportOptions {
    /// Turns a panic in a handler into a `500` response instead of a dropped connection, so
    /// a client learns the call failed rather than timing out on it.
    pub recover_panics: bool,
}

/// Everything needed to serve one agent.
///
/// The default is not usable on its own — an agent with no name and no skills is not
/// describable — but every field below the identity has a working default.
#[derive(Clone, Default)]
pub struct ServerConfig {
    /// The agent's name as clients see it on the card.
    pub name: String,
    /// A one-line description for the card.
    pub description: String,
    /// The agent's own version, not the protocol's.
    pub version: String,

    /// The single-transport form, for the common case. [`ServerConfig::transports`] wins
    /// when both are set; with neither the agent speaks [`Transport::JsonRpc`].
    pub transport: Option<Transport>,
    /// The bindings the agent answers on, in the order the card lists them — the first is
    /// the preferred one.
    pub transports: Vec<Transport>,

    /// The listen address for the HTTP transports, as `host:port`. Ignored when the agent
    /// speaks only gRPC, and when [`ServerConfig::mux`] is set — the host owns the listener
    /// then. Defaults to [`DEFAULT_HOST`]:[`DEFAULT_PORT`].
    pub addr: String,

    /// Where JSON-RPC mounts, and the prefix REST is served under. Defaults to
    /// [`super::DEFAULT_BASE_PATH`].
    pub base_path: String,
    /// The path a code generator derived from the agent's proto. It takes precedence over
    /// [`ServerConfig::base_path`]: a generated path is part of a contract clients already
    /// hold, a hand-set one is a preference.
    pub generated_base_path: String,

    /// The base URL clients should use, for an agent behind a proxy or inside a container.
    /// Without it the card advertises the listen address, which is wrong past any hop.
    pub public_url: String,

    /// The distinct things this agent can do, carried on the card verbatim.
    pub skills: Vec<Skill>,
    /// The optional protocol features supported — streaming, push notifications.
    pub capabilities: Capabilities,
    /// The content types skills accept unless a skill overrides them. Empty means
    /// [`super::DEFAULT_MODE`].
    pub default_input_modes: Vec<String>,
    /// The content types skills produce unless a skill overrides them. Empty means
    /// [`super::DEFAULT_MODE`].
    pub default_output_modes: Vec<String>,

    /// The organisation behind the agent, as card decoration.
    pub provider: Option<Provider>,
    /// Where the agent's documentation lives, as card decoration.
    pub documentation_url: Option<String>,
    /// An icon for the agent, as card decoration.
    pub icon_url: Option<String>,

    /// A card served as-is, with every card-shaped field above ignored. For a signed or
    /// registry-sourced card that is the only correct behaviour — rebuilding it would
    /// invalidate the signature.
    pub card: Option<Card>,

    /// Forwards HTTP headers into gRPC metadata for the executor.
    pub header_mappings: Vec<HeaderMapping>,

    /// Bounds reads on a listener [`super::start_server`] owns. `None` means no limit, which
    /// is what streaming needs.
    pub read_timeout: Option<Duration>,
    /// Bounds writes on a listener [`super::start_server`] owns. `None` means no limit.
    pub write_timeout: Option<Duration>,

    /// When set, the HTTP transports mount here instead of on a listener of their own, and
    /// [`super::start_server`] blocks until cancelled with the host owning the server.
    pub mux: Option<Mux>,
    /// Where [`Transport::Grpc`] registers. Required for that transport, unused by the
    /// others.
    pub grpc_routes: Option<GrpcRoutes>,

    /// Passed through to the SDK's request handler.
    pub handler_options: HandlerOptions,
    /// Passed through to the HTTP transport handlers.
    pub transport_options: TransportOptions,

    /// Mounts the public card at [`super::AGENT_CARD_PATH`]. Forced on when
    /// [`super::start_server`] owns its mux.
    ///
    /// The well-known path is singular per host, so on a shared mux the first agent to mount
    /// takes it and any later one silently does not — asking is a preference, not a claim.
    pub serve_agent_card: bool,

    /// Called once every transport is mounted and before the listener opens.
    pub on_ready: Option<OnReady>,
}

impl ServerConfig {
    /// The transport set this config asks for, in card order.
    pub(super) fn resolved_transports(&self) -> Vec<Transport> {
        if !self.transports.is_empty() {
            return self.transports.clone();
        }
        if let Some(transport) = self.transport {
            return vec![transport];
        }
        vec![Transport::JsonRpc]
    }

    /// Where JSON-RPC mounts, generated path first.
    pub(super) fn resolved_base_path(&self) -> String {
        resolve_base_path(self, "")
    }

    /// The listen address, with the defaults filled in.
    pub(super) fn resolved_addr(&self) -> String {
        if self.addr.is_empty() {
            format!("{DEFAULT_HOST}:{DEFAULT_PORT}")
        } else {
            self.addr.clone()
        }
    }

    /// Whether `transport` is among the configured transports.
    pub(super) fn has(&self, transport: Transport) -> bool {
        self.resolved_transports().contains(&transport)
    }
}

impl fmt::Debug for ServerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerConfig")
            .field("name", &self.name)
            .field("version", &self.version)
            .field("transports", &self.resolved_transports())
            .field("addr", &self.addr)
            .field("base_path", &self.base_path)
            .field("generated_base_path", &self.generated_base_path)
            .field("public_url", &self.public_url)
            .field("skills", &self.skills.len())
            .field("card", &self.card.is_some())
            .field("mux", &self.mux.is_some())
            .field("grpc_routes", &self.grpc_routes.is_some())
            .field("handler_options", &self.handler_options)
            .field("transport_options", &self.transport_options)
            .field("serve_agent_card", &self.serve_agent_card)
            .field("on_ready", &self.on_ready.is_some())
            .finish_non_exhaustive()
    }
}
