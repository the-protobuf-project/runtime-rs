//! Serving an agent: standalone on a listener of its own, or mounted into what a runtime
//! handed it. Both go through [`start_server`], so the runtime only decides *where*.
//!
//! Two places differ from Go because the router does:
//!
//! - Go mounts REST as `http.StripPrefix(prefix, rest)` on the `prefix/` subtree. axum's
//!   [`Router::nest`] is that same strip-and-delegate, so REST and JSON-RPC nest together
//!   under the base path. At a base path of `/` there is no prefix to strip and axum refuses
//!   to nest at the root, so the routers are merged instead.
//! - Go asks `http.ServeMux` which pattern answers the card path before mounting it, because
//!   a duplicate registration panics. axum panics on overlapping routes too and offers no
//!   lookup, so [`card_path_taken`] probes the router with a method nothing serves: a path
//!   that is routed answers `405`, an unrouted one does not.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::Request;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use futures::FutureExt;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use tower_http::timeout::TimeoutLayer;

use a2a_server::{AgentExecutor, StaticAgentCard};

use crate::agents::{Endpoint as RuntimeEndpoint, Placement, Protocol, Ready, Requirements};
use crate::error::ServiceError;
use crate::mux::Mux;
use crate::shared::with_header_forwarding;

use super::config::ServerConfig;
use super::endpoint::server_endpoints;
use super::error::Error;
use super::grpc::register_grpc;
use super::handler::{Handler, new_request_handler};
use super::service::{Inner, Service};
use super::{AGENT_CARD_PATH, Transport, build_card};

/// Bounds the graceful drain of a listener [`start_server`] owns once it is cancelled.
pub const SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Serves `cfg`'s public card at [`AGENT_CARD_PATH`].
///
/// A host sharing one router across several agents mounts exactly one of these itself, since
/// the well-known path admits one card per host.
pub fn agent_card_handler(cfg: &ServerConfig) -> Router {
    a2a_server::agent_card::agent_card_router(Arc::new(StaticAgentCard::new(build_card(cfg))))
}

/// Serves the agent over every configured transport and returns when `cancel` fires.
///
/// What it owns depends on the config. With [`ServerConfig::mux`] set it mounts handlers and
/// nothing else — the host opened the listener and will close it. Without one it binds
/// [`ServerConfig::addr`] itself, serves the card there whatever
/// [`ServerConfig::serve_agent_card`] says, and drains on the way out, returning `Ok` for an
/// ordinary shutdown. The gRPC transport is never owned either way: it registers on
/// [`ServerConfig::grpc_routes`] and that server's lifecycle is the caller's.
///
/// `cfg` is taken by value, so a caller reusing one config across several servers never sees
/// another server's resolved state written back into it — the reason Go copies it.
pub async fn start_server(
    cancel: CancellationToken,
    mut cfg: ServerConfig,
    executor: Arc<dyn AgentExecutor>,
) -> Result<(), ServiceError> {
    let transports = cfg.resolved_transports();
    if transports.is_empty() {
        return Err(Error::NoTransports.into());
    }
    cfg.addr = cfg.resolved_addr();
    cfg.base_path = cfg.resolved_base_path();
    cfg.transports = transports;
    cfg.generated_base_path = String::new();

    let owns_mux = cfg.mux.is_none();
    if owns_mux {
        cfg.serve_agent_card = true;
    }

    let handler = new_request_handler(&cfg, executor);

    if cfg.has(Transport::Grpc) {
        let Some(routes) = &cfg.grpc_routes else {
            return Err(Error::NoGrpcRoutes.into());
        };
        register_grpc(routes, Arc::clone(&handler));
    }

    let serve_jsonrpc = cfg.has(Transport::JsonRpc);
    let serve_rest = cfg.has(Transport::Rest);
    // At "/" the exact route JSON-RPC wants and the subtree REST wants are the same pattern,
    // and whichever mounts second is the one that never answers.
    if serve_jsonrpc && serve_rest && cfg.base_path == "/" {
        return Err(Error::BasePathConflict.into());
    }

    if !serve_jsonrpc && !serve_rest {
        // gRPC only: everything is on the caller's server already and there is nothing here
        // to listen on. Stay alive so the caller's lifetime still governs this one.
        tracing::debug!(name = %cfg.name, "a2a: gRPC only, nothing to mount over HTTP");
        notify_ready(&cfg);
        cancel.cancelled().await;
        return Ok(());
    }

    let mux = cfg.mux.clone().unwrap_or_default();
    mount_http(&cfg, &mux, &handler, serve_jsonrpc, serve_rest);
    notify_ready(&cfg);

    if !owns_mux {
        cancel.cancelled().await;
        return Ok(());
    }
    serve_owned(&cfg, mux.router(), cancel).await
}

/// Mounts the HTTP transports and, unless something already answers there, the card.
fn mount_http(
    cfg: &ServerConfig,
    mux: &Mux,
    handler: &Arc<Handler>,
    serve_jsonrpc: bool,
    serve_rest: bool,
) {
    // JSON-RPC claims only `POST /`, and REST claims named sub-paths, so the two merge
    // without collision and mount together under one base path.
    let mut protocol_router = Router::new();
    if serve_jsonrpc {
        protocol_router =
            protocol_router.merge(a2a_server::jsonrpc::jsonrpc_router(Arc::clone(handler)));
    }
    if serve_rest {
        protocol_router = protocol_router.merge(a2a_server::rest::rest_router(Arc::clone(handler)));
    }
    protocol_router = with_header_forwarding(protocol_router, cfg.header_mappings.clone());
    if cfg.transport_options.recover_panics {
        protocol_router = protocol_router.layer(axum::middleware::from_fn(recover_panics));
    }

    let card = cfg.serve_agent_card.then(|| agent_card_handler(cfg));
    let base_path = cfg.base_path.clone();
    let name = cfg.name.clone();
    mux.mount(move |router| {
        let mut router = if base_path == "/" {
            router.merge(protocol_router)
        } else {
            router.nest(&base_path, protocol_router)
        };
        if let Some(card) = card {
            // Probed under the same lock the mount holds, so two agents racing for the path
            // cannot both see it free.
            if card_path_taken(&router) {
                tracing::debug!(%name, "a2a: card path already served on this mux; not mounting");
            } else {
                router = router.merge(card);
            }
        }
        router
    });
    tracing::debug!(name = %cfg.name, base_path = %cfg.base_path, serve_jsonrpc, serve_rest, "a2a: HTTP transports mounted");
}

/// Whether something already answers at [`AGENT_CARD_PATH`] on `router`.
///
/// The well-known path admits one card per host by protocol, so two agents on one router
/// cannot both have it — and axum answers an overlapping route with a panic. Probing first
/// makes the rule "the first agent to mount publishes the card" instead of "the second agent
/// kills the process".
///
/// The probe uses a method no route serves, so no handler runs: a routed path answers
/// `405 Method Not Allowed`, an unrouted one falls through to the fallback. A probe that does
/// not resolve at once counts as taken, since skipping the card is recoverable and a panic is
/// not.
pub(super) fn card_path_taken(router: &Router) -> bool {
    let Ok(method) = http::Method::from_bytes(b"A2A-PROBE") else {
        return true;
    };
    let Ok(probe) = Request::builder()
        .method(method)
        .uri(AGENT_CARD_PATH)
        .body(Body::empty())
    else {
        return true;
    };
    match router.clone().oneshot(probe).now_or_never() {
        Some(Ok(response)) => response.status() == http::StatusCode::METHOD_NOT_ALLOWED,
        _ => true,
    }
}

/// Turns a handler panic into a `500`, for [`super::TransportOptions::recover_panics`].
async fn recover_panics(request: Request, next: Next) -> Response {
    match AssertUnwindSafe(next.run(request)).catch_unwind().await {
        Ok(response) => response,
        Err(_) => {
            tracing::error!("a2a: handler panicked; answering 500");
            http::StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// Calls [`ServerConfig::on_ready`], if set.
fn notify_ready(cfg: &ServerConfig) {
    if let Some(on_ready) = &cfg.on_ready {
        on_ready(cfg);
    }
}

/// Binds the listener [`start_server`] owns, serves `router` on it, and drains it on cancel.
async fn serve_owned(
    cfg: &ServerConfig,
    mut router: Router,
    cancel: CancellationToken,
) -> Result<(), ServiceError> {
    // A TCP address, Go's ":9000" meaning every interface, or a Unix socket's `unix:PATH`.
    let bind = cfg.addr.clone();
    let listener = crate::listen::Bound::bind(&bind).await?;

    // net/http bounds reads and writes separately; hyper does not split them, so the larger
    // bound covers the whole exchange and neither is tightened silently.
    if let Some(limit) = cfg.read_timeout.max(cfg.write_timeout) {
        router = router.layer(TimeoutLayer::with_status_code(
            http::StatusCode::REQUEST_TIMEOUT,
            limit,
        ));
    }

    tracing::debug!(addr = %bind, name = %cfg.name, "a2a: serving on an owned listener");
    let drain = CancellationToken::new();
    let stop = drain.clone();
    let mut serving = tokio::spawn(async move {
        listener
            .serve(router, async move { stop.cancelled().await })
            .await
    });

    tokio::select! {
        served = &mut serving => return Ok(served??),
        () = cancel.cancelled() => {}
    }
    // The drain has to outlive the cancellation that triggered it, or it would be cut off
    // before a single in-flight request finished.
    drain.cancel();
    if tokio::time::timeout(SHUTDOWN_TIMEOUT, &mut serving)
        .await
        .is_err()
    {
        tracing::warn!(addr = %bind, "a2a: drain timed out; aborting the listener");
        serving.abort();
    }
    Ok(())
}

#[async_trait::async_trait]
impl crate::Service for Service {
    fn protocol(&self) -> Protocol {
        Protocol::A2a
    }

    /// Which of the two it asks for depends on the transports: the gRPC binding registers on
    /// the runtime's registry and needs no listener of its own, and an agent serving only that
    /// one should not cause a port to be bound.
    fn requires(&self) -> Requirements {
        Requirements {
            addr: self.addr.clone(),
            http: self.transports.iter().any(|t| t.listens()),
            grpc: self.transports.contains(&Transport::Grpc),
        }
    }

    /// Hands [`start_server`] — or the registered [`super::ServiceFunc`] — a config built from
    /// the placement and lets it mount, which is the same path a standalone agent takes.
    async fn serve(
        &self,
        cancel: CancellationToken,
        placement: Placement,
        ready: Arc<Ready>,
    ) -> Result<(), ServiceError> {
        // Go lets an explicitly empty list fall through to JSON-RPC at serve time, after
        // `requires` already told the runtime no listener was needed — a JSON-RPC mount
        // nobody can reach. Refusing here is the failure that sentence describes.
        if self.transports.is_empty() {
            return Err(Error::NoTransports.into());
        }
        let mut cfg = self.config_for(&placement);
        cfg.on_ready = Some(Arc::new(move |resolved: &ServerConfig| {
            ready.ready(
                server_endpoints(resolved)
                    .into_iter()
                    .map(|endpoint| RuntimeEndpoint {
                        protocol: Protocol::A2a,
                        transport: endpoint.transport.as_str().to_string(),
                        url: endpoint.url,
                        detail: endpoint.card_url,
                    })
                    .collect(),
            );
        }));

        match &self.inner {
            Inner::Func(serve) => serve(cancel, cfg).await,
            Inner::Agent(executor) => start_server(cancel, cfg, Arc::clone(executor)).await,
        }
    }
}
