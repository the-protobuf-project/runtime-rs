//! The HTTP surface: one router, served over as many transports as are enabled.
//!
//! # What this does and does not own
//!
//! It owns listeners and TLS. It does **not** own transcoding — this module has no idea how a
//! JSON request becomes a gRPC call, and deliberately so. A generated HTTP/JSON handler is a
//! [`tower::Service`] over [`http::Request`], so it mounts here like any other service, and
//! this crate depends on no particular generator to say so.
//!
//! That is what lets the [`the-protobuf-project/http`] runtime plug in: its handler is exactly
//! that shape. It is also what lets it be replaced without touching this module.
//!
//! # One router, several listeners
//!
//! The router is built once and cloned per transport. An HTTP/3 listener is the same router
//! behind a QUIC socket, not a second implementation — which is the whole reason the surface
//! is a value rather than a server.
//!
//! ```no_run
//! # use grpc::http::Surface;
//! # use axum::Router;
//! # fn example(generated: Router) {
//! // A generated transcode router mounts like anything else.
//! let surface = Surface::new().mount(generated);
//! # let _ = surface;
//! # }
//! ```
//!
//! [`the-protobuf-project/http`]: https://github.com/the-protobuf-project/http

mod health;

pub mod h1;
#[cfg(feature = "http3")]
pub mod h3;

use axum::Router;

pub use health::health_router;

/// Everything this server answers over HTTP, before a listener is bound to it.
#[derive(Clone, Default)]
pub struct Surface {
    router: Router,
}

impl Surface {
    /// A surface carrying only `GET /health`.
    ///
    /// The health endpoint is here rather than left to the caller because a server that
    /// answers nothing else should still answer that: it is what a load balancer asks before
    /// it sends any real traffic.
    #[must_use]
    pub fn new() -> Self {
        Self {
            router: health_router(),
        }
    }

    /// A surface over a router the caller built entirely.
    #[must_use]
    pub fn from_router(router: Router) -> Self {
        Self { router }
    }

    /// Merges another router in — a generated HTTP/JSON handler, or anything else.
    ///
    /// Merging rather than nesting, because a generated route table already carries its own
    /// absolute paths: the templates came from `google.api.http`, and prefixing them here
    /// would move every endpoint away from what the API declares.
    #[must_use]
    pub fn mount(mut self, router: Router) -> Self {
        self.router = self.router.merge(router);
        self
    }

    /// Mounts a service under `path` and everything beneath it.
    ///
    /// For a handler that owns a subtree — an MCP endpoint, a metrics scrape — rather than a
    /// set of declared routes.
    #[must_use]
    pub fn mount_at<S>(mut self, path: &str, service: S) -> Self
    where
        S: tower::Service<
                axum::extract::Request,
                Error = std::convert::Infallible,
                Response = axum::response::Response,
            > + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        self.router = self.router.nest_service(path, service);
        self
    }

    /// Adds a [`tower`] layer over everything mounted so far.
    #[must_use]
    pub fn layer<L>(mut self, layer: L) -> Self
    where
        L: tower::Layer<axum::routing::Route> + Clone + Send + Sync + 'static,
        L::Service: tower::Service<axum::extract::Request, Error = std::convert::Infallible>
            + Clone
            + Send
            + Sync
            + 'static,
        <L::Service as tower::Service<axum::extract::Request>>::Response:
            axum::response::IntoResponse + 'static,
        <L::Service as tower::Service<axum::extract::Request>>::Future: Send + 'static,
    {
        self.router = self.router.layer(layer);
        self
    }

    /// The router, for a listener about to serve it.
    pub fn router(&self) -> Router {
        self.router.clone()
    }

    /// Consumes the surface, yielding its router.
    pub fn into_router(self) -> Router {
        self.router
    }
}

impl std::fmt::Debug for Surface {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Surface").finish_non_exhaustive()
    }
}
