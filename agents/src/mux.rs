//! The two registries a service mounts itself into.
//!
//! Both exist for the same reason: Go hands around `*http.ServeMux` and `*grpc.Server`,
//! objects that accept registrations by mutation, and the tonic and axum equivalents are
//! consumed by their own combinators instead. These restore the shape the runtime needs —
//! several services registering into one thing that nobody owns.

use std::fmt;
use std::sync::{Arc, Mutex};

use axum::Router;
use tonic::service::{Routes, RoutesBuilder};

/// A router shared by every service placed at one address.
///
/// [`axum::Router`] is consumed by its own combinators, where Go's `*http.ServeMux` is
/// mutated in place. This wrapper restores the shape the runtime needs: several services
/// mounting onto one router, each under a path of its own, without any of them owning it.
#[derive(Clone, Default)]
pub struct Mux(Arc<Mutex<Router>>);

impl Mux {
    /// An empty router.
    pub fn new() -> Self {
        Self::default()
    }

    /// Wraps a router the host already built, for a [`Config::mux`] that mounts every
    /// sharing service onto the host's own server.
    pub fn from_router(router: Router) -> Self {
        Self(Arc::new(Mutex::new(router)))
    }

    /// Mounts handlers by replacing the router with whatever `f` returns.
    ///
    /// ```
    /// use agents::Mux;
    /// use axum::{Router, routing::get};
    ///
    /// let mux = Mux::new();
    /// mux.mount(|r| r.route("/mcp", get(|| async { "ok" })));
    /// ```
    pub fn mount<F>(&self, f: F)
    where
        F: FnOnce(Router) -> Router,
    {
        let mut guard = self.0.lock().expect("agents: mux poisoned");
        let current = std::mem::take(&mut *guard);
        *guard = f(current);
    }

    /// The router as it stands, for the runtime about to serve it.
    pub fn router(&self) -> Router {
        self.0.lock().expect("agents: mux poisoned").clone()
    }
}

impl fmt::Debug for Mux {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Mux").finish_non_exhaustive()
    }
}

/// The gRPC service registry a protocol with a gRPC binding registers on.
///
/// This is the counterpart of Go's `*grpc.Server` being handed around before it serves:
/// [`tonic::service::RoutesBuilder`] is the one piece of tonic that registers by mutation
/// rather than by consuming the server, which is what lets A2A's gRPC binding be a service
/// among the others.
#[derive(Clone, Default)]
pub struct GrpcRoutes(Arc<Mutex<RoutesBuilder>>);

impl GrpcRoutes {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a generated tonic service, taking the builder by `&mut` so several
    /// callers can add to one registry.
    pub fn register<F>(&self, f: F)
    where
        F: FnOnce(&mut RoutesBuilder),
    {
        let mut guard = self.0.lock().expect("agents: grpc routes poisoned");
        f(&mut guard);
    }

    /// The routes as they stand, for the server about to serve them.
    pub fn routes(&self) -> Routes {
        self.0
            .lock()
            .expect("agents: grpc routes poisoned")
            .clone()
            .routes()
    }
}

impl fmt::Debug for GrpcRoutes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GrpcRoutes").finish_non_exhaustive()
    }
}
