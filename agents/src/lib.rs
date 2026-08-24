//! Serves the protocols a model-driven client speaks to a service: build one [`Runtime`],
//! register the protocols it should answer, start it.
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use agents::{Config, Endpoint, Identity, Placement, Protocol, Ready, Requirements, Runtime, Service};
//! # use agents::error::ServiceError;
//! # use tokio_util::sync::CancellationToken;
//! # struct Tools;
//! # #[async_trait::async_trait]
//! # impl Service for Tools {
//! #     fn protocol(&self) -> Protocol { Protocol::Mcp }
//! #     fn requires(&self) -> Requirements { Requirements { http: true, ..Default::default() } }
//! #     async fn serve(&self, cancel: CancellationToken, _p: Placement, ready: Arc<Ready>) -> Result<(), ServiceError> {
//! #         ready.ready(vec![]);
//! #         cancel.cancelled().await;
//! #         Ok(())
//! #     }
//! # }
//! # async fn example() -> agents::Result<()> {
//! let runtime = Runtime::new(Config {
//!     identity: Identity {
//!         name: "my-service".into(),
//!         version: "1.0.0".into(),
//!         ..Default::default()
//!     },
//!     port: 9000,
//!     ..Default::default()
//! })
//! .register(Tools);
//!
//! runtime.serve(CancellationToken::new()).await
//! # }
//! ```
//!
//! That is two protocols on one port, each under its own base path, with one shutdown that
//! drains both.
//!
//! # Why one object
//!
//! The protocols are unrelated — one is a model reaching your tools, the other is an agent
//! delegating a task — but a process serving both has to make the same four decisions
//! either way: what it calls itself, where it listens, what owns the listener, and what
//! drains it on the way out. Made twice, they drift. The identity on an A2A card and the
//! identity an MCP client is told are the same string here because they came from the same
//! [`Config`], not because someone kept them in step.
//!
//! A runtime is where those four decisions live, and nothing else. The protocol crates are
//! not layered under it: [`Runtime::register`] takes a [`Service`], and each crate builds
//! one from its own vocabulary.
//!
//! # Placement
//!
//! A service says what it needs with [`Requirements`] and is handed a [`Placement`]. It does
//! not choose where it sits, which is what lets the runtime put two protocols behind one
//! listener without either knowing about the other.
//!
//! The default is to share: services naming no address of their own answer on
//! [`Config::host`] and [`Config::port`] together. A service that names one gets a listener
//! and router of its own, and services naming the same address share with each other. That
//! is how the `grpc` crate's hybrid server keeps MCP and A2A on separate ports while still
//! driving a single runtime.
//!
//! Not every service listens. A2A serving only its gRPC binding registers on
//! [`Config::grpc_routes`] and mounts nothing, and the runtime opens no listener it would
//! have nothing to put behind.
//!
//! # Starting
//!
//! [`Runtime::start`] mounts every service, then opens listeners, then returns —
//! non-blocking, for a host with its own lifecycle. [`Runtime::serve`] is start, a wait on
//! the token, and a drain, for a process that does nothing else.
//!
//! The order inside start is not an implementation detail. Handlers are registered before
//! any port accepts, so nothing answers 404 to whoever got there first; and a gRPC binding
//! must be registered before its server reads its routes out, because tonic takes them by
//! value when it starts.
//!
//! # Differences from the Go original
//!
//! Three, all forced by the ecosystem rather than chosen:
//!
//! - Go's `context.Context` is a [`tokio_util::sync::CancellationToken`].
//! - Go's `*http.ServeMux` is mutated in place; [`axum::Router`] is consumed by its own
//!   combinators, so [`Mux`] restores the shape several services need to mount onto one
//!   router.
//! - Go's `*grpc.Server` accepts registrations until it serves; tonic's equivalent seam is
//!   [`tonic::service::RoutesBuilder`], which [`GrpcRoutes`] wraps for the same reason.

#![warn(missing_docs)]

mod agents;
mod config;
pub mod error;
mod lifecycle;
mod mux;
mod placement;
mod runtime;
pub mod shared;
mod start;

#[cfg(feature = "a2a")]
pub mod a2a;
#[cfg(feature = "mcp")]
pub mod mcp;

pub use agents::{
    Config, DEFAULT_HOST, DEFAULT_READY_TIMEOUT, Endpoint, GrpcRoutes, Identity, Mux, Placement,
    Protocol, Ready, Requirements, SHUTDOWN_TIMEOUT, Service,
};
pub use error::{Error, Result};
pub use runtime::Runtime;
