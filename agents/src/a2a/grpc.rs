//! The gRPC binding: A2A as a tonic service, registered on the routes the host already serves.
//!
//! Go registers `a2agrpc.NewHandler(handler)` on a `*grpc.Server`. The Rust SDK ships the same
//! piece — `a2a-grpc`'s `GrpcHandler`, implementing the `lf.a2a.v1.A2AService` server that
//! `a2a-pb` generates from the protocol's own `a2a.proto` — so nothing is vendored or
//! generated here. What this adds is where it goes: onto [`GrpcRoutes`], beside every other
//! service on the process's gRPC port.

use std::sync::Arc;

use a2a_grpc::GrpcHandler;
use a2a_pb::proto::a2a_service_server::{A2aServiceServer, SERVICE_NAME};

use crate::mux::GrpcRoutes;

use super::handler::Handler;

/// The fully-qualified name [`super::Transport::Grpc`] registers under.
///
/// A host listing the services on its server needs it, and taking it from the generated
/// service means it cannot drift from what was actually registered.
pub const GRPC_SERVICE_NAME: &str = SERVICE_NAME;

/// The generated gRPC wire types and the tonic client, for a caller that dials an agent's gRPC
/// binding directly rather than through an A2A client.
pub use a2a_pb::proto as pb;

/// Registers `handler` as the A2A gRPC service on `routes`.
///
/// The routes must still be un-served: tonic takes them by value when a server starts, so a
/// registration after that point is one nobody will ever route to.
pub fn register_grpc(routes: &GrpcRoutes, handler: Arc<Handler>) {
    routes.register(|builder| {
        builder.add_service(A2aServiceServer::new(GrpcHandler::new(handler)));
    });
    tracing::debug!(service = GRPC_SERVICE_NAME, "a2a: gRPC binding registered");
}
