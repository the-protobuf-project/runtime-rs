//! Turning an agent's own failure into the event that reports it, and the errors this runtime
//! refuses to start on.

use a2a::{StreamResponse, TaskState};
use a2a_server::ExecutorContext;

use super::executor::{agent_message, status_event, text_part};

/// What a misconfigured agent is refused with, before it serves anything.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    /// The config resolved to no transport at all, which would start a server nothing can
    /// reach.
    #[error("a2a: no transports configured")]
    NoTransports,

    /// The gRPC transport was asked for without a registry to register on.
    ///
    /// This runtime never opens a gRPC listener of its own: A2A is one service among the
    /// process's others, and giving it a private port would put it somewhere clients of the
    /// rest cannot see.
    #[error("a2a: the gRPC transport requires Config::grpc_routes")]
    NoGrpcRoutes,

    /// JSON-RPC and REST were both enabled under a base path of `/`, where the exact route
    /// and the subtree route would be the same pattern.
    #[error(r#"a2a: JSON-RPC and REST cannot share a base path of "/"; give JSON-RPC its own"#)]
    BasePathConflict,
}

/// Turns an error from an agent's own work into the event that reports it — a terminal status
/// update carrying a message a client can read.
///
/// A gRPC status keeps its code, because the code is the part a caller can act on: `NotFound`
/// and `Unavailable` ask for different things from whoever sent the request, and flattening
/// both to "failed" throws that away. The code arrives as the message's leading text rather
/// than a separate field, since the protocol has no place for a transport's status.
///
/// The state follows the code. Cancellation reports cancelled rather than failed, because a
/// task the client stopped did not fail; an unauthenticated or permission-denied call reports
/// rejected, which is what the protocol says about work a server declined to do; everything
/// else is failed.
///
/// Every state this resolves to is terminal, so emitting the event ends the execution.
pub fn handle_error(ctx: &ExecutorContext, status: &tonic::Status) -> StreamResponse {
    status_event(
        ctx,
        state_for_code(status.code()),
        Some(agent_message(vec![text_part(error_text(status))])),
    )
}

/// Maps a gRPC status code onto the task state that describes what happened to the work.
pub fn state_for_code(code: tonic::Code) -> TaskState {
    match code {
        tonic::Code::Ok => TaskState::Completed,
        tonic::Code::Cancelled => TaskState::Canceled,
        tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => TaskState::Rejected,
        _ => TaskState::Failed,
    }
}

/// Renders a status the way [`handle_error`] puts it on the wire.
///
/// Exported for agents that build their own failure event but want the message to read the
/// same as every other one. An `Unknown` code carries no information a reader can act on, so
/// it is left off rather than prefixed.
pub fn error_text(status: &tonic::Status) -> String {
    if status.code() == tonic::Code::Unknown {
        return status.message().to_string();
    }
    format!(
        "{}: {}",
        crate::shared::status_name(status.code()),
        status.message()
    )
}
