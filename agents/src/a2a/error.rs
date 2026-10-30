//! Turning an agent's own failure into the event that reports it, and the errors this runtime
//! refuses to start on.
//!
//! The text matches Go's on the wire: a status code is named the way grpc-go's
//! `codes.Code.String()` names it (`PermissionDenied`, `Canceled`), not the `SCREAMING_SNAKE`
//! form the MCP payloads use. A client reading A2A failures from both runtimes sees one
//! format.

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
    #[error("a2a: the gRPC transport requires ServerConfig::grpc_routes")]
    NoGrpcRoutes,

    /// JSON-RPC and REST were both enabled under a base path of `/`, where the exact route
    /// and the subtree route would be the same pattern.
    #[error(r#"a2a: JSON-RPC and REST cannot share a base path of "/"; give JSON-RPC its own"#)]
    BasePathConflict,
}

/// Turns the outcome of an agent's own work into the event that reports it — a terminal
/// status update carrying a message a client can read.
///
/// `None` is success and reports completed, as Go's `HandleError(ctx, nil)` does. A gRPC
/// status keeps its code, because the code is the part a caller can act on: `NotFound` and
/// `Unavailable` ask for different things from whoever sent the request, and flattening both
/// to "failed" throws that away. The code arrives as the message's leading text rather than a
/// separate field, since the protocol has no place for a transport's status.
///
/// The state follows the code. Cancellation reports cancelled rather than failed, because a
/// task the client stopped did not fail; an unauthenticated or permission-denied call reports
/// rejected, which is what the protocol says about work a server declined to do; everything
/// else — a status carrying `Ok` included, since it was still returned as an error — is
/// failed.
///
/// Every state this resolves to is terminal, so emitting the event ends the execution.
pub fn handle_error<'a>(
    ctx: &ExecutorContext,
    status: impl Into<Option<&'a tonic::Status>>,
) -> StreamResponse {
    let Some(status) = status.into() else {
        return status_event(ctx, TaskState::Completed, None);
    };
    let state = if status.code() == tonic::Code::Unknown {
        TaskState::Failed
    } else {
        state_for_code(status.code())
    };
    status_event(
        ctx,
        state,
        Some(agent_message(vec![text_part(error_text(status))])),
    )
}

/// Maps a gRPC status code onto the task state that describes what happened to the work.
///
/// It is only ever asked about an error, so `Ok` maps to failed like any other code that is
/// not cancellation or a refusal — success is the absence of a status, not a status.
pub fn state_for_code(code: tonic::Code) -> TaskState {
    match code {
        tonic::Code::Cancelled => TaskState::Canceled,
        tonic::Code::Unauthenticated | tonic::Code::PermissionDenied => TaskState::Rejected,
        _ => TaskState::Failed,
    }
}

/// Renders a status the way [`handle_error`] puts it on the wire, and `None` as empty.
///
/// Exported for agents that build their own failure event but want the message to read the
/// same as every other one. An `Unknown` code carries no information a reader can act on, so
/// it is left off rather than prefixed.
pub fn error_text<'a>(status: impl Into<Option<&'a tonic::Status>>) -> String {
    let Some(status) = status.into() else {
        return String::new();
    };
    if status.code() == tonic::Code::Unknown {
        return status.message().to_string();
    }
    format!("{}: {}", code_name(status.code()), status.message())
}

/// A gRPC code named as grpc-go's `codes.Code.String()` names it.
///
/// It is not [`crate::shared::status_name`] on purpose: that is the canonical
/// `google.rpc.Code` spelling MCP payloads carry, and A2A's Go runtime writes the CamelCase
/// one into message text — including Go's single-`l` `Canceled`.
pub fn code_name(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::Ok => "OK",
        tonic::Code::Cancelled => "Canceled",
        tonic::Code::Unknown => "Unknown",
        tonic::Code::InvalidArgument => "InvalidArgument",
        tonic::Code::DeadlineExceeded => "DeadlineExceeded",
        tonic::Code::NotFound => "NotFound",
        tonic::Code::AlreadyExists => "AlreadyExists",
        tonic::Code::PermissionDenied => "PermissionDenied",
        tonic::Code::ResourceExhausted => "ResourceExhausted",
        tonic::Code::FailedPrecondition => "FailedPrecondition",
        tonic::Code::Aborted => "Aborted",
        tonic::Code::OutOfRange => "OutOfRange",
        tonic::Code::Unimplemented => "Unimplemented",
        tonic::Code::Internal => "Internal",
        tonic::Code::Unavailable => "Unavailable",
        tonic::Code::DataLoss => "DataLoss",
        tonic::Code::Unauthenticated => "Unauthenticated",
    }
}
