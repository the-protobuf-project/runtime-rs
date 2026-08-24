//! Turning a failed gRPC call into the error result an MCP client can read.

use rmcp::model::CallToolResult;
use serde::Serialize;

use crate::mcp::tool::error_result;

/// A gRPC failure in the shape an MCP client receives it.
///
/// The code is carried as its name rather than its number because the payload is read by a
/// model as often as by a program, and `NOT_FOUND` needs no lookup table.
#[derive(Debug, Clone, Serialize)]
pub struct GrpcError {
    /// The canonical status code name, e.g. `NOT_FOUND`.
    pub code: String,
    /// The human-readable message from the status.
    pub message: String,
    /// Any structured details, omitted when empty.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub details: Vec<serde_json::Value>,
}

/// Converts a gRPC status into an MCP tool error result.
///
/// Use it where a generated tool handler's gRPC call fails, so the status code survives into
/// the payload instead of being flattened to a message:
///
/// ```
/// use agents::mcp::handle_error;
///
/// let status = tonic::Status::not_found("no such todo");
/// let result = handle_error(&status);
/// assert_eq!(result.is_error, Some(true));
/// ```
pub fn handle_error(status: &tonic::Status) -> CallToolResult {
    let error = GrpcError {
        code: crate::mcp::status_name(status.code()).to_string(),
        message: status.message().to_string(),
        details: Vec::new(),
    };
    match serde_json::to_string(&error) {
        Ok(payload) => error_result(payload),
        // The message is the part a reader needs; losing the envelope beats losing both.
        Err(_) => error_result(error.message),
    }
}

/// The canonical `SCREAMING_SNAKE` name for a gRPC code, as MCP error payloads carry it.
pub fn status_name(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::Ok => "OK",
        tonic::Code::Cancelled => "CANCELLED",
        tonic::Code::Unknown => "UNKNOWN",
        tonic::Code::InvalidArgument => "INVALID_ARGUMENT",
        tonic::Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        tonic::Code::NotFound => "NOT_FOUND",
        tonic::Code::AlreadyExists => "ALREADY_EXISTS",
        tonic::Code::PermissionDenied => "PERMISSION_DENIED",
        tonic::Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        tonic::Code::FailedPrecondition => "FAILED_PRECONDITION",
        tonic::Code::Aborted => "ABORTED",
        tonic::Code::OutOfRange => "OUT_OF_RANGE",
        tonic::Code::Unimplemented => "UNIMPLEMENTED",
        tonic::Code::Internal => "INTERNAL",
        tonic::Code::Unavailable => "UNAVAILABLE",
        tonic::Code::DataLoss => "DATA_LOSS",
        tonic::Code::Unauthenticated => "UNAUTHENTICATED",
    }
}
