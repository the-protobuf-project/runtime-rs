//! Turning a failed gRPC call into the error result an MCP client can read.
//!
//! The payload is a wire contract clients of the Go runtime already parse, so it is matched
//! field for field: the code as Go's `codes.Code.String()` spells it, and the status details
//! as Go's `encoding/json` renders the generated `errdetails` structs.

use rmcp::model::CallToolResult;
use serde::Serialize;
use serde_json::{Value, json};
use tonic_types::{ErrorDetail, StatusExt};

use crate::mcp::tool::error_result;

/// A gRPC failure in the shape an MCP client receives it.
///
/// The code is carried as its name rather than its number because the payload is read by a
/// model as often as by a program, and `NotFound` needs no lookup table.
#[derive(Debug, Clone, Serialize)]
pub struct GrpcError {
    /// The status code as Go's `codes.Code.String()` spells it, e.g. `PermissionDenied` —
    /// see [`code_string`]. Not the `SCREAMING_SNAKE` of [`crate::shared::status_name`],
    /// because this is what Go puts on the wire.
    pub code: String,
    /// The human-readable message from the status.
    pub message: String,
    /// The status's structured details, omitted when empty.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub details: Vec<Value>,
}

/// The status code as Go's `codes.Code.String()` spells it — one `l` in `Canceled` included.
pub fn code_string(code: tonic::Code) -> &'static str {
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

/// One status detail as Go's `encoding/json` renders the generated struct: the proto field
/// names, with empty fields left out as `omitempty` leaves them.
fn detail_json(detail: ErrorDetail) -> Option<Value> {
    let rendered = match detail {
        ErrorDetail::ErrorInfo(info) => json!({
            "reason": info.reason, "domain": info.domain, "metadata": info.metadata,
        }),
        ErrorDetail::BadRequest(bad) => json!({
            "field_violations": bad.field_violations.into_iter().map(|violation| json!({
                "field": violation.field, "description": violation.description,
            })).collect::<Vec<_>>(),
        }),
        ErrorDetail::PreconditionFailure(failure) => json!({
            "violations": failure.violations.into_iter().map(|violation| json!({
                "type": violation.r#type, "subject": violation.subject,
                "description": violation.description,
            })).collect::<Vec<_>>(),
        }),
        ErrorDetail::QuotaFailure(failure) => json!({
            "violations": failure.violations.into_iter().map(|violation| json!({
                "subject": violation.subject, "description": violation.description,
            })).collect::<Vec<_>>(),
        }),
        ErrorDetail::RequestInfo(info) => json!({
            "request_id": info.request_id, "serving_data": info.serving_data,
        }),
        ErrorDetail::ResourceInfo(info) => json!({
            "resource_type": info.resource_type, "resource_name": info.resource_name,
            "owner": info.owner, "description": info.description,
        }),
        ErrorDetail::Help(help) => json!({
            "links": help.links.into_iter().map(|link| json!({
                "description": link.description, "url": link.url,
            })).collect::<Vec<_>>(),
        }),
        ErrorDetail::LocalizedMessage(message) => json!({
            "locale": message.locale, "message": message.message,
        }),
        ErrorDetail::DebugInfo(info) => json!({
            "stack_entries": info.stack_entries, "detail": info.detail,
        }),
        // A RetryInfo's delay is a nested Duration whose Go rendering no client reads.
        _ => return None,
    };
    let Value::Object(mut fields) = rendered else {
        return None;
    };
    fields.retain(|_, value| !is_empty(value));
    Some(Value::Object(fields))
}

/// What `omitempty` drops.
fn is_empty(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(text) => text.is_empty(),
        Value::Array(items) => items.is_empty(),
        Value::Object(fields) => fields.is_empty(),
        _ => false,
    }
}

/// Converts a gRPC status into an MCP tool error result.
///
/// Use it where a generated tool handler's gRPC call fails, so the status code and its
/// structured details survive into the payload instead of being flattened to a message:
///
/// ```
/// use agents::mcp::handle_error;
///
/// let status = tonic::Status::permission_denied("not yours");
/// let result = handle_error(&status);
/// assert_eq!(result.is_error, Some(true));
/// ```
pub fn handle_error(status: &tonic::Status) -> CallToolResult {
    let details = status
        .get_error_details_vec()
        .into_iter()
        .filter_map(detail_json)
        .collect();
    let error = GrpcError {
        code: code_string(status.code()).to_string(),
        message: status.message().to_string(),
        details,
    };
    match serde_json::to_string(&error) {
        Ok(payload) => error_result(payload),
        // The message is the part a reader needs; losing the envelope beats losing both.
        Err(_) => error_result(error.message),
    }
}

/// [`handle_error`] for any error, as Go's `HandleError` takes any `error`: a status found
/// along the source chain keeps its code, and anything else falls back to its text.
pub fn handle_any_error(error: &(dyn std::error::Error + 'static)) -> CallToolResult {
    let mut current = Some(error);
    while let Some(candidate) = current {
        if let Some(status) = candidate.downcast_ref::<tonic::Status>() {
            return handle_error(status);
        }
        current = candidate.source();
    }
    error_result(error.to_string())
}
