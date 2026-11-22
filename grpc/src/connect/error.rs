//! Connect's JSON error body, and the HTTP status each code carries.

use base64::Engine;
use base64::engine::general_purpose::STANDARD_NO_PAD;
use http::{HeaderMap, HeaderValue, StatusCode};
use prost::Message;
use tonic::Code;

/// The Connect name of a gRPC code, and the HTTP status a Connect error carries for it.
pub(super) fn connect_code(code: Code) -> (&'static str, StatusCode) {
    match code {
        Code::Cancelled => (
            "canceled",
            StatusCode::from_u16(499).expect("499 is a valid status"),
        ),
        Code::InvalidArgument => ("invalid_argument", StatusCode::BAD_REQUEST),
        Code::DeadlineExceeded => ("deadline_exceeded", StatusCode::GATEWAY_TIMEOUT),
        Code::NotFound => ("not_found", StatusCode::NOT_FOUND),
        Code::AlreadyExists => ("already_exists", StatusCode::CONFLICT),
        Code::PermissionDenied => ("permission_denied", StatusCode::FORBIDDEN),
        Code::ResourceExhausted => ("resource_exhausted", StatusCode::TOO_MANY_REQUESTS),
        Code::FailedPrecondition => ("failed_precondition", StatusCode::BAD_REQUEST),
        Code::Aborted => ("aborted", StatusCode::CONFLICT),
        Code::OutOfRange => ("out_of_range", StatusCode::BAD_REQUEST),
        Code::Unimplemented => ("unimplemented", StatusCode::NOT_IMPLEMENTED),
        Code::Internal => ("internal", StatusCode::INTERNAL_SERVER_ERROR),
        Code::Unavailable => ("unavailable", StatusCode::SERVICE_UNAVAILABLE),
        Code::DataLoss => ("data_loss", StatusCode::INTERNAL_SERVER_ERROR),
        Code::Unauthenticated => ("unauthenticated", StatusCode::UNAUTHORIZED),
        Code::Ok | Code::Unknown => ("unknown", StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// A Connect error response. `details` is the encoded `google.rpc.Status` from
/// `grpc-status-details-bin`; each of its details is sent as its type name, without the
/// `type.googleapis.com/` prefix, and its bytes in base64. `metadata` is the failed call's
/// headers and trailers, sent as headers as Connect unary does.
pub(super) fn connect_error(
    code: Code,
    message: &str,
    details: &[u8],
    metadata: Option<&HeaderMap>,
) -> axum::response::Response {
    let (name, status) = connect_code(code);
    let details: Vec<serde_json::Value> = tonic_types::Status::decode(details)
        .map(|s| s.details)
        .unwrap_or_default()
        .into_iter()
        .map(|any| {
            serde_json::json!({
                "type": any.type_url.trim_start_matches("type.googleapis.com/"),
                "value": STANDARD_NO_PAD.encode(any.value),
            })
        })
        .collect();
    let mut body = serde_json::json!({ "code": name });
    if !message.is_empty() {
        body["message"] = message.into();
    }
    if !details.is_empty() {
        body["details"] = details.into();
    }
    let mut response = axum::response::Response::new(axum::body::Body::from(body.to_string()));
    *response.status_mut() = status;
    if let Some(metadata) = metadata {
        response.headers_mut().extend(metadata.clone());
    }
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}

/// `415` for a body that is neither `application/proto` nor `application/json`.
pub(super) fn unsupported_media_type() -> axum::response::Response {
    let mut response = connect_error(
        Code::Unknown,
        "unsupported content type: send application/proto or application/json",
        &[],
        None,
    );
    *response.status_mut() = StatusCode::UNSUPPORTED_MEDIA_TYPE;
    response.headers_mut().insert(
        "accept-post",
        HeaderValue::from_static("application/proto, application/json"),
    );
    response
}
