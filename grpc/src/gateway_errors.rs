//! Mapping a gRPC status onto an HTTP response the REST gateway can return.
//!
//! Every failed gateway request leaves through here, so a REST client sees one envelope
//! shape whatever went wrong underneath.

use axum::response::{IntoResponse, Response};
use http::{HeaderValue, StatusCode, header};
use serde::Serialize;

/// The canonical `SCREAMING_SNAKE` `google.rpc.Code` name for a gRPC code, which is the
/// conventional form for REST error bodies.
///
/// ```
/// use grpc::gateway_errors::status_name;
///
/// assert_eq!(status_name(tonic::Code::NotFound), "NOT_FOUND");
/// ```
pub fn status_name(code: tonic::Code) -> &'static str {
    match code {
        tonic::Code::Ok => "OK",
        // google.rpc.Code spells this with two Ls, where the gRPC code constant uses one.
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

/// Converts a gRPC status code into the canonical HTTP response status, using the same
/// table grpc-gateway does.
///
/// | | | | |
/// |---|---|---|---|
/// | `OK` → 200 | `Cancelled` → 499 | `InvalidArgument` → 400 | `DeadlineExceeded` → 504 |
/// | `NotFound` → 404 | `AlreadyExists` → 409 | `PermissionDenied` → 403 | `Unauthenticated` → 401 |
/// | `ResourceExhausted` → 429 | `FailedPrecondition` → 400 | `Aborted` → 409 | `OutOfRange` → 400 |
/// | `Unimplemented` → 501 | `Unavailable` → 503 | `Internal`/`Unknown`/`DataLoss` → 500 | |
pub fn http_status_from_code(code: tonic::Code) -> StatusCode {
    match code {
        tonic::Code::Ok => StatusCode::OK,
        // 499 is nginx's "client closed request"; it has no http crate constant.
        tonic::Code::Cancelled => StatusCode::from_u16(499).expect("499 is a valid status"),
        tonic::Code::InvalidArgument
        | tonic::Code::FailedPrecondition
        | tonic::Code::OutOfRange => StatusCode::BAD_REQUEST,
        tonic::Code::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
        tonic::Code::NotFound => StatusCode::NOT_FOUND,
        tonic::Code::AlreadyExists | tonic::Code::Aborted => StatusCode::CONFLICT,
        tonic::Code::PermissionDenied => StatusCode::FORBIDDEN,
        tonic::Code::Unauthenticated => StatusCode::UNAUTHORIZED,
        tonic::Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        tonic::Code::Unimplemented => StatusCode::NOT_IMPLEMENTED,
        tonic::Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        tonic::Code::Unknown | tonic::Code::Internal | tonic::Code::DataLoss => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// The JSON envelope the REST gateway returns for any failed request.
///
/// Field names are single words, so they are identical under both camelCase and snake_case
/// marshalling — the body does not change shape when
/// [`crate::HybridServer::with_rest_snake_case`] is on.
#[derive(Debug, Clone, Serialize)]
pub struct GatewayError {
    /// The error itself, nested so the envelope has room to grow.
    pub error: GatewayErrorBody,
}

/// The body of a [`GatewayError`].
#[derive(Debug, Clone, Serialize)]
pub struct GatewayErrorBody {
    /// The HTTP status code, so REST clients can branch on it without understanding gRPC
    /// codes.
    pub code: u16,
    /// The canonical `google.rpc.Code` name, e.g. `NOT_FOUND`.
    pub status: &'static str,
    /// The human-readable message from the gRPC status.
    pub message: String,
    /// Any structured status details, omitted when empty.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub details: Vec<serde_json::Value>,
}

impl GatewayError {
    /// Builds the envelope a gRPC status should be reported as.
    pub fn from_status(status: &tonic::Status) -> Self {
        Self::with_http_status(status, http_status_from_code(status.code()))
    }

    /// Builds the envelope with the HTTP status overridden, for a routing error that knows
    /// better than the code mapping does — a 404 from the router itself, say, rather than
    /// from a handler.
    pub fn with_http_status(status: &tonic::Status, http_status: StatusCode) -> Self {
        Self {
            error: GatewayErrorBody {
                code: http_status.as_u16(),
                status: status_name(status.code()),
                message: status.message().to_string(),
                details: Vec::new(),
            },
        }
    }

    /// Attaches structured `google.rpc.Status` details.
    #[must_use]
    pub fn with_details(mut self, details: Vec<serde_json::Value>) -> Self {
        self.error.details = details;
        self
    }

    /// The HTTP status this envelope reports.
    pub fn http_status(&self) -> StatusCode {
        StatusCode::from_u16(self.error.code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
    }
}

impl IntoResponse for GatewayError {
    fn into_response(self) -> Response {
        let status = self.http_status();
        let unauthenticated = self.error.status == "UNAUTHENTICATED";
        let challenge = unauthenticated.then(|| HeaderValue::from_str(&self.error.message).ok());

        let body = serde_json::to_vec(&self).unwrap_or_else(|err| {
            tracing::debug!(error = %err, "HTTP gateway error: failed to marshal envelope");
            br#"{"error":{"code":500,"status":"INTERNAL","message":"failed to marshal error"}}"#
                .to_vec()
        });

        let mut response = Response::new(axum::body::Body::from(body));
        *response.status_mut() = status;
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        // Trailers and chunked framing belong to the gRPC leg, not to the JSON body that
        // replaced it; leaving them on confuses a plain HTTP client.
        response.headers_mut().remove(header::TRAILER);
        response.headers_mut().remove(header::TRANSFER_ENCODING);
        if let Some(Some(value)) = challenge {
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, value);
        }
        response
    }
}

/// Turns a gRPC status into the gateway's HTTP response, the direct analogue of Go's
/// `httpErrorHandler`.
pub fn http_error_response(status: &tonic::Status) -> Response {
    tracing::debug!(
        code = ?status.code(),
        message = status.message(),
        "HTTP gateway error"
    );
    GatewayError::from_status(status).into_response()
}
