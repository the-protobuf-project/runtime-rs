//! A probe route answering in the shape of an MCP ping.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};
use tonic::body::Body;
use tonic::client::GrpcService;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

/// How long a probe waits for its answer.
const HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(5);

/// What a probe learned from the backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthState {
    /// The backend reports `SERVING`.
    Serving,
    /// The backend answered with anything else.
    NotServing,
    /// The backend could not be reached.
    Unavailable,
}

/// The check behind a probe: one async question, asked once per request.
///
/// Go takes a `*grpc.ClientConn`. This is a closure because a tonic client is generic over
/// its transport; [`grpc_health_check`] builds one from a channel or an in-process
/// `Loopback` alike.
pub type HealthCheck =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = HealthState> + Send>> + Send + Sync>;

/// A [`HealthCheck`] asking `grpc.health.v1.Health/Check` about the whole server.
pub fn grpc_health_check<T>(service: T) -> HealthCheck
where
    T: GrpcService<Body> + Clone + Send + Sync + 'static,
    T::Future: Send,
    T::ResponseBody: http_body::Body<Data = bytes::Bytes> + Send + 'static,
    <T::ResponseBody as http_body::Body>::Error:
        Into<Box<dyn std::error::Error + Send + Sync>> + Send,
    T::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    Arc::new(move || {
        let mut client = HealthClient::new(service.clone());
        Box::pin(async move {
            let request = HealthCheckRequest {
                service: String::new(),
            };
            match client.check(request).await {
                Ok(response) if response.get_ref().status() == ServingStatus::Serving => {
                    HealthState::Serving
                }
                Ok(_) => HealthState::NotServing,
                Err(_) => HealthState::Unavailable,
            }
        })
    })
}

/// Answers a load balancer or a Kubernetes probe.
///
/// `GET` and `HEAD` run `check` and respond in MCP ping form: `200` with
/// `{"jsonrpc":"2.0","id":"health","result":{}}` when serving, and `503` with a JSON-RPC
/// error of code `-32000` otherwise. Anything else is `405`. A check that outlasts five
/// seconds counts as unreachable.
pub fn health_check_handler(check: HealthCheck) -> axum::routing::MethodRouter {
    axum::routing::any(move |method: Method| {
        let check = Arc::clone(&check);
        async move {
            if method != Method::GET && method != Method::HEAD {
                return StatusCode::METHOD_NOT_ALLOWED.into_response();
            }
            let state = tokio::time::timeout(HEALTH_CHECK_TIMEOUT, check())
                .await
                .unwrap_or(HealthState::Unavailable);
            tracing::debug!(?state, "mcp: health probe answered");
            respond(state)
        }
    })
}

/// The response for one probe's outcome.
fn respond(state: HealthState) -> Response {
    let failure = |message: &str| -> Value {
        json!({"jsonrpc": "2.0", "id": "health", "error": {"code": -32000, "message": message}})
    };
    match state {
        HealthState::Serving => (
            StatusCode::OK,
            Json(json!({"jsonrpc": "2.0", "id": "health", "result": {}})),
        ),
        HealthState::NotServing => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(failure("not serving")),
        ),
        HealthState::Unavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(failure("service unavailable")),
        ),
    }
    .into_response()
}
