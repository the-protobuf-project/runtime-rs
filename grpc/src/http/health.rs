//! `GET /health`, the endpoint a load balancer asks before sending real traffic.

use axum::Router;
use axum::routing::get;

/// The friendly messages the endpoint answers with, one picked per request.
///
/// Ported verbatim from the Go runtime, which picks at random. There is no reason for a health
/// check to be cheerful, and every reason for the two runtimes to look alike.
const MESSAGES: [&str; 5] = [
    "I'm doing fine, bro. Don't worry. 🌱",
    "Still alive and kicking. 🚀",
    "All systems nominal. 👍",
    "Healthy as a horse. 🐴",
    "Feeling great, thanks for asking. 😎",
];

/// A router carrying `GET /health`.
pub fn health_router() -> Router {
    Router::new().route("/health", get(health))
}

/// Always 200, with a small JSON body.
///
/// It reports the process is answering, which is what the path is asked for. A check that
/// probed dependencies would fail a server that is up and merely waiting on something else,
/// and take it out of rotation for a fault it did not have.
async fn health() -> axum::response::Response {
    let message = MESSAGES[next_index()];
    let body = format!(r#"{{"status":"ok","message":"{message}"}}"#);

    axum::response::Response::builder()
        .status(http::StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(axum::body::Body::from(body))
        .unwrap_or_default()
}

/// Cycles through the messages.
///
/// Go seeds a random pick; a counter gives the same variety without pulling in an RNG for a
/// decoration.
fn next_index() -> usize {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed) % MESSAGES.len()
}
