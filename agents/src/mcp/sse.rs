//! The legacy HTTP+SSE transport from the 2024-11-05 spec.
//!
//! rmcp 3 dropped its SSE server, and Go still mounts one at the base path plus `/`, so a
//! client written for the old transport would find nothing here without this. It is the spec
//! and no more: a `GET` opens a session as an event stream whose first event, `endpoint`,
//! names where to `POST` messages; each `POST` is accepted with `202` and its answer arrives
//! on the stream. The session's handler runs on rmcp's own service loop, over a pair of
//! channels standing in for the socket.

use std::collections::HashMap;
use std::convert::Infallible;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use futures::channel::mpsc;
use futures::{SinkExt, Stream, StreamExt};
use rmcp::ServerHandler;
use rmcp::model::{ClientJsonRpcMessage, ServerJsonRpcMessage};
use tokio_util::sync::CancellationToken;

/// How often an idle stream is pinged, so a proxy does not close it.
const KEEP_ALIVE: Duration = Duration::from_secs(15);

/// One open session's inbox.
type Sessions = Arc<Mutex<HashMap<String, mpsc::Sender<ClientJsonRpcMessage>>>>;

/// What every request to one mount shares.
struct Shared<S> {
    factory: Arc<dyn Fn() -> Result<S, io::Error> + Send + Sync>,
    sessions: Sessions,
    endpoint: String,
    cancel: CancellationToken,
}

/// The query a `POST` names its session by.
#[derive(serde::Deserialize)]
struct SessionQuery {
    #[serde(rename = "sessionId")]
    session_id: String,
}

/// The routes for one SSE mount at `path`, which ends in `/`.
///
/// Every stream ends when `cancel` fires, which is what lets a drain finish instead of
/// waiting out the client — Go's `releaseOnShutdown`.
pub(crate) fn router<S: ServerHandler>(
    path: &str,
    factory: Arc<dyn Fn() -> Result<S, io::Error> + Send + Sync>,
    cancel: CancellationToken,
) -> Router {
    let shared = Arc::new(Shared {
        factory,
        sessions: Sessions::default(),
        endpoint: path.to_string(),
        cancel,
    });
    Router::new().route(
        path,
        axum::routing::get(open::<S>)
            .post(post::<S>)
            .with_state(shared),
    )
}

/// Opens a session and streams its messages.
async fn open<S: ServerHandler>(State(shared): State<Arc<Shared<S>>>) -> Response {
    let handler = match (shared.factory)() {
        Ok(handler) => handler,
        Err(err) => return (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    };
    let id = uuid::Uuid::new_v4().simple().to_string();
    let (inbox_tx, inbox_rx) = mpsc::channel::<ClientJsonRpcMessage>(16);
    let (outbox_tx, outbox_rx) = mpsc::channel::<ServerJsonRpcMessage>(16);
    shared
        .sessions
        .lock()
        .expect("mcp: sse sessions poisoned")
        .insert(id.clone(), inbox_tx);

    let session_cancel = shared.cancel.child_token();
    let sessions = Arc::clone(&shared.sessions);
    let session = id.clone();
    let serving = session_cancel.clone();
    tokio::spawn(async move {
        match rmcp::service::serve_server_with_ct(handler, (outbox_tx, inbox_rx), serving).await {
            Ok(running) => {
                let _ = running.waiting().await;
            }
            Err(err) => tracing::debug!(%err, "mcp: sse session ended before initialize"),
        }
        sessions
            .lock()
            .expect("mcp: sse sessions poisoned")
            .remove(&session);
    });
    tracing::debug!(session = %id, "mcp: sse session opened");

    let endpoint = format!("{}?sessionId={id}", shared.endpoint);
    let first = futures::stream::once(async move {
        Ok::<_, Infallible>(Event::default().event("endpoint").data(endpoint))
    });
    let messages = outbox_rx.filter_map(|message| async move {
        let data = serde_json::to_string(&message).ok()?;
        Some(Ok(Event::default().event("message").data(data)))
    });
    let ended = session_cancel.clone();
    let stream = first
        .chain(messages)
        .take_until(async move { ended.cancelled().await });
    Sse::new(Box::pin(stream) as std::pin::Pin<Box<dyn Stream<Item = _> + Send>>)
        .keep_alive(KeepAlive::new().interval(KEEP_ALIVE))
        .into_response()
}

/// Hands one message to its session.
async fn post<S: ServerHandler>(
    State(shared): State<Arc<Shared<S>>>,
    Query(query): Query<SessionQuery>,
    body: Bytes,
) -> Response {
    let Some(mut inbox) = shared
        .sessions
        .lock()
        .expect("mcp: sse sessions poisoned")
        .get(&query.session_id)
        .cloned()
    else {
        return (StatusCode::NOT_FOUND, "unknown session").into_response();
    };
    let message: ClientJsonRpcMessage = match serde_json::from_slice(&body) {
        Ok(message) => message,
        Err(err) => return (StatusCode::BAD_REQUEST, err.to_string()).into_response(),
    };
    if inbox.send(message).await.is_err() {
        return (StatusCode::GONE, "session closed").into_response();
    }
    StatusCode::ACCEPTED.into_response()
}
