//! Progress notifications for a tool call that streams.
//!
//! A server-streaming RPC exposed as an MCP tool reports incremental progress through this,
//! so a client watching a long call sees movement rather than a stall. Chunks arrive as a
//! generated `MCPProgress`, which [`ProgressMessage`] reads without tying this crate to any
//! one build of that schema; [`crate::mcp::InProcessServerStream`] carries them in-process.

use rmcp::model::{
    NotificationMetaObject, NumberOrString, ProgressNotificationParam, ProgressToken,
};
use rmcp::service::{Peer, RoleServer};
use serde_json::{Map, Value};

/// The shape of a generated `mcp.v1.MCPProgress`.
///
/// It is a trait rather than the generated struct for the reason Go's is an interface: a
/// dependency on one build of the annotations would clash with a differently-built one in the
/// same binary. The token and meta accessors default to empty because older schemas have
/// neither; a total of 0 is what the spec means by unknown.
pub trait ProgressMessage: Send + Sync {
    /// The progress so far.
    fn progress(&self) -> f64;
    /// What to show alongside it. Empty means nothing.
    fn message(&self) -> &str;
    /// The expected final value, or 0 when unknown.
    fn total(&self) -> f64;
    /// The chunk's own string token, which wins over the request's.
    fn token_string(&self) -> &str {
        ""
    }
    /// The chunk's own integer token, which wins over the request's.
    fn token_int(&self) -> i64 {
        0
    }
    /// Arbitrary metadata carried alongside the update, as the notification's `_meta`.
    fn meta(&self) -> Map<String, Value> {
        Map::new()
    }
}

/// Maps a chunk onto the notification a client receives.
///
/// `fallback` is the request's token; a chunk carrying its own overrides it, so a gRPC
/// server fanning one stream out to several MCP requests can tag each chunk.
pub fn progress_params(
    fallback: ProgressToken,
    chunk: &dyn ProgressMessage,
) -> ProgressNotificationParam {
    let token = if !chunk.token_string().is_empty() {
        ProgressToken(NumberOrString::String(chunk.token_string().into()))
    } else if chunk.token_int() != 0 {
        ProgressToken(NumberOrString::Number(chunk.token_int()))
    } else {
        fallback
    };
    let mut params = ProgressNotificationParam::new(token, chunk.progress());
    if chunk.total() != 0.0 {
        params = params.with_total(chunk.total());
    }
    if !chunk.message().is_empty() {
        params = params.with_message(chunk.message().to_string());
    }
    let meta = chunk.meta();
    if !meta.is_empty() {
        params.meta = Some(NotificationMetaObject(rmcp::model::MetaObject(meta)));
    }
    params
}

/// Sends a notification for `chunk`. With no peer, token or chunk it does nothing, as Go's
/// `SendProgressFromProto` does with a nil one.
///
/// # Errors
///
/// The notification could not be sent.
pub async fn send_progress_from_proto(
    peer: Option<&Peer<RoleServer>>,
    token: Option<ProgressToken>,
    chunk: Option<&dyn ProgressMessage>,
) -> Result<(), rmcp::ServiceError> {
    let (Some(peer), Some(token), Some(chunk)) = (peer, token, chunk) else {
        return Ok(());
    };
    peer.notify_progress(progress_params(token, chunk)).await
}

/// The final update [`send_done_progress`] sends, built without a generated value.
struct DoneProgress(String);

impl ProgressMessage for DoneProgress {
    fn progress(&self) -> f64 {
        1.0
    }
    fn message(&self) -> &str {
        &self.0
    }
    fn total(&self) -> f64 {
        1.0
    }
}

/// Sends progress=1, total=1 with `result_json` as the message, telling the client a
/// streaming call has finished.
///
/// # Errors
///
/// The notification could not be sent.
pub async fn send_done_progress(
    peer: Option<&Peer<RoleServer>>,
    token: Option<ProgressToken>,
    result_json: impl Into<String>,
) -> Result<(), rmcp::ServiceError> {
    let done = DoneProgress(result_json.into());
    send_progress_from_proto(peer, token, Some(&done)).await
}

/// Sends MCP progress notifications for one tool call.
///
/// When the client sent no progress token there is nothing to correlate a notification with,
/// so every send is a no-op and an implementation never has to check. The [`Default`] sink is
/// exactly that inert one, which is what a unit test for a streaming tool wants.
#[derive(Clone, Default)]
pub struct ProgressSink {
    peer: Option<Peer<RoleServer>>,
    token: Option<ProgressToken>,
}

impl ProgressSink {
    /// A sink bound to one request's peer and progress token.
    ///
    /// Both are optional because both are absent on a call the client is not watching.
    pub fn new(peer: Option<Peer<RoleServer>>, token: Option<ProgressToken>) -> Self {
        Self { peer, token }
    }

    /// Whether anything is listening. A caller doing expensive work purely to report it can
    /// skip that work when nothing will receive it.
    pub fn is_active(&self) -> bool {
        self.peer.is_some() && self.token.is_some()
    }

    /// Reports progress. `total` is the expected final value when it is known.
    pub async fn send(&self, progress: f64, total: Option<f64>, message: Option<String>) {
        let (Some(peer), Some(token)) = (self.peer.as_ref(), self.token.as_ref()) else {
            return;
        };

        // ProgressNotificationParam is #[non_exhaustive], so it is built through its
        // constructor rather than a struct literal.
        let mut param = ProgressNotificationParam::new(token.clone(), progress);
        if let Some(total) = total {
            param = param.with_total(total);
        }
        if let Some(message) = message {
            param = param.with_message(message);
        }

        // A client that has gone away should not fail the tool call it is no longer
        // watching, so a failed notification is dropped rather than propagated.
        let _ = peer.notify_progress(param).await;
    }

    /// Reports one generated `MCPProgress` chunk; its own token, if any, wins.
    pub async fn send_chunk(&self, chunk: &dyn ProgressMessage) {
        let _ = send_progress_from_proto(self.peer.as_ref(), self.token.clone(), Some(chunk)).await;
    }

    /// Reports completion, carrying the final result as the message.
    ///
    /// Progress and total are both 1, which is how a client that is only watching the
    /// fraction learns the call is done.
    pub async fn send_done(&self, result_json: impl Into<String>) {
        self.send(1.0, Some(1.0), Some(result_json.into())).await;
    }
}

impl std::fmt::Debug for ProgressSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProgressSink")
            .field("active", &self.is_active())
            .finish()
    }
}
