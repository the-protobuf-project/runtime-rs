//! Progress notifications for a tool call that streams.
//!
//! A server-streaming RPC exposed as an MCP tool reports incremental progress through this,
//! so a client watching a long call sees movement rather than a stall.
//!
//! Go's runtime also carries an `InProcessServerStream` here, because a Go streaming handler
//! is *handed* a `grpc.ServerStream` to write into, and calling one in-process means supplying
//! that sink. A tonic streaming handler instead *returns* a `Stream`, so generated code just
//! consumes what the method already gave it — there is nothing for an adapter to bridge, which
//! is why this module has no counterpart to it.

use rmcp::model::{ProgressNotificationParam, ProgressToken};
use rmcp::service::{Peer, RoleServer};

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
