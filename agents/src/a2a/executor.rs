//! What an agent implements, and the events it emits while doing the work.
//!
//! The vocabulary is re-exported from the SDK rather than wrapped: these are wire types the
//! protocol defines, and a copy here would be one more thing to keep in step for no gain.
//! What this module adds are the constructors an agent actually reaches for.

use std::future::Future;

use a2a::{
    A2AError, Artifact, Message, Part, Role, StreamResponse, TaskState, TaskStatus,
    TaskStatusUpdateEvent,
};
use a2a_server::{AgentExecutor, ExecutorContext};
use futures::stream::{self, BoxStream};

/// The sequence an execution yields.
///
/// Go's executor returns an `iter.Seq2[Event, error]`; the Rust SDK's returns a boxed
/// [`futures::Stream`] of the same pairs, which is the same contract in the shape an async
/// runtime can poll.
pub type Events = BoxStream<'static, Result<StreamResponse, A2AError>>;

/// Wraps text as a message part.
pub fn text_part(text: impl Into<String>) -> Part {
    Part::text(text)
}

/// Wraps structured data as a message part, for an agent answering with something a client
/// will parse rather than display.
pub fn data_part(value: serde_json::Value) -> Part {
    Part::data(value)
}

/// Builds a message from this agent carrying `parts`.
pub fn agent_message(parts: Vec<Part>) -> Message {
    Message::new(Role::Agent, parts)
}

/// Reports that a task has moved to `state`, with an optional message explaining why.
///
/// It is the event an agent emits most. The task and context ids come from the execution
/// rather than the caller, so an event cannot be attributed to the wrong task.
pub fn status_event(
    ctx: &ExecutorContext,
    state: TaskState,
    message: Option<Message>,
) -> StreamResponse {
    let (task_id, context_id) = ctx.task_info();
    StreamResponse::StatusUpdate(TaskStatusUpdateEvent {
        task_id,
        context_id,
        status: TaskStatus {
            state,
            message,
            timestamp: Some(chrono::Utc::now()),
        },
        metadata: None,
    })
}

/// Emits a new artifact — a result the client keeps, as opposed to a message it reads.
pub fn artifact_event(ctx: &ExecutorContext, parts: Vec<Part>) -> StreamResponse {
    let (task_id, context_id) = ctx.task_info();
    StreamResponse::ArtifactUpdate(a2a::TaskArtifactUpdateEvent {
        task_id,
        context_id,
        artifact: Artifact {
            artifact_id: uuid::Uuid::new_v4().to_string(),
            name: None,
            description: None,
            parts,
            metadata: None,
            extensions: None,
        },
        append: None,
        last_chunk: Some(true),
        metadata: None,
    })
}

/// The text of the message that triggered this execution, with the parts joined by newlines.
///
/// It is a convenience for the common case and a lossy one by construction: an agent handling
/// files or structured input should read [`ExecutorContext::message`] itself rather than the
/// flattened text.
pub fn request_text(ctx: &ExecutorContext) -> String {
    let Some(message) = ctx.message.as_ref() else {
        return String::new();
    };
    message
        .parts
        .iter()
        .filter_map(|part| match &part.content {
            a2a::PartContent::Text(text) if !text.is_empty() => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// One event, as the stream an executor returns.
pub fn once(event: StreamResponse) -> Events {
    Box::pin(stream::once(async move { Ok(event) }))
}

/// The smallest useful agent: text in, text out, one reply per request.
///
/// It exists because [`AgentExecutor`] is built for streaming — a sequence of events over a
/// task's lifetime — and an agent that answers in one shot should not have to write that out.
/// The reply is emitted as a message rather than a task, which is what the protocol prescribes
/// for work that finished before there was anything to track.
///
/// An error becomes a failed task carrying its message; see [`crate::a2a::handle_error`] for
/// how a gRPC status is preserved through that.
pub struct TextAgent<F>(F);

impl<F, Fut> TextAgent<F>
where
    F: Fn(String) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<String, tonic::Status>> + Send + 'static,
{
    /// Builds an agent from an async function over the request text.
    pub fn new(f: F) -> Self {
        Self(f)
    }
}

impl<F, Fut> AgentExecutor for TextAgent<F>
where
    F: Fn(String) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<String, tonic::Status>> + Send + 'static,
{
    fn execute(&self, ctx: ExecutorContext) -> Events {
        let text = request_text(&ctx);
        let running = (self.0)(text);
        Box::pin(stream::once(async move {
            Ok(match running.await {
                Ok(reply) => StreamResponse::Message(agent_message(vec![text_part(reply)])),
                Err(status) => crate::a2a::handle_error(&ctx, &status),
            })
        }))
    }

    /// Reports the task cancelled without interrupting anything.
    ///
    /// That is the honest answer for work that is not interruptible: the client learns the
    /// task will produce nothing more, rather than waiting on an execution that ignored the
    /// request. An agent that can genuinely abort mid-flight should implement
    /// [`AgentExecutor`] itself.
    fn cancel(&self, ctx: ExecutorContext) -> Events {
        once(status_event(&ctx, TaskState::Canceled, None))
    }
}
