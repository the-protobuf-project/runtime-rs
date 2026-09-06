//! The execution vocabulary: what an agent emits, and how a failure becomes an event.

#![cfg(feature = "a2a")]

// Everything comes from the one module, which is the point of its re-exports.
use agents::a2a::{self, Event, Executor, ExecutorContext, Role, TaskState};
use futures::StreamExt;
use serde_json::json;

/// A context standing in for one the SDK would build from a real request.
fn context(text: Option<&str>) -> ExecutorContext {
    ExecutorContext {
        message: text.map(|t| a2a::agent_message(vec![a2a::text_part(t)])),
        task_id: "task-1".into(),
        stored_task: None,
        context_id: "ctx-1".into(),
        metadata: None,
        user: None,
        service_params: Default::default(),
        tenant: None,
    }
}

#[test]
fn request_text_joins_the_parts_and_skips_the_rest() {
    let mut ctx = context(None);
    ctx.message = Some(a2a::agent_message(vec![
        a2a::text_part("first"),
        a2a::data_part(json!({"ignored": true})),
        a2a::text_part("second"),
    ]));

    assert_eq!(a2a::request_text(&ctx), "first\nsecond");
}

#[test]
fn a_request_with_no_message_reads_as_empty_rather_than_panicking() {
    assert_eq!(a2a::request_text(&context(None)), "");
}

#[test]
fn a_status_event_carries_the_executions_own_task_and_context() {
    let ctx = context(Some("hello"));
    let event = a2a::status_event(&ctx, TaskState::Working, None);

    let Event::StatusUpdate(update) = event else {
        panic!("expected a status update");
    };
    assert_eq!(update.task_id, "task-1");
    assert_eq!(update.context_id, "ctx-1");
    assert_eq!(update.status.state, TaskState::Working);
}

#[test]
fn an_artifact_event_is_attributed_to_the_same_task() {
    let ctx = context(Some("hello"));
    let event = a2a::artifact_event(&ctx, vec![a2a::text_part("result")]);

    let Event::ArtifactUpdate(update) = event else {
        panic!("expected an artifact update");
    };
    assert_eq!(update.task_id, "task-1");
    assert_eq!(update.artifact.parts.len(), 1);
    assert_eq!(
        update.last_chunk,
        Some(true),
        "a one-shot artifact is complete when it is sent"
    );
}

#[test]
fn an_agent_message_is_addressed_from_the_agent() {
    let message = a2a::agent_message(vec![a2a::text_part("hi")]);
    assert_eq!(message.role, Role::Agent);
}

#[tokio::test]
async fn a_text_agent_replies_with_a_message() {
    let agent = a2a::TextAgent::new(|text: String| async move { Ok(format!("you said: {text}")) });

    let events: Vec<_> = agent.execute(context(Some("hello"))).collect().await;
    assert_eq!(events.len(), 1);

    let Event::Message(message) = events[0].as_ref().expect("an event") else {
        panic!("a one-shot reply is a message, not a task: {:?}", events[0]);
    };
    assert_eq!(message.text(), Some("you said: hello"));
}

#[tokio::test]
async fn a_text_agent_failure_becomes_a_terminal_status_carrying_the_code() {
    let agent = a2a::TextAgent::new(|_: String| async move {
        Err(tonic::Status::permission_denied("not yours"))
    });

    let events: Vec<_> = agent.execute(context(Some("hello"))).collect().await;
    let Event::StatusUpdate(update) = events[0].as_ref().expect("an event") else {
        panic!("expected a status update");
    };

    // Permission denied is work the server declined, which the protocol calls rejected.
    assert_eq!(update.status.state, TaskState::Rejected);
    let text = update
        .status
        .message
        .as_ref()
        .and_then(|m| m.text())
        .unwrap_or_default();
    assert!(text.contains("PERMISSION_DENIED"), "{text}");
    assert!(text.contains("not yours"), "{text}");
}

#[tokio::test]
async fn cancelling_reports_cancelled_rather_than_failed() {
    let agent = a2a::TextAgent::new(|_: String| async move { Ok(String::new()) });

    let events: Vec<_> = agent.cancel(context(None)).collect().await;
    let Event::StatusUpdate(update) = events[0].as_ref().expect("an event") else {
        panic!("expected a status update");
    };
    assert_eq!(
        update.status.state,
        TaskState::Canceled,
        "a task the client stopped did not fail"
    );
}

#[test]
fn error_text_reads_the_same_wherever_it_is_built() {
    assert_eq!(
        a2a::error_text(&tonic::Status::not_found("no such agent")),
        "NOT_FOUND: no such agent"
    );
    // An Unknown code carries nothing a reader can act on, so it is left off.
    assert_eq!(
        a2a::error_text(&tonic::Status::unknown("something broke")),
        "something broke"
    );
}

#[test]
fn a_status_code_maps_onto_the_state_that_describes_the_work() {
    use a2a::state_for_code;
    assert_eq!(state_for_code(tonic::Code::Cancelled), TaskState::Canceled);
    assert_eq!(
        state_for_code(tonic::Code::Unauthenticated),
        TaskState::Rejected
    );
    assert_eq!(
        state_for_code(tonic::Code::PermissionDenied),
        TaskState::Rejected
    );
    assert_eq!(state_for_code(tonic::Code::Internal), TaskState::Failed);
    assert_eq!(state_for_code(tonic::Code::Ok), TaskState::Completed);
}
