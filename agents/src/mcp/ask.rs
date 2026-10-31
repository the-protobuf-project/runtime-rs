//! Elicitation a service raises: a question only the handler can ask, because only it knows
//! what is missing or what is at stake — "Book Sea View Deluxe, 12 to 14 December?".
//!
//! The handler returns [`ask`]'s status: a gRPC error carrying a `google.rpc.ErrorInfo` whose
//! reason is [`ASK_REASON`] and whose metadata holds the question and the fields it asks for,
//! as the JSON Schema MCP elicitation takes. Every surface sees the same error — a REST or
//! gRPC client reads it with [`asked`] — and an MCP tool turns it into an elicitation with
//! [`ask_pending`], then, when the client calls again with the answers, overlays them on the
//! arguments with [`merge_answers`] and calls the handler again.

use std::collections::{BTreeMap, HashMap};

use rmcp::model::{
    CallToolRequestParams, CallToolResponse, ElicitRequest, ElicitRequestParams, ElicitResult,
    ElicitationAction, InputRequest, InputRequiredResult,
};
use rmcp::service::{RequestContext, RoleServer};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tonic_types::{ErrorDetails, StatusExt};

use super::{ElicitField, elicit_schema, merge_elicit_result};
use crate::mcp::tool::error_result;

/// The `ErrorInfo.reason` of an [`ask`] status.
pub const ASK_REASON: &str = "ELICITATION_REQUIRED";
/// The `ErrorInfo.domain` of an [`ask`] status.
pub const ASK_DOMAIN: &str = "mcp.the-protobuf-project.org";

/// Starts the input-request id of a raised question; the rest hashes the question, so each
/// question has its own answer.
const ASK_PREFIX: &str = "ask:";

/// The status a handler returns to ask its caller for fields — each a field of its request —
/// before it can go on.
///
/// `code` is how a client that cannot ask reads it: `InvalidArgument` for missing details,
/// `FailedPrecondition` for a consent. `message` is the question.
pub fn ask(code: tonic::Code, message: &str, fields: &[ElicitField]) -> tonic::Status {
    let schema = match serde_json::to_string(&elicit_schema(fields)) {
        Ok(schema) => schema,
        Err(err) => return tonic::Status::internal(format!("mcp: ask: {err}")),
    };
    let metadata = HashMap::from([
        ("message".to_string(), message.to_string()),
        ("requested_schema".to_string(), schema),
    ]);
    tonic::Status::with_error_details(
        code,
        message,
        ErrorDetails::with_error_info(ASK_REASON, ASK_DOMAIN, metadata),
    )
}

/// The question `status` asks — its message and JSON Schema — when it is an [`ask`] status.
pub fn asked(status: &tonic::Status) -> Option<(String, Value)> {
    let info = status.get_details_error_info()?;
    if info.reason != ASK_REASON || info.domain != ASK_DOMAIN {
        return None;
    }
    let schema = serde_json::from_str(info.metadata.get("requested_schema")?).ok()?;
    Some((info.metadata.get("message")?.clone(), schema))
}

/// The input-request id of `message`: the prefix and six bytes of its SHA-256, in hex.
pub fn ask_id(message: &str) -> String {
    let digest = Sha256::digest(message.as_bytes());
    let hex: String = digest[..6].iter().map(|b| format!("{b:02x}")).collect();
    format!("{ASK_PREFIX}{hex}")
}

/// Whether the client said it answers elicitations: in the request's `_meta`, as a client of
/// the stateless 2026-07-28 protocol does, or in its session's `initialize`. A stateless
/// server has no `initialize` from an older client, which so cannot be asked.
fn can_elicit(context: &RequestContext<RoleServer>) -> bool {
    let per_request = context
        .meta
        .client_capabilities()
        .is_some_and(|capabilities| capabilities.elicitation.is_some());
    per_request
        || context
            .peer
            .peer_info()
            .is_some_and(|info| info.capabilities.elicitation.is_some())
}

/// The fields `schema` asks for, sorted.
fn names(schema: &Value) -> Vec<String> {
    let mut names: Vec<String> = schema["properties"]
        .as_object()
        .map(|properties| properties.keys().cloned().collect())
        .unwrap_or_default();
    names.sort_unstable();
    names
}

/// The tool result that puts `status`'s question to the client, when it is an [`ask`] status.
///
/// That is an elicitation when the client can answer one, and otherwise the question as a
/// tool error — "message (field, field)" — for the model to ask in its own words and call
/// again with the answer.
pub fn ask_pending(
    context: &RequestContext<RoleServer>,
    status: &tonic::Status,
) -> Option<CallToolResponse> {
    let (message, schema) = asked(status)?;
    if !can_elicit(context) {
        return Some(error_result(format!("{message} ({})", names(&schema).join(", "))).into());
    }
    let requested_schema = serde_json::from_value(schema).ok()?;
    let params = ElicitRequestParams::FormElicitationParams {
        meta: None,
        message: message.clone(),
        requested_schema,
    };
    let requests = BTreeMap::from([(
        ask_id(&message),
        InputRequest::Elicitation(ElicitRequest::new(params)),
    )]);
    Some(InputRequiredResult::new(Some(requests), None).into())
}

/// Overlays on `arguments` the answers `request` carries to questions a handler raised.
///
/// The second value is true when the user declined or cancelled one: the call should stop,
/// not run on without the answer.
pub fn merge_answers(request: &CallToolRequestParams, arguments: Value) -> (Value, bool) {
    let mut merged = arguments;
    let Some(responses) = &request.input_responses else {
        return (merged, false);
    };
    for (id, response) in responses {
        if !id.starts_with(ASK_PREFIX) {
            continue;
        }
        let Ok(answer) = serde_json::from_value::<ElicitResult>(response.clone()) else {
            continue;
        };
        if answer.action != ElicitationAction::Accept {
            return (merged, true);
        }
        if let Some(content) = answer.content {
            merged = merge_elicit_result(merged, &content, &[]);
        }
    }
    (merged, false)
}
