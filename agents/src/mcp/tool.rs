//! Building the tools a generated server advertises, and the results they return.
//!
//! Generated code carries its schemas as JSON string constants — the plugin already produced
//! them from the proto — so everything here takes raw JSON and parses it once at startup.

use std::sync::Arc;

use rmcp::model::{CallToolResult, ContentBlock, JsonObject, Tool};
use serde_json::Value;

use crate::mcp::extras::ExtraProperty;

/// Parses a JSON schema string into the object shape [`Tool`] holds.
///
/// # Panics
///
/// Panics if the JSON is not a valid object. This is the Rust home of Go's `MustParseSchema`:
/// the input is a generated constant, so a failure here is a broken build rather than bad
/// input, and failing at startup beats serving a tool no client can call.
pub fn parse_schema(raw: &str) -> Arc<JsonObject> {
    let value: Value = serde_json::from_str(raw)
        .unwrap_or_else(|err| panic!("agents/mcp: failed to parse JSON schema: {err}"));
    match value {
        Value::Object(map) => Arc::new(map),
        other => panic!("agents/mcp: JSON schema must be an object, got {other}"),
    }
}

/// Builds a tool from a name, description, and raw input schema.
///
/// # Panics
///
/// Panics if the schema will not parse — see [`parse_schema`].
pub fn create_tool(name: &str, description: &str, input_schema: &str) -> Tool {
    Tool::new(
        name.to_string(),
        description.to_string(),
        parse_schema(input_schema),
    )
}

/// Attaches an output schema, which is what lets a client validate structured results.
#[must_use]
pub fn with_output_schema(mut tool: Tool, output_schema: &str) -> Tool {
    tool.output_schema = Some(parse_schema(output_schema));
    tool
}

/// Returns a copy of `tool` with extra properties injected into its input schema.
///
/// A tool with no extras is returned untouched, so the unconfigured case allocates nothing.
#[must_use]
pub fn prepare_tool_with_extras(tool: Tool, extras: &[ExtraProperty]) -> Tool {
    if extras.is_empty() {
        return tool;
    }

    let mut schema = (*tool.input_schema).clone();

    let properties = schema
        .entry("properties")
        .or_insert_with(|| Value::Object(serde_json::Map::new()));
    if let Some(properties) = properties.as_object_mut() {
        for extra in extras {
            properties.insert(
                extra.name.clone(),
                serde_json::json!({"type": "string", "description": extra.description}),
            );
        }
    }

    let required: Vec<&ExtraProperty> = extras.iter().filter(|e| e.required).collect();
    if !required.is_empty() {
        let entry = schema
            .entry("required")
            .or_insert_with(|| Value::Array(Vec::new()));
        if let Some(list) = entry.as_array_mut() {
            for extra in required {
                list.push(Value::String(extra.name.clone()));
            }
        }
    }

    let mut cloned = tool;
    cloned.input_schema = Arc::new(schema);
    cloned
}

/// Returns a copy of `tool` marked as an MCP App, which is what makes a supporting host
/// render the tool's own UI instead of a generic form.
#[must_use]
pub fn set_tool_app_meta(mut tool: Tool, resource_uri: &str) -> Tool {
    let mut meta = tool.meta.unwrap_or_default();
    meta.insert(
        "ui".to_string(),
        serde_json::json!({ "resourceUri": resource_uri }),
    );
    tool.meta = Some(meta);
    tool
}

/// A successful result carrying one text block.
pub fn text_result(text: impl Into<String>) -> CallToolResult {
    CallToolResult::success(vec![ContentBlock::text(text.into())])
}

/// A result flagged as an error.
///
/// Prefer [`crate::mcp::handle_error`] for a gRPC status; this is for messages of your own.
pub fn error_result(text: impl Into<String>) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(text.into())])
}

/// A successful result carrying both the JSON text and its parsed structured form.
///
/// Both are sent because clients disagree about which they read: one that understands
/// `structuredContent` validates it against the tool's output schema, and one that does not
/// still has the text to show.
pub fn structured_result(payload: &Value) -> CallToolResult {
    let mut result = text_result(payload.to_string());
    result.structured_content = Some(payload.clone());
    result
}
