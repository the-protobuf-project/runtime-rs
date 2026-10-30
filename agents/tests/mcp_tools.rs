//! Tools and results: what a generated server advertises, and what a call returns.

#![cfg(feature = "mcp")]

use agents::mcp;
use serde_json::json;

const SCHEMA: &str =
    r#"{"type":"object","properties":{"to":{"type":"integer"}},"required":["to"]}"#;

#[test]
fn a_tool_is_built_from_the_generated_schema_constants() {
    let tool = mcp::with_output_schema(
        mcp::create_tool("count", "Counts to a number", SCHEMA),
        r#"{"type":"object","properties":{"value":{"type":"integer"}}}"#,
    );

    assert_eq!(tool.name, "count");
    assert_eq!(tool.input_schema["type"], "object");
    assert!(
        tool.output_schema.is_some(),
        "an output schema is what lets a client validate structured results"
    );
}

#[test]
fn extras_are_added_to_the_schema_and_taken_back_out_of_the_arguments() {
    let extras = vec![
        mcp::ExtraProperty::new("api_key", "API key for auth").required(),
        mcp::ExtraProperty::new("tenant", "Tenant id"),
    ];

    let tool = mcp::prepare_tool_with_extras(mcp::create_tool("count", "", SCHEMA), &extras);
    let properties = tool.input_schema["properties"]
        .as_object()
        .expect("properties");
    assert!(properties.contains_key("api_key"));
    assert!(properties.contains_key("tenant"));
    assert!(
        properties.contains_key("to"),
        "the proto's own fields must survive the injection"
    );

    let required = tool.input_schema["required"].as_array().expect("required");
    assert!(required.iter().any(|v| v == "api_key"));
    assert!(
        !required.iter().any(|v| v == "tenant"),
        "an optional extra must not become required"
    );

    // On the way in, the extras come back off so they never reach a proto field.
    let (arguments, found) = mcp::extract_extras(
        json!({"to": 5, "api_key": "secret", "tenant": "acme"}),
        &extras,
    );
    assert_eq!(arguments, json!({"to": 5}));
    assert_eq!(found["api_key"], "secret");
    assert_eq!(found["tenant"], "acme");
}

#[test]
fn a_tool_with_no_extras_is_left_exactly_as_it_was() {
    let tool = mcp::create_tool("count", "", SCHEMA);
    let prepared = mcp::prepare_tool_with_extras(tool.clone(), &[]);
    assert_eq!(prepared.input_schema, tool.input_schema);

    let (arguments, found) = mcp::extract_extras(json!({"to": 1}), &[]);
    assert_eq!(arguments, json!({"to": 1}));
    assert!(found.is_empty());
}

#[test]
fn results_carry_both_the_text_and_the_structured_form() {
    let payload = json!({"value": 7});
    let result = mcp::structured_result(&payload);

    assert_eq!(result.structured_content, Some(payload.clone()));
    assert_ne!(result.is_error, Some(true));
    assert!(
        !result.content.is_empty(),
        "a client that ignores structuredContent still needs the text"
    );
}

#[test]
fn a_grpc_failure_keeps_its_code_in_the_payload() {
    let result = mcp::handle_error(&tonic::Status::not_found("no such todo"));
    assert_eq!(result.is_error, Some(true));

    let text = serde_json::to_string(&result.content).expect("content serialises");
    assert!(text.contains("NOT_FOUND"), "code should survive: {text}");
    assert!(
        text.contains("no such todo"),
        "message should survive: {text}"
    );
}

#[test]
fn an_app_tool_points_at_its_own_ui_resource() {
    let uri = mcp::app_resource_uri("CounterService");
    assert_eq!(uri, "ui://counterservice/app.html");

    let tool = mcp::set_tool_app_meta(mcp::create_tool("count", "", SCHEMA), &uri);
    let meta = tool.meta.expect("meta");
    assert_eq!(meta["ui"]["resourceUri"], uri.as_str());
}
