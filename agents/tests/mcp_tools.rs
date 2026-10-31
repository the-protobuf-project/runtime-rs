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

    let payload: serde_json::Value =
        serde_json::from_str(&result.content[0].as_text().expect("a text block").text)
            .expect("the payload is JSON");
    // Go's codes.Code.String(), which is what clients of the Go runtime already parse.
    assert_eq!(payload["code"], "NotFound");
    assert_eq!(payload["message"], "no such todo");
    assert!(
        payload.get("details").is_none(),
        "empty details are omitted: {payload}"
    );
}

#[test]
fn every_code_is_spelled_as_go_spells_it() {
    assert_eq!(
        mcp::code_string(tonic::Code::PermissionDenied),
        "PermissionDenied"
    );
    assert_eq!(mcp::code_string(tonic::Code::Cancelled), "Canceled");
    assert_eq!(mcp::code_string(tonic::Code::Ok), "OK");
}

#[test]
fn status_details_reach_the_payload() {
    use tonic_types::{ErrorDetails, StatusExt};

    let mut details = ErrorDetails::with_error_info(
        "QUOTA",
        "example.com",
        [("limit".to_string(), "5".to_string())],
    );
    details.add_bad_request_violation("name", "must not be empty");
    let status =
        tonic::Status::with_error_details(tonic::Code::InvalidArgument, "bad request", details);

    let result = mcp::handle_error(&status);
    let payload: serde_json::Value =
        serde_json::from_str(&result.content[0].as_text().expect("a text block").text)
            .expect("the payload is JSON");
    assert_eq!(payload["code"], "InvalidArgument");
    let details = payload["details"].as_array().expect("details");
    assert!(
        details.iter().any(|d| d["reason"] == "QUOTA"
            && d["domain"] == "example.com"
            && d["metadata"]["limit"] == "5"),
        "{payload}"
    );
    assert!(
        details
            .iter()
            .any(|d| d["field_violations"][0]["field"] == "name"),
        "{payload}"
    );
}

#[test]
fn any_error_with_a_status_inside_keeps_its_code() {
    let status = tonic::Status::unavailable("down");
    let result = mcp::handle_any_error(&status);
    let text = &result.content[0].as_text().expect("text").text;
    assert!(text.contains("\"Unavailable\""), "{text}");

    let plain = std::io::Error::other("disk on fire");
    let result = mcp::handle_any_error(&plain);
    assert_eq!(
        result.content[0].as_text().expect("text").text,
        "disk on fire"
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
