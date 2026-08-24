//! Elicitation, completions, and cache hints: the parts of a generated server declared
//! in the proto and resolved at runtime.

#![cfg(feature = "mcp")]

use agents::mcp;
use serde_json::json;

#[test]
fn an_elicitation_form_carries_its_fields_and_which_are_required() {
    let fields = vec![
        mcp::ElicitField::new("confirm", "Really delete?", "boolean").required(),
        mcp::ElicitField::new("priority", "How urgent?", "string").with_enum(
            vec!["High".into(), "Low".into()],
            vec!["PRIORITY_HIGH".into(), "PRIORITY_LOW".into()],
        ),
    ];

    let schema = mcp::elicit_schema(&fields);
    assert!(schema.properties.contains_key("confirm"));
    assert!(schema.properties.contains_key("priority"));
    assert_eq!(
        schema.required.as_deref(),
        Some(&["confirm".to_string()][..])
    );
}

#[test]
fn an_accepted_answer_is_mapped_back_to_the_proto_enum_name() {
    let fields = vec![
        mcp::ElicitField::new("priority", "How urgent?", "string").with_enum(
            vec!["High".into(), "Low".into()],
            vec!["PRIORITY_HIGH".into(), "PRIORITY_LOW".into()],
        ),
    ];

    // The form showed "High"; the request behind it only decodes "PRIORITY_HIGH".
    let merged = mcp::merge_elicit_result(
        json!({"name": "todo-1"}),
        &json!({"priority": "High"}),
        &fields,
    );
    assert_eq!(
        merged,
        json!({"name": "todo-1", "priority": "PRIORITY_HIGH"})
    );
}

#[test]
fn an_answer_with_no_enum_mapping_is_merged_verbatim() {
    let fields = vec![mcp::ElicitField::new("note", "Anything to add?", "string")];
    let merged = mcp::merge_elicit_result(json!({"to": 3}), &json!({"note": "hi"}), &fields);
    assert_eq!(merged, json!({"to": 3, "note": "hi"}));
}

#[test]
fn completions_narrow_by_what_has_been_typed() {
    let completions = mcp::EnumCompletions::new(
        [(
            "review:priority".to_string(),
            vec!["High".to_string(), "Higher".to_string(), "Low".to_string()],
        )]
        .into_iter()
        .collect(),
    );

    let result = completions.complete("review", "priority", "hig");
    assert_eq!(result.completion.values, vec!["High", "Higher"]);

    // An argument nothing was declared for completes to nothing, rather than failing.
    let unknown = completions.complete("review", "nobody", "");
    assert!(unknown.completion.values.is_empty());
}

#[test]
fn cache_hints_fall_back_from_a_resource_to_the_service_default() {
    let mut hints = mcp::CacheHints {
        list: Some(mcp::CacheHint::public(60_000)),
        ..Default::default()
    };
    hints
        .resources
        .insert("todo://mine".into(), mcp::CacheHint::private(5_000));

    assert_eq!(
        hints.for_resource("todo://mine"),
        Some(mcp::CacheHint::private(5_000))
    );
    assert_eq!(
        hints.for_resource("todo://public"),
        Some(mcp::CacheHint::public(60_000)),
        "an unlisted resource should still get the service default"
    );
    assert!(!hints.is_empty());
}

#[test]
fn a_private_hint_is_stamped_onto_a_read_result() {
    let mut hints = mcp::CacheHints {
        list: Some(mcp::CacheHint::public(60_000)),
        ..Default::default()
    };
    hints
        .resources
        .insert("todo://mine".into(), mcp::CacheHint::private(5_000));

    let stamped = hints.apply_to_read("todo://mine", mcp::default_resource_result("todo://mine"));
    assert_eq!(stamped.ttl_ms, Some(5_000));
    assert_eq!(stamped.cache_scope, Some(rmcp::model::CacheScope::Private));

    // A resource with nothing declared still picks up the service default.
    let fallback = hints.apply_to_read("todo://any", mcp::default_resource_result("todo://any"));
    assert_eq!(fallback.ttl_ms, Some(60_000));

    // And with no hints at all, nothing is stamped.
    let bare = mcp::CacheHints::default()
        .apply_to_read("todo://any", mcp::default_resource_result("todo://any"));
    assert_eq!(bare.ttl_ms, None);
}
