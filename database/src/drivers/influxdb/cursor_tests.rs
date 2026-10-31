//! Cursor identity, scope binding and parameter safety checks.
use super::{cursor, sql};
use crate::{Column, Kind, Range, Resource, Role};
use serde_json::json;

fn resource() -> Resource {
    Resource::new(
        "samples",
        vec![
            Column::new("device", Kind::String, Role::Tag),
            Column::new("channel", Kind::String, Role::Tag),
            Column::new("time", Kind::Timestamp, Role::Timestamp),
            Column::new("value", Kind::Float, Role::Field),
        ],
    )
    .unwrap()
}

#[test]
fn continuation_uses_all_identity_columns_and_never_interpolates_values() {
    let resource = resource();
    let mut range = Range::new(0, 100);
    range.descending = true;
    let row =
        serde_json::from_value(json!({"time": 10, "device": "a' OR true --", "channel": "c"}))
            .unwrap();
    let mut query = sql::range(&resource, &range).unwrap();
    let scope = cursor::scope("host", "db", &resource, &query);
    let token = cursor::encode(&resource, &row, scope.clone()).unwrap();
    cursor::apply(&mut query, &resource, &range, &scope, &token).unwrap();
    assert!(query.sql.contains("\"time\" < $cursor0"));
    assert!(
        query
            .sql
            .contains("\"device\" = $cursor1 AND \"channel\" > $cursor2")
    );
    assert!(!query.sql.contains("OR true --"));
    assert_eq!(query.params["cursor1"], "a' OR true --");
}

#[test]
fn tokens_are_rejected_for_another_scope_or_malformed_input() {
    let resource = resource();
    let range = Range::new(0, 100);
    let row = serde_json::from_value(json!({"time": 10, "device": "a", "channel": "c"})).unwrap();
    let token = cursor::encode(&resource, &row, "original".into()).unwrap();
    for token in [token.as_str(), "broken-token", ""] {
        let mut query = sql::range(&resource, &range).unwrap();
        assert!(cursor::apply(&mut query, &resource, &range, "different", token).is_err());
    }
    let null = serde_json::from_value(json!({"time": 10, "device": null, "channel": "c"})).unwrap();
    assert!(cursor::encode(&resource, &null, "scope".into()).is_err());
}
