//! Query construction and SDK codec checks without network I/O.
use super::{aggregate, codec, sql};
use crate::store::window::{Aggregate, Comparison, Predicate, Range, Reduce, Reduction};
use crate::store::{Column, Kind, Resource, Role};
use influxdb3_client::Precision;
use serde_json::json;

fn resource() -> Resource {
    Resource::new(
        "readings",
        vec![
            Column::new("device", Kind::String, Role::Tag),
            Column::new("value", Kind::Uint, Role::Field),
            Column::new("observed_at", Kind::Timestamp, Role::Timestamp),
        ],
    )
    .unwrap()
}

#[test]
fn predicates_bind_values_and_keep_both_comparisons_on_one_column() {
    let mut range = Range::new(0, 1_000_000_000);
    let injection = "a' OR true --";
    range.predicates = vec![
        Predicate {
            column: "device".into(),
            comparison: Comparison::Eq,
            value: json!(injection),
        },
        Predicate {
            column: "value".into(),
            comparison: Comparison::Ge,
            value: json!(10),
        },
        Predicate {
            column: "value".into(),
            comparison: Comparison::Le,
            value: json!(20),
        },
    ];
    let query = sql::range(&resource(), &range).unwrap();
    assert!(!query.sql.contains(injection));
    assert_eq!(query.params["p0"], injection);
    assert!(query.sql.contains("\"value\" >= $p1 AND \"value\" <= $p2"));
    assert!(query.sql.contains("\"time\" AS \"observed_at\""));
    assert!(
        query
            .sql
            .ends_with("ORDER BY \"time\" ASC, \"device\" ASC LIMIT 100")
    );
    assert_eq!(query.params["end"], "1970-01-01T00:00:01.000000000Z");
}

#[test]
fn unknown_or_wrongly_typed_predicates_fail_before_a_query() {
    let mut range = Range::new(0, 100);
    range.predicates.push(Predicate {
        column: "typo".into(),
        comparison: Comparison::Eq,
        value: json!(1),
    });
    assert!(sql::range(&resource(), &range).is_err());
    range.predicates[0].column = "value".into();
    range.predicates[0].value = json!(-1);
    assert!(sql::range(&resource(), &range).is_err());
}

#[test]
fn aggregate_checks_groups_aliases_and_reduces_on_server() {
    let mut query = Aggregate {
        range: Range::new(0, 1_000_000_000),
        every_seconds: 60,
        reduce: vec![Reduce {
            column: "value".into(),
            function: Reduction::Mean,
            alias: "mean".into(),
        }],
        group_by: vec!["device".into()],
    };
    let sql = aggregate::aggregate(&resource(), &query).unwrap().sql;
    assert!(sql.contains("date_bin(INTERVAL '60 seconds', \"time\")"));
    assert!(sql.contains("avg(\"value\") AS \"mean\""));
    query.reduce[0].alias = "device".into();
    assert!(aggregate::aggregate(&resource(), &query).is_err());
    query.reduce[0].alias = "mean".into();
    query.group_by = vec!["value".into()];
    assert!(aggregate::aggregate(&resource(), &query).is_err());
    query.every_seconds = 0;
    assert!(aggregate::aggregate(&resource(), &query).is_err());
}

#[test]
fn sdk_point_retains_full_unsigned_precision_and_nanoseconds() {
    let row = serde_json::from_value(json!({
        "device": "room, a=b", "value": u64::MAX, "observed_at": -1,
    }))
    .unwrap();
    let point = codec::point(&resource(), &row).unwrap();
    let line = point.to_line_protocol(Precision::Nanosecond).unwrap();
    assert!(line.contains("18446744073709551615u"));
    assert!(line.contains(r"device=room\,\ a\=b"));
    assert!(line.ends_with(" -1"));
}
