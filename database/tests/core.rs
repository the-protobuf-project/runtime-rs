//! Descriptor validation and capability behavior without a server.
mod common;
use common::{record, resource};
use database::{Capabilities, Column, DB, Error, Kind, Range, Resource, Role};
use serde_json::json;

#[test]
fn invalid_schema_is_rejected_at_construction() {
    let columns = resource().columns().to_vec();
    assert!(Resource::new("readings; DROP TABLE x", columns.clone()).is_err());
    let mut duplicate = columns.clone();
    duplicate.push(columns[0].clone());
    assert!(Resource::new("readings", duplicate).is_err());
    assert!(Resource::new("readings", vec![Column::new("x", Kind::Int, Role::Tag)]).is_err());
    assert!(Resource::new("readings", columns[..2].to_vec()).is_err());
}

#[test]
fn records_are_complete_typed_and_do_not_silently_drop_data() {
    let res = resource();
    res.validate(&record()).unwrap();
    let _: common::Reading =
        serde_json::from_value(serde_json::to_value(record()).unwrap()).unwrap();
    let mut row = record();
    row.insert("temperature".into(), json!("22.5"));
    assert!(res.validate(&row).is_err());
    row = record();
    row.insert("device".into(), json!(""));
    assert!(res.validate(&row).is_err());
    row.insert("device".into(), json!("injected\npoint"));
    assert!(res.validate(&row).is_err());
    row = record();
    row.remove("observed_at");
    assert!(res.validate(&row).is_err());
    row = record();
    row.insert("undeclared".into(), json!(1));
    assert!(res.validate(&row).is_err());
}

#[test]
fn unsupported_capabilities_name_the_backend() {
    let db = DB::new("empty", "tenant".into(), Capabilities::default());
    assert!(matches!(db.reader(), Err(Error::Unsupported { .. })));
    assert!(matches!(db.maintenance(), Err(Error::Unsupported { .. })));
    assert!(matches!(db.row_deletion(), Err(Error::Unsupported { .. })));
    assert!(matches!(
        db.records(),
        Err(Error::Unsupported {
            backend: "empty",
            ..
        })
    ));
    assert!(matches!(
        db.clone().series(),
        Err(Error::Unsupported { .. })
    ));
}

#[test]
fn query_bounds_and_limits_are_explicit() {
    assert!(Range::new(1, 1).validate().is_err());
    assert!(Range::new(2, 1).validate().is_err());
    let mut range = Range::new(1, 2);
    range.limit = 10001;
    assert!(range.validate().is_err());
    range.limit = 0;
    assert!(range.validate().is_err());
    range.limit = 100;
    range.validate().unwrap();
}

#[test]
fn record_descriptors_do_not_require_a_time_series_schema() {
    let resource = Resource::new(
        "books",
        vec![
            Column::new("id", Kind::String, Role::Key),
            Column::new("title", Kind::String, Role::Field),
        ],
    )
    .unwrap();
    assert_eq!(resource.key_column(), Some("id"));
    assert!(resource.time_column().is_err());
}
