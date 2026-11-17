//! The record-store contract without a backend: the filter subset, paging tokens, managed
//! columns, the message/column bridge and capability refusals. Ported from runtime-go's
//! `database/core` and `database/store` tests.

#[path = "support/records.rs"]
mod support;

use std::sync::Arc;

use database::Error;
use database::records::core::{self, Clause};
use database::records::{
    Capabilities, DB, Driver, ListOptions, ListResult, Resource, Value, WriteResult,
    check_database_name, columns_to_message, key_of, message_to_columns,
};
use prost_reflect::DynamicMessage;

fn clause(column: &str, op: &str, value: &str) -> Clause {
    Clause {
        column: column.into(),
        op: op.into(),
        value: value.into(),
    }
}

#[test]
fn filters_parse_as_conjunctions() {
    for (expr, want) in [
        ("age >= 20", vec![clause("age", ">=", "20")]),
        (
            "a = 1 AND b < 2",
            vec![clause("a", "=", "1"), clause("b", "<", "2")],
        ),
        (
            "a = 1 and b = 2",
            vec![clause("a", "=", "1"), clause("b", "=", "2")],
        ),
        (r#"title = "Dune""#, vec![clause("title", "=", "Dune")]),
        ("", vec![]),
        (r#"title = "a != b""#, vec![clause("title", "=", "a != b")]),
        (
            r#"title = "x AND y""#,
            vec![clause("title", "=", "x AND y")],
        ),
        (r#"name = """#, vec![clause("name", "=", "")]),
    ] {
        assert_eq!(core::parse_filter(expr).unwrap(), want, "{expr}");
    }
}

#[test]
fn filters_refuse_what_they_cannot_express() {
    for expr in ["title LIKE Dune", "age", "= 20", "age >=", "a = 1 OR b = 2"] {
        let err = core::parse_filter(expr).expect_err(expr).to_string();
        assert!(
            err.contains(expr) || err.contains("not understood"),
            "{expr}: {err}"
        );
    }
}

#[test]
fn page_tokens_are_offsets() {
    assert_eq!(core::page_size(0), core::DEFAULT_PAGE_SIZE);
    assert_eq!(core::decode_token(""), 0);
    assert_eq!(core::decode_token("junk"), 0);
    assert_eq!(core::decode_token("-3"), 0);
    assert_eq!(core::encode_token(0, 2, 5), "2");
    assert_eq!(core::encode_token(4, 1, 5), "");
    assert_eq!(core::fetch_limit(2, true), 3);
    assert_eq!(
        core::trim_page(vec![1, 2, 3], 0, 2, -1, true),
        (vec![1, 2], "2".to_owned())
    );
    assert_eq!(
        core::trim_page(vec![1, 2], 0, 2, -1, true),
        (vec![1, 2], String::new())
    );
}

#[test]
fn managed_columns_are_filled_by_the_driver() {
    let res = support::books();
    let mut cols = message_to_columns(&res, &support::book("dune", "Dune", 412)).unwrap();
    // Backed by an unset field, an audit column reaches the driver as null to overwrite.
    assert!(support::is_null(cols.get("create_time")), "{cols:?}");
    core::fill_managed(&res, &mut cols, true);
    let (Some(Value::Timestamp(c)), Some(Value::Timestamp(u))) =
        (cols.get("create_time"), cols.get("update_time"))
    else {
        panic!("audit times filled on create: {cols:?}")
    };
    assert!(c <= u);
    cols.insert("create_time".into(), Value::Null);
    core::fill_managed(&res, &mut cols, false);
    assert!(
        support::is_null(cols.get("create_time")),
        "create time is not touched on update"
    );
    assert!(core::has_managed(&res));
}

#[test]
fn a_message_round_trips_through_columns() {
    let res = support::books();
    let book = support::book("dune", "Dune", 412);
    let cols = message_to_columns(&res, &book).unwrap();
    assert_eq!(cols.get("name"), Some(&Value::String("books/dune".into())));
    assert_eq!(cols.get("pages"), Some(&Value::Int(412)));
    assert_eq!(cols.get("isbn"), Some(&Value::Uint(9_780_441_013_593)));
    assert_eq!(cols.get("status"), Some(&Value::Int(2)));
    assert!(
        support::is_null(cols.get("subtitle")),
        "an unset optional field is null"
    );
    let back = columns_to_message(&res, &cols).unwrap();
    assert_eq!(back, book);
    assert_eq!(key_of(&res, &book).unwrap(), "books/dune");
}

#[test]
fn timestamps_read_from_the_layouts_databases_print() {
    let res = support::books();
    let mut cols = message_to_columns(&res, &support::book("dune", "Dune", 412)).unwrap();
    for text in [
        "2026-11-17T02:06:09Z",
        "2026-11-17 02:06:09.5+00:00",
        "2026-11-17 02:06:09",
        "2026-11-17",
    ] {
        cols.insert("create_time".into(), Value::String(text.into()));
        let msg = columns_to_message(&res, &cols).unwrap_or_else(|e| panic!("{text}: {e}"));
        assert!(msg.has_field_by_name("create_time"), "{text}");
    }
    cols.insert("create_time".into(), Value::String("yesterday".into()));
    assert!(columns_to_message(&res, &cols).is_err());
}

#[test]
fn database_names_go_where_identifiers_go() {
    for ok in ["", "tenant_a", "_x", "a1"] {
        check_database_name(ok).unwrap();
    }
    for bad in ["1a", "a-b", "a;drop", &"x".repeat(64)] {
        assert!(check_database_name(bad).is_err(), "{bad}");
    }
}

struct Nothing;

#[async_trait::async_trait]
impl Driver for Nothing {
    async fn create(&self, _: &Resource, m: &DynamicMessage) -> database::Result<WriteResult> {
        Ok(WriteResult::done(m.clone()))
    }
    async fn get(&self, _: &Resource, _: &str) -> database::Result<DynamicMessage> {
        Err(Error::NotFound)
    }
    async fn update(&self, _: &Resource, m: &DynamicMessage) -> database::Result<WriteResult> {
        Ok(WriteResult::done(m.clone()))
    }
    async fn delete(&self, _: &Resource, _: &str) -> database::Result<()> {
        Ok(())
    }
    async fn list(&self, _: &Resource, _: &ListOptions) -> database::Result<ListResult> {
        Ok(ListResult::default())
    }
    async fn count(&self, _: &Resource, _: &ListOptions) -> database::Result<i64> {
        Ok(0)
    }
    async fn exists(&self, _: &Resource, _: &str) -> database::Result<bool> {
        Ok(false)
    }
}

#[tokio::test]
async fn missing_capabilities_refuse_naming_the_backend() {
    let db = DB::build("nothing", "", Capabilities::new(Arc::new(Nothing)), None);
    let res = support::books();
    let err = db.schema.ensure_schema(&res).await.unwrap_err();
    assert!(
        matches!(
            err,
            Error::Unimplemented {
                backend: "nothing",
                ..
            }
        ),
        "{err}"
    );
    assert!(db.batch.get_many(&res, &[]).await.is_err());
    assert!(!db.has_batch());
    assert!(matches!(db.get(&res, "x").await, Err(Error::NotFound)));
    db.close().await.unwrap();
}
