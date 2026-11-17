//! Selecting a database, the schema capability and the typed view, on SQLite in-process:
//! runtime-go's `orm/provider_test.go`. Its transaction tests run with the dialect matrix
//! in `orm_sqlite` and `orm_live`.

mod support {
    #[path = "orm.rs"]
    pub mod orm;
    #[path = "orm_scenarios.rs"]
    pub mod orm_scenarios;
}

use std::sync::Arc;

use database::Error;
use database::orm::{OrmProvider, sqlx};
use database::records::{
    Column, DB, Kind, Provider, Resource, Typed, WatchOptions, check_database_name,
};
use support::orm::{book_res, new_book, sqlite, text};
use support::orm_scenarios;

async fn open() -> DB {
    OrmProvider::new(sqlite().await)
        .set_database("")
        .await
        .unwrap()
}

#[tokio::test]
async fn set_database_reports_its_capabilities() {
    let p = OrmProvider::new(sqlite().await);
    assert_eq!(p.backend(), "orm");
    let db = p.set_database("").await.unwrap();
    assert_eq!((db.backend, db.name.as_str()), ("orm", ""));
    // Transactions and schema are the backend's own; batches are not, and say so.
    db.schema.ensure_schema(&book_res()).await.unwrap();
    assert!(!db.has_batch());
    db.close().await.expect("a no-op: the pool is the caller's");
    assert!(p.pool().dialect() == database::orm::Dialect::Sqlite);
}

#[tokio::test]
async fn ensure_schema_creates_the_table_from_the_descriptor() {
    let db = open().await;
    let res = book_res();
    assert!(
        !db.schema.has_schema(&res).await.unwrap(),
        "exists before anything made it"
    );
    db.schema.ensure_schema(&res).await.unwrap();
    assert!(db.schema.has_schema(&res).await.unwrap());
    db.create(&res, &new_book("books/dune", "Dune", 1965, 1))
        .await
        .unwrap();
    assert_eq!(
        text(&db.get(&res, "books/dune").await.unwrap(), "title"),
        "Dune"
    );
}

#[tokio::test]
async fn ensure_schema_is_idempotent() {
    let db = open().await;
    for i in 0..3 {
        db.schema
            .ensure_schema(&book_res())
            .await
            .unwrap_or_else(|e| panic!("call {i}: {e}"));
    }
}

#[tokio::test]
async fn ensure_schema_refuses_a_resource_without_columns() {
    let mut res = book_res();
    res.columns.clear();
    let err = open().await.schema.ensure_schema(&res).await.unwrap_err();
    assert!(err.to_string().contains("describes no columns"), "{err}");
}

#[tokio::test]
async fn drop_schema() {
    let db = open().await;
    let res = book_res();
    db.schema.ensure_schema(&res).await.unwrap();
    db.schema.drop_schema(&res).await.unwrap();
    assert!(!db.schema.has_schema(&res).await.unwrap());
}

#[tokio::test]
async fn selecting_a_database_redirects_every_resource() {
    let pool = sqlite().await;
    for name in ["tenant_a", "tenant_b"] {
        let attach = format!("ATTACH DATABASE ':memory:' AS {name}");
        sqlx::raw_sql(sqlx::AssertSqlSafe(attach))
            .execute(&pool)
            .await
            .unwrap();
    }
    let p = OrmProvider::new(pool);
    let (a, b) = (
        p.set_database("tenant_a").await.unwrap(),
        p.set_database("tenant_b").await.unwrap(),
    );
    orm_scenarios::tenants(&a, &b).await;

    // A selection overrides the schema a descriptor was generated with; no selection keeps
    // the descriptor's own.
    let mut res = book_res();
    res.schema = "tenant_b".into();
    a.schema.ensure_schema(&res).await.unwrap();
    let main = p.set_database("").await.unwrap();
    assert!(
        !main.schema.has_schema(&book_res()).await.unwrap(),
        "nothing in main"
    );
    assert!(
        !main.schema.has_schema(&res).await.unwrap(),
        "nothing in tenant_b"
    );
    res.schema = "tenant_a".into();
    assert!(
        main.schema.has_schema(&res).await.unwrap(),
        "a made it in tenant_a"
    );
}

#[tokio::test]
async fn database_name_is_checked() {
    let bad = [
        "a\"; DROP TABLE books; --",
        "a.b",
        "a b",
        "a-b",
        "1abc",
        &"x".repeat(64),
    ];
    for name in bad {
        assert!(check_database_name(name).is_err(), "{name:?} was accepted");
    }
    for name in ["", "orders", "tenant_a", "Tenant1", "_private"] {
        check_database_name(name).unwrap();
    }
    let p = OrmProvider::new(sqlite().await);
    let err = p.set_database(bad[0]).await.unwrap_err();
    assert!(
        matches!(err, Error::Invalid(_)),
        "an injected identifier: {err}"
    );
}

#[tokio::test]
async fn missing_capabilities_refuse_by_name() {
    let db = open().await;
    let res = book_res();
    let refused = |e: Error| matches!(e, Error::Unimplemented { backend: "orm", .. });
    assert!(refused(db.batch.get_many(&res, &[]).await.unwrap_err()));
    assert!(refused(db.batch.create_many(&res, &[]).await.unwrap_err()));
    assert!(refused(
        db.watch
            .watch(&res, WatchOptions::default())
            .await
            .err()
            .unwrap()
    ));
}

/// Go's `TestTypedViewChecksTheDescriptorAgainstTheType`, with a well-known type standing in
/// for a generated one: `google.protobuf.Duration`, keyed by `seconds`.
#[tokio::test]
async fn typed_view_checks_the_descriptor_against_the_type() {
    use prost_reflect::ReflectMessage;
    let db = open().await;
    let durations = Resource::new(
        prost_types::Duration::default().descriptor(),
        "durations",
        "seconds",
        vec![
            Column::new("seconds", Kind::Int).primary_key(),
            Column::new("nanos", Kind::Int),
        ],
    );
    db.schema.ensure_schema(&durations).await.unwrap();
    let typed = Typed::<prost_types::Duration>::new(&db, Arc::new(durations)).unwrap();
    let d = prost_types::Duration {
        seconds: 7,
        nanos: 5,
    };
    typed.create(&d).await.unwrap();
    assert_eq!(
        typed.get("7").await.unwrap(),
        d,
        "an integer key, typed by its column"
    );
    let bad_key = typed.get("seven").await.unwrap_err();
    assert!(matches!(bad_key, Error::Invalid(_)), "{bad_key}");

    let mismatch = Typed::<prost_types::Duration>::new(&db, Arc::new(book_res()));
    assert!(mismatch.is_err(), "a Book descriptor for a Duration type");
}
