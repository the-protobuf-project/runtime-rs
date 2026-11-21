//! The D1 backend against a stand-in for D1's REST query endpoint backed by a real SQLite,
//! so the column affinity the driver leans on, text bound into an `INTEGER` column coming
//! back as a number, is SQLite's own behavior rather than the fake's idea of it. runtime-go's
//! `d1/d1_test.go`, plus the AIP-160 conformance table every records provider runs.

mod support {
    #[path = "d1_fake.rs"]
    pub mod d1_fake;
    #[path = "filter_conformance.rs"]
    pub mod filter_conformance;
    #[path = "records.rs"]
    pub mod records;
}

use database::Error;
use database::records::{DB, ListOptions, Provider, Resource};
use prost_reflect::{DynamicMessage, Value as P};
use support::d1_fake::{MAIN, serve};
use support::records::{book, books, string_field};

async fn setup() -> (DB, Resource) {
    let (p, _) = serve(&[("main", MAIN)]).await;
    let db = p.set_database("").await.unwrap();
    let res = books();
    db.schema.ensure_schema(&res).await.unwrap();
    (db, res)
}

#[tokio::test]
async fn d1_round_trip_keeps_every_kind() {
    let (db, res) = setup().await;
    let mut want = book("dune", "Dune", 412);
    want.set_field_by_name("subtitle", P::String("A Novel".into()));
    want.set_field_by_name("cover", P::Bytes(vec![0u8, 1, 2, 0xff].into()));
    let stored = db.create(&res, &want).await.unwrap().message;
    let got = db.get(&res, "books/dune").await.unwrap();
    // What came back is what was stored, audit times included, to the nanosecond.
    assert_eq!(got, stored);
    assert_eq!(
        got.get_field_by_name("isbn").unwrap().as_u64(),
        Some(9_780_441_013_593)
    );
    assert!(got.get_field_by_name("create_time").is_some());
}

#[tokio::test]
async fn d1_duplicates_are_already_exists() {
    let (db, res) = setup().await;
    db.create(&res, &book("dune", "Dune", 412)).await.unwrap();
    let key = db.create(&res, &book("dune", "Other", 1)).await;
    assert!(matches!(key, Err(Error::AlreadyExists)), "{key:?}");
    let unique = db.create(&res, &book("other", "Dune", 1)).await;
    assert!(matches!(unique, Err(Error::AlreadyExists)), "{unique:?}");
}

#[tokio::test]
async fn d1_missing_records_are_not_found() {
    let (db, res) = setup().await;
    assert!(matches!(
        db.get(&res, "books/none").await,
        Err(Error::NotFound)
    ));
    assert!(matches!(
        db.update(&res, &book("none", "X", 1)).await,
        Err(Error::NotFound)
    ));
    assert!(matches!(
        db.delete(&res, "books/none").await,
        Err(Error::NotFound)
    ));
    assert!(!db.exists(&res, "books/none").await.unwrap());
}

#[tokio::test]
async fn d1_update_and_delete() {
    let (db, res) = setup().await;
    db.create(&res, &book("dune", "Dune", 412)).await.unwrap();
    db.update(&res, &book("dune", "Dune (1965)", 896))
        .await
        .unwrap();
    let got = db.get(&res, "books/dune").await.unwrap();
    assert_eq!(string_field(&got, "title"), "Dune (1965)");
    assert_eq!(got.get_field_by_name("pages").unwrap().as_i32(), Some(896));
    db.delete(&res, "books/dune").await.unwrap();
    assert!(!db.exists(&res, "books/dune").await.unwrap());
}

#[tokio::test]
async fn d1_matches_the_filter_conformance_table() {
    let (db, res) = setup().await;
    support::filter_conformance::run("d1", &db, &res, book).await;
}

/// Pages bind as text; affinity must still order them as numbers: 1000 after 999.
#[tokio::test]
async fn d1_orders_numerically_and_pages() {
    let (db, res) = setup().await;
    let msgs: Vec<DynamicMessage> = [999, 1000, 5, 80]
        .iter()
        .map(|n| book(&n.to_string(), &format!("t{n}"), *n))
        .collect();
    db.batch.create_many(&res, &msgs).await.unwrap();
    let opts = ListOptions::default().order_by("pages desc").page_size(3);
    let page = db.list(&res, &opts).await.unwrap();
    let pages: Vec<i32> = page
        .items
        .iter()
        .map(|m| m.get_field_by_name("pages").unwrap().as_i32().unwrap())
        .collect();
    assert_eq!(pages, vec![1000, 999, 80]);
    assert_eq!(page.total, 4);
    let rest = db
        .list(&res, &opts.clone().page_token(page.next_page_token))
        .await
        .unwrap();
    assert_eq!(rest.items.len(), 1);
    assert!(rest.next_page_token.is_empty());
}

#[tokio::test]
async fn d1_get_many_keeps_order_and_gaps_past_the_bind_limit() {
    let (db, res) = setup().await;
    let msgs: Vec<DynamicMessage> = (0..120)
        .map(|i| book(&format!("{i:03}"), &format!("t{i}"), i))
        .collect();
    db.batch.create_many(&res, &msgs).await.unwrap();
    let keys: Vec<String> = (0..150).rev().map(|i| format!("books/{i:03}")).collect();
    let got = db.batch.get_many(&res, &keys).await.unwrap();
    for (key, m) in keys.iter().zip(&got) {
        let have = m.as_ref().map(|m| string_field(m, "name"));
        let want = (key.as_str() < "books/120").then(|| key.clone());
        assert_eq!(have, want, "{key}");
    }
}

#[tokio::test]
async fn d1_a_batch_is_one_unit() {
    let (db, res) = setup().await;
    let msgs = [book("a", "Same", 1), book("b", "Same", 2)];
    let out = db.batch.create_many(&res, &msgs).await;
    assert!(matches!(out, Err(Error::AlreadyExists)), "{out:?}");
    assert!(
        !db.exists(&res, "books/a").await.unwrap(),
        "half a failed batch landed"
    );
}

#[tokio::test]
async fn d1_schema_and_capabilities() {
    let (db, res) = setup().await;
    assert!(db.schema.has_schema(&res).await.unwrap());
    db.schema.ensure_schema(&res).await.unwrap();
    let tx = db.tx.run(Box::new(|_| Box::pin(async { Ok(()) }))).await;
    assert!(matches!(tx, Err(Error::Unimplemented { .. })), "{tx:?}");
    db.schema.drop_schema(&res).await.unwrap();
    assert!(!db.schema.has_schema(&res).await.unwrap());
}

#[tokio::test]
async fn d1_set_database_by_name() {
    let (p, _) = serve(&[
        ("tenant_a", "22222222-2222-2222-2222-222222222222"),
        ("tenant_ab", "33333333-3333-3333-3333-333333333333"),
    ])
    .await;
    let res = books();
    let a = p.set_database("tenant_a").await.unwrap();
    a.schema.ensure_schema(&res).await.unwrap();
    let ab = p.set_database("tenant_ab").await.unwrap();
    assert!(
        !ab.schema.has_schema(&res).await.unwrap(),
        "tenant_ab sees tenant_a's table: the name matched loosely"
    );
    let unknown = p.set_database("tenant_b").await;
    assert!(
        matches!(&unknown, Err(Error::Invalid(m)) if m.contains("wrangler d1 create")),
        "{:?}",
        unknown.err()
    );
    // The configured id is not served here.
    assert!(p.set_database("").await.is_err());
    assert!(p.set_database("bad name;").await.is_err());
}
