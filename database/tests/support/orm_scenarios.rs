//! What the SQL backend claims, written once and run on every engine: SQLite in-process, and
//! Postgres and MySQL in the live suite. Ported from runtime-go's `orm/dialect_test.go` and
//! `orm/provider_test.go`, where the same assertions run against each dialect.

#![allow(dead_code)]

use super::orm::{int, new_book, text, unique_res};
use database::Error;
use database::records::{DB, ListOptions, Resource};

/// A fresh table on `db`, dropped by the caller with [`drop`].
pub async fn table(db: &DB) -> Resource {
    let res = unique_res();
    db.schema.ensure_schema(&res).await.unwrap();
    res
}

/// Drops `res`'s table.
pub async fn drop(db: &DB, res: &Resource) {
    db.schema.drop_schema(res).await.unwrap();
}

/// Go's `TestEveryDialect`: the column types, identifier quoting, unique enforcement, update,
/// a filtered and ordered listing, a filtered count and delete, on one engine.
pub async fn every_dialect(db: &DB) {
    let res = table(db).await;
    db.create(&res, &new_book("books/dune", "Dune", 1965, 1))
        .await
        .unwrap();
    // A wrongly quoted predicate is always false on MySQL: every read silently empty.
    let got = db
        .get(&res, "books/dune")
        .await
        .expect("the predicate matched nothing");
    assert_eq!(text(&got, "title"), "Dune");

    // The unique title is enforced; the refused row is not counted below.
    let dup = db
        .create(&res, &new_book("books/other", "Dune", 2021, 1))
        .await;
    assert!(matches!(dup, Err(Error::AlreadyExists)), "{dup:?}");

    db.update(&res, &new_book("books/dune", "Dune (revised)", 1965, 1))
        .await
        .unwrap();
    let got = db.get(&res, "books/dune").await.unwrap();
    assert_eq!(text(&got, "title"), "Dune (revised)");

    for i in 0..5 {
        let id = format!("books/{i:02}");
        db.create(&res, &new_book(&id, &id, 2000 + i, 1))
            .await
            .unwrap();
    }
    let opts = ListOptions::default()
        .filter("published_year >= 2003")
        .order_by("published_year desc")
        .page_size(10);
    let out = db.list(&res, &opts).await.unwrap();
    assert_eq!(out.items.len(), 2, "filtered list");
    assert_eq!(
        int(&out.items[0], "published_year"),
        2004,
        "first ordered row"
    );
    assert_eq!(out.total, 2);
    let n = db
        .count(
            &res,
            &ListOptions::default().filter("published_year >= 2003"),
        )
        .await;
    assert_eq!(n.unwrap(), 2, "filtered count");

    db.delete(&res, "books/dune").await.unwrap();
    let gone = db.get(&res, "books/dune").await;
    assert!(matches!(gone, Err(Error::NotFound)), "{gone:?}");
    drop(db, &res).await;
}

/// Go's `TestEnsureSchemaCreatesTheTableFromTheDescriptor`, `TestEnsureSchemaIsIdempotent`
/// and `TestDropSchema`.
pub async fn schema_lifecycle(db: &DB) {
    let res = unique_res();
    assert!(
        !db.schema.has_schema(&res).await.unwrap(),
        "exists before anything made it"
    );
    for _ in 0..3 {
        db.schema.ensure_schema(&res).await.expect("idempotent");
    }
    assert!(db.schema.has_schema(&res).await.unwrap());
    db.create(&res, &new_book("books/dune", "Dune", 1965, 1))
        .await
        .unwrap();
    assert_eq!(
        text(&db.get(&res, "books/dune").await.unwrap(), "title"),
        "Dune"
    );
    db.schema.drop_schema(&res).await.unwrap();
    assert!(!db.schema.has_schema(&res).await.unwrap(), "dropped");
    db.schema
        .drop_schema(&res)
        .await
        .expect("dropping what is gone");
}

/// Go's `TestSelectingADatabaseRedirectsEveryResource`: one descriptor, two tenants.
pub async fn tenants(a: &DB, b: &DB) {
    let res = unique_res();
    a.schema.ensure_schema(&res).await.unwrap();
    b.schema.ensure_schema(&res).await.unwrap();
    a.create(&res, &new_book("books/dune", "Dune", 1965, 1))
        .await
        .unwrap();
    let other = b.get(&res, "books/dune").await;
    assert!(
        matches!(other, Err(Error::NotFound)),
        "b sees a's record: {other:?}"
    );
    a.get(&res, "books/dune")
        .await
        .expect("a lost its own record");
    assert!(a.schema.has_schema(&res).await.unwrap() && b.schema.has_schema(&res).await.unwrap());
    assert_eq!((a.name.as_str(), b.name.as_str()), ("tenant_a", "tenant_b"));
    drop(a, &res).await;
    drop(b, &res).await;
}

/// Go's `TestOmitTotalPagesToTheEnd`: without a count, paging ends on exactly the right page,
/// including when the last one comes out full.
pub async fn omit_total_pages_to_the_end(db: &DB) {
    let res = table(db).await;
    const TOTAL: usize = 20;
    for i in 0..TOTAL {
        let (id, title) = (format!("books/{i:02}"), format!("t{i:02}"));
        db.create(&res, &new_book(&id, &title, 2000, 1))
            .await
            .unwrap();
    }
    let (mut seen, mut token) = (Vec::new(), String::new());
    for pages in 0.. {
        assert!(pages <= TOTAL, "paging did not terminate");
        let opts = ListOptions::default()
            .page_size(5)
            .page_token(token)
            .omit_total(true);
        let out = db.list(&res, &opts).await.unwrap();
        assert_eq!(out.total, -1);
        seen.extend(out.items.iter().map(|m| text(m, "id")));
        if out.next_page_token.is_empty() {
            break;
        }
        token = out.next_page_token;
    }
    assert_eq!(seen.len(), TOTAL);
    assert!(
        seen.windows(2).all(|w| w[0] < w[1]),
        "duplicate or out of order: {seen:?}"
    );
    drop(db, &res).await;
}
