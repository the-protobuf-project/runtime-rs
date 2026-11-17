//! What `database::cached::wrap` carries across: every capability, transactions that
//! invalidate on commit and not on rollback, and bulk writes that invalidate. Ported from
//! runtime-go's `database/cached/cached_test.go`; the bulk cases are Rust's own.

#[path = "support/records.rs"]
mod books;
#[path = "support/memory_cache.rs"]
mod memory_cache;
#[path = "support/memory_records.rs"]
mod memory_records;

use std::sync::Arc;

use books::{book, books, string_field};
use database::Error;
use database::cached::{Driver, wrap};
use database::records::{DB, ListOptions, Resource, WatchOptions};
use futures::FutureExt;
use memory_cache::memory_cache;
use memory_records::{MemoryRecords, capable, plain};

fn setup() -> (DB, Arc<MemoryRecords>, Resource) {
    let store = MemoryRecords::new();
    (
        wrap(&capable(store.clone()), memory_cache()),
        store,
        books(),
    )
}

fn refused<T>(got: database::Result<T>) -> bool {
    matches!(got, Err(Error::Unimplemented { .. }))
}

// Wrapping a database in a cache must not quietly cost it a capability, nor invent one.
#[tokio::test]
async fn wrap_keeps_capabilities() {
    let inner = plain(MemoryRecords::new());
    let outer = wrap(&inner, memory_cache());
    let res = books();
    assert_eq!(outer.name, inner.name);
    assert_eq!(outer.backend, "fake+cache");
    assert!(!outer.has_batch());
    assert!(refused(
        outer.tx.run(Box::new(|_| async { Ok(()) }.boxed())).await
    ));
    assert!(refused(outer.schema.ensure_schema(&res).await));
    assert!(refused(outer.batch.create_many(&res, &[]).await));
    assert!(refused(
        outer.watch.watch(&res, WatchOptions::default()).await
    ));
    outer.close().await.unwrap();

    let (outer, _, _) = setup();
    assert!(outer.has_batch());
}

// Or a write through a transaction is invisible in a way the same write outside one is not.
#[tokio::test]
async fn a_committed_transaction_invalidates() {
    let (db, _, res) = setup();
    db.create(&res, &book("a", "Before", 1)).await.unwrap();
    db.get(&res, "books/a").await.unwrap(); // populates
    let r = res.clone();
    db.tx
        .run(Box::new(move |tx| {
            async move { tx.update(&r, &book("a", "After", 1)).await.map(|_| ()) }.boxed()
        }))
        .await
        .unwrap();
    let got = db.get(&res, "books/a").await.unwrap();
    assert_eq!(
        string_field(&got, "title"),
        "After",
        "a stale record survived the commit"
    );
}

// A rolled-back transaction wrote nothing, so the cache is still right.
#[tokio::test]
async fn a_rolled_back_transaction_keeps_the_cache() {
    let (db, store, res) = setup();
    db.create(&res, &book("a", "Before", 1)).await.unwrap();
    db.get(&res, "books/a").await.unwrap();
    let before = store.gets();
    let r = res.clone();
    let got = db
        .tx
        .run(Box::new(move |tx| {
            async move {
                tx.update(&r, &book("a", "Lost", 1)).await?;
                Err(Error::Invalid("rolled back".into()))
            }
            .boxed()
        }))
        .await;
    assert!(
        matches!(got, Err(Error::Invalid(ref m)) if m == "rolled back"),
        "{got:?}"
    );
    db.get(&res, "books/a").await.unwrap();
    assert_eq!(
        store.gets(),
        before,
        "a rollback threw away a correct entry"
    );
}

// Reads inside a transaction go to the store, never the cache.
#[tokio::test]
async fn reads_inside_a_transaction_bypass_the_cache() {
    let (db, store, res) = setup();
    db.create(&res, &book("a", "A", 1)).await.unwrap();
    db.get(&res, "books/a").await.unwrap();
    let r = res.clone();
    db.tx
        .run(Box::new(move |tx| {
            async move { tx.get(&r, "books/a").await.map(|_| ()) }.boxed()
        }))
        .await
        .unwrap();
    assert_eq!(store.gets(), 2);
}

#[tokio::test]
async fn a_bulk_create_clears_a_remembered_absence() {
    let (db, _, res) = setup();
    assert!(matches!(
        db.get(&res, "books/b").await,
        Err(Error::NotFound)
    ));
    db.batch
        .create_many(&res, &[book("b", "B", 1)])
        .await
        .unwrap();
    db.get(&res, "books/b")
        .await
        .expect("the bulk-created record is invisible");

    assert!(matches!(
        db.get(&res, "books/c").await,
        Err(Error::NotFound)
    ));
    let r = res.clone();
    db.tx
        .run(Box::new(move |tx| {
            async move {
                tx.batch
                    .create_many(&r, &[book("c", "C", 1)])
                    .await
                    .map(|_| ())
            }
            .boxed()
        }))
        .await
        .unwrap();
    db.get(&res, "books/c")
        .await
        .expect("a committed bulk create is invisible");
}

// List, count and exists are never cached, so they see every write at once.
#[tokio::test]
async fn whole_set_reads_pass_through() {
    let (db, store, res) = setup();
    assert!(!db.exists(&res, "books/a").await.unwrap());
    db.create(&res, &book("a", "A", 1)).await.unwrap();
    assert!(db.exists(&res, "books/a").await.unwrap());
    assert_eq!(db.count(&res, &ListOptions::default()).await.unwrap(), 1);
    assert_eq!(store.gets(), 0);

    let bare = Driver::new(store.clone(), memory_cache());
    assert!(bare.unwrap().exists(&res, "books/a").await.unwrap());
}
