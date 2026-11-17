//! The read path of `database::cached` over an in-memory store and the cache crate's own
//! in-memory Aside: hits, invalidation, remembered absence, collapsed misses and overload.
//! Ported from runtime-go's `database/cached/cached_test.go`.

#[path = "support/records.rs"]
mod books;
#[path = "support/memory_cache.rs"]
mod memory_cache;
#[path = "support/memory_records.rs"]
mod memory_records;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use books::{book, books, string_field};
use database::Error;
use database::cached::{Cache, wrap};
use database::records::{DB, Resource};
use memory_cache::{Refusing, memory_cache};
use memory_records::{MemoryRecords, plain};
use prost_reflect::Value;

fn setup_with(cache: Arc<dyn Cache>) -> (DB, Arc<MemoryRecords>, Resource) {
    let store = MemoryRecords::new();
    (wrap(&plain(store.clone()), cache), store, books())
}

fn setup() -> (DB, Arc<MemoryRecords>, Resource) {
    setup_with(memory_cache())
}

#[tokio::test]
async fn a_second_read_does_not_reach_the_store() {
    let (db, store, res) = setup();
    db.create(&res, &book("dune", "Dune", 412)).await.unwrap();
    for _ in 0..5 {
        db.get(&res, "books/dune").await.unwrap();
    }
    assert_eq!(
        store.gets(),
        1,
        "five reads reached the store more than once"
    );
}

// The record goes through the cache as wire bytes: bytes that are not UTF-8 and integers
// past 2^53 come back exactly.
#[tokio::test]
async fn values_survive_the_cache() {
    let (db, _, res) = setup();
    let cover = vec![0x00, 0xff, 0xfe, 0x80, 0x01];
    let mut msg = book("x", "X", 1);
    msg.set_field_by_name("cover", Value::Bytes(cover.clone().into()));
    msg.set_field_by_name("isbn", Value::U64(u64::MAX - 1));
    db.create(&res, &msg).await.unwrap();
    db.get(&res, "books/x").await.unwrap(); // populates
    let got = db.get(&res, "books/x").await.unwrap(); // served from the cache
    assert_eq!(got, msg);
    let back = got.get_field_by_name("cover").unwrap();
    assert_eq!(back.as_bytes().unwrap().as_ref(), cover.as_slice());
}

// The bug a read-through cache produces that looks exactly like a database losing a write.
#[tokio::test]
async fn create_clears_a_remembered_absence() {
    let (db, _, res) = setup();
    let first = db.get(&res, "books/new").await;
    assert!(matches!(first, Err(Error::NotFound)), "{first:?}");
    db.create(&res, &book("new", "New", 1)).await.unwrap();
    let got = db
        .get(&res, "books/new")
        .await
        .expect("the created record is invisible");
    assert_eq!(string_field(&got, "title"), "New");
}

#[tokio::test]
async fn update_is_visible_immediately() {
    let (db, _, res) = setup();
    db.create(&res, &book("a", "First", 1)).await.unwrap();
    db.get(&res, "books/a").await.unwrap(); // populates
    db.update(&res, &book("a", "Second", 1)).await.unwrap();
    let got = db.get(&res, "books/a").await.unwrap();
    assert_eq!(
        string_field(&got, "title"),
        "Second",
        "a stale record was served"
    );
}

#[tokio::test]
async fn delete_is_visible_immediately() {
    let (db, _, res) = setup();
    db.create(&res, &book("a", "A", 1)).await.unwrap();
    db.get(&res, "books/a").await.unwrap();
    db.delete(&res, "books/a").await.unwrap();
    let got = db.get(&res, "books/a").await;
    assert!(
        matches!(got, Err(Error::NotFound)),
        "a deleted record was served: {got:?}"
    );
}

// Or a stream of requests for something that does not exist reaches the store forever.
#[tokio::test]
async fn absence_is_remembered() {
    let (db, store, res) = setup();
    for _ in 0..10 {
        let got = db.get(&res, "books/ghost").await;
        assert!(matches!(got, Err(Error::NotFound)), "{got:?}");
    }
    assert_eq!(
        store.gets(),
        1,
        "requests for a missing record kept reaching the store"
    );
}

// The property that makes a cache worth putting in front of a database at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_misses_collapse() {
    let (db, store, res) = setup();
    db.create(&res, &book("hot", "Hot", 1)).await.unwrap();
    let gate = store.hold();
    let callers: Vec<_> = (0..50)
        .map(|_| {
            let (db, res) = (db.clone(), res.clone());
            tokio::spawn(async move { db.get(&res, "books/hot").await })
        })
        .collect();
    // Hold the first load until the rest have reached the cache and found it running:
    // releasing on the first arrival would measure how fast fifty tasks start.
    store.arrived.notified().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    gate.close();
    for caller in callers {
        caller.await.unwrap().unwrap();
    }
    assert!(
        store.gets() <= 2,
        "50 concurrent misses caused {} loads",
        store.gets()
    );
}

// A refusal must not fail the request: the load happens either way.
#[tokio::test]
async fn an_overloaded_cache_falls_through() {
    let (db, store, res) = setup_with(Arc::new(Refusing));
    db.create(&res, &book("a", "A", 1)).await.unwrap();
    let got = db
        .get(&res, "books/a")
        .await
        .expect("an overloaded cache failed the read");
    assert_eq!(string_field(&got, "title"), "A");
    assert!(store.gets() > 0, "the read did not reach the store");
}

// Not in Go's suite: the store's own error crosses the cache as itself, and is not cached.
#[tokio::test]
async fn a_store_failure_comes_back_as_itself_and_is_not_cached() {
    let (db, store, res) = setup();
    db.create(&res, &book("a", "A", 1)).await.unwrap();
    store.broken.store(true, Ordering::SeqCst);
    let got = db.get(&res, "books/a").await;
    assert!(matches!(got, Err(Error::Timeout)), "{got:?}");
    store.broken.store(false, Ordering::SeqCst);
    db.get(&res, "books/a")
        .await
        .expect("a failure was remembered");
    assert_eq!(store.gets(), 2);
}
