//! `database::cached` over the real cache crate against a live Redis, because the adapter is
//! where the two contracts meet and an in-process cache cannot prove it. Ported from
//! runtime-go's `database/cached/live_test.go`.
//!
//! Selected by the `live-tests` feature rather than ignored, so a missing server fails
//! instead of passing. From the repository root:
//!
//! ```text
//! docker compose -p runtime-cache-live -f cache/docker/compose.live.yaml up -d --wait redis
//! cargo test -p tpp-database --features cached,live-tests --test cached_live
//! docker compose -p runtime-cache-live -f cache/docker/compose.live.yaml down
//! ```
//!
//! `RUNTIME_CACHE_REDIS_ADDRESS` defaults to `127.0.0.1:16379`; `RUNTIME_CACHE_REDIS_USERNAME`,
//! `_PASSWORD` and `_DATABASE` reach a caller-supplied server, as the cache's own live tests
//! read them. Each test uses a UUID prefix and a minute's TTL, and drops only its own keys.

#[path = "support/records.rs"]
mod books;
#[path = "support/memory_records.rs"]
mod memory_records;

use std::sync::Arc;
use std::time::Duration;

use books::{book, books, string_field};
use cache::core::Options;
use cache::drivers::redis::{RedisClient, RedisConfig, RedisProvider};
use cache::{Config, Provider};
use database::Error;
use database::cached::{from_aside, wrap};
use database::records::{DB, Resource};
use memory_records::{MemoryRecords, plain};
use prost_reflect::Value;

const TTL: Duration = Duration::from_secs(60);

/// A wrapped store over a fresh namespace of the live Redis, and what to clean up.
struct Live {
    db: DB,
    store: Arc<MemoryRecords>,
    res: Resource,
    client: Arc<RedisClient>,
    provider: RedisProvider,
    cache: Arc<cache::DB>,
}

fn env(name: &str) -> String {
    std::env::var(format!("RUNTIME_CACHE_REDIS_{name}")).unwrap_or_default()
}

async fn live() -> Live {
    let address = Some(env("ADDRESS"))
        .filter(|a| !a.is_empty())
        .unwrap_or_else(|| "127.0.0.1:16379".into());
    let config = RedisConfig {
        address,
        username: env("USERNAME"),
        password: env("PASSWORD"),
        database: env("DATABASE").parse().unwrap_or(0),
        ..RedisConfig::default()
    };
    let client = Arc::new(RedisClient::connect(config).await.expect("live Redis"));
    let provider = RedisProvider::new(
        client.clone(),
        Config {
            prefix: format!("ctest-{}", uuid::Uuid::new_v4()),
            default_ttl: TTL,
            ..Config::default()
        },
    );
    let cache = Arc::new(provider.set_database("records").await.unwrap());
    let store = MemoryRecords::new();
    let cached = from_aside(cache.clone(), Options::default().with_ttl(TTL));
    let db = wrap(&plain(store.clone()), cached);
    Live {
        db,
        store,
        res: books(),
        client,
        provider,
        cache,
    }
}

impl Live {
    async fn finish(self) {
        self.provider.drop_database("records").await.unwrap();
        self.cache.close().await.unwrap();
        self.client.close().await;
    }
}

// The adapter's whole job: a record cached through Redis comes back byte for byte.
#[tokio::test]
async fn live_round_trip() {
    let t = live().await;
    let cover = vec![0x00, 0xff, 0xfe, 0x80, 0x01, 0x0a, 0x22, 0x5c];
    let mut msg = book("x", "X", 1);
    msg.set_field_by_name("cover", Value::Bytes(cover.into()));
    t.db.create(&t.res, &msg).await.unwrap();
    t.db.get(&t.res, "books/x").await.unwrap(); // populates
    let got = t.db.get(&t.res, "books/x").await.unwrap(); // served from Redis
    assert_eq!(got, msg);
    assert_eq!(string_field(&got, "title"), "X");
    assert_eq!(
        t.store.gets(),
        1,
        "two reads reached the store more than once"
    );
    t.finish().await;
}

// NotFound survives the trip into the cache's vocabulary and back, and is remembered.
#[tokio::test]
async fn live_not_found_survives_translation() {
    let t = live().await;
    for _ in 0..5 {
        let got = t.db.get(&t.res, "books/ghost").await;
        assert!(matches!(got, Err(Error::NotFound)), "{got:?}");
    }
    assert_eq!(
        t.store.gets(),
        1,
        "the absence was not remembered on the server"
    );
    t.finish().await;
}

#[tokio::test]
async fn live_create_clears_a_remembered_absence() {
    let t = live().await;
    let first = t.db.get(&t.res, "books/new").await;
    assert!(matches!(first, Err(Error::NotFound)), "{first:?}");
    t.db.create(&t.res, &book("new", "New", 1)).await.unwrap();
    let got = t.db.get(&t.res, "books/new").await;
    assert_eq!(
        string_field(&got.expect("invisible through Redis"), "title"),
        "New"
    );
    t.finish().await;
}

#[tokio::test]
async fn live_write_is_visible_immediately() {
    let t = live().await;
    t.db.create(&t.res, &book("a", "First", 1)).await.unwrap();
    t.db.get(&t.res, "books/a").await.unwrap();
    t.db.update(&t.res, &book("a", "Second", 1)).await.unwrap();
    let got = t.db.get(&t.res, "books/a").await.unwrap();
    assert_eq!(
        string_field(&got, "title"),
        "Second",
        "a stale record survived"
    );
    t.db.delete(&t.res, "books/a").await.unwrap();
    let got = t.db.get(&t.res, "books/a").await;
    assert!(
        matches!(got, Err(Error::NotFound)),
        "a deleted record was served: {got:?}"
    );
    t.finish().await;
}

// Many callers, one cold key, one load: the cache's property, reaching the store layer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_concurrent_misses_collapse() {
    let t = live().await;
    t.db.create(&t.res, &book("hot", "Hot", 1)).await.unwrap();
    let gate = t.store.hold();
    let callers: Vec<_> = (0..200)
        .map(|_| {
            let (db, res) = (t.db.clone(), t.res.clone());
            tokio::spawn(async move { db.get(&res, "books/hot").await })
        })
        .collect();
    t.store.arrived.notified().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    gate.close();
    for caller in callers {
        caller.await.unwrap().unwrap();
    }
    assert!(
        t.store.gets() <= 3,
        "200 callers caused {} loads",
        t.store.gets()
    );
    t.finish().await;
}
