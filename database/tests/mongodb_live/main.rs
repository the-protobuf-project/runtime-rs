//! Live MongoDB verification of the record contract: runtime-go's `mongodb_test.go`, ported
//! case for case, plus the encoding, filters and capabilities Go checks only implicitly.
//!
//! The target builds only with `mongodb` and `live-tests`, and once built an unreachable
//! server is a failure, never a skip. It needs a replica set, which the compose file runs:
//!
//! ```text
//! docker compose -p runtime-database-mongodb-live -f database/docker/compose.mongodb.live.yaml up -d --wait mongodb
//! cargo test -p tpp-database --features mongodb,live-tests --test mongodb_live
//! docker compose -p runtime-database-mongodb-live -f database/docker/compose.mongodb.live.yaml down
//! ```
//!
//! `RUNTIME_DATABASE_MONGODB_ADDRESS` defaults to `127.0.0.1:27117`. Every test works in its
//! own database, dropped afterwards, panic or not.

#[path = "../support/records.rs"]
mod support;

mod capabilities;
mod codec;
mod crud;
mod list;

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use database::mongodb::{MongoClient, MongoConfig, MongoProvider};
use database::records::{DB, Provider, Resource};
use futures::FutureExt;

/// Where the server is.
pub fn address() -> String {
    std::env::var("RUNTIME_DATABASE_MONGODB_ADDRESS").unwrap_or_else(|_| "127.0.0.1:27117".into())
}

/// A client of the test server, failing fast when it is not there.
pub async fn client() -> MongoClient {
    let config = MongoConfig {
        connect_timeout: Duration::from_secs(5),
        ..MongoConfig::address(address())
    };
    MongoClient::connect(config)
        .await
        .unwrap_or_else(|e| panic!("no MongoDB replica set at {}: {e}", address()))
}

/// A database name no other test or run uses.
pub fn unique_name(tag: &str) -> String {
    static SEQ: AtomicUsize = AtomicUsize::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("itest_{tag}_{}_{n}", std::process::id())
}

/// One selected database with the books collection made, handed to `test`, then dropped.
pub struct Fixture {
    /// The selected database.
    pub db: DB,
    /// The `Book` resource.
    pub res: Arc<Resource>,
    /// The client, for reading stored documents raw.
    pub client: MongoClient,
}

/// Runs `test` against a fresh database, and drops it afterwards even when `test` panics.
pub async fn with_db<F, Fut>(test: F)
where
    F: FnOnce(Fixture) -> Fut,
    Fut: Future<Output = ()>,
{
    let client = client().await;
    let provider = MongoProvider::new(client.clone());
    let name = unique_name("db");
    let db = provider.set_database(&name).await.expect("set_database");
    let res = Arc::new(support::books());
    db.schema.ensure_schema(&res).await.expect("ensure_schema");
    let fixture = Fixture {
        db: db.clone(),
        res,
        client: client.clone(),
    };
    let outcome = AssertUnwindSafe(test(fixture)).catch_unwind().await;
    provider.drop_database(&name).await.expect("drop_database");
    db.close().await.expect("close");
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
