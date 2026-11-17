//! The repository suite against a live MongoDB replica set, each case in a database of its
//! own, dropped afterwards. Racing writers exercise session transactions, whose write
//! conflicts are `Conflict`.
//!
//! ```text
//! docker compose -p runtime-database-repository-mongodb -f database/docker/compose.mongodb.live.yaml up -d --wait mongodb
//! cargo test -p tpp-database --features mongodb,live-tests --test repository_mongodb
//! docker compose -p runtime-database-repository-mongodb -f database/docker/compose.mongodb.live.yaml down
//! ```
//!
//! `RUNTIME_DATABASE_MONGODB_ADDRESS` defaults to `127.0.0.1:27117`.

mod support {
    #[path = "articles.rs"]
    pub mod articles;
    #[path = "repo_crud.rs"]
    pub mod repo_crud;
    #[path = "repo_delete.rs"]
    pub mod repo_delete;
    #[path = "repo_more.rs"]
    pub mod repo_more;
    #[path = "repo_suite.rs"]
    pub mod repo_suite;
    #[path = "repo_update.rs"]
    pub mod repo_update;
}

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use database::mongodb::{MongoClient, MongoConfig, MongoProvider};
use database::records::Provider;
use futures::FutureExt;
use support::repo_suite::Scenario;

async fn run(case: Scenario) {
    static SEQ: AtomicUsize = AtomicUsize::new(0);
    let at = std::env::var("RUNTIME_DATABASE_MONGODB_ADDRESS")
        .unwrap_or_else(|_| "127.0.0.1:27117".into());
    let config = MongoConfig {
        connect_timeout: Duration::from_secs(5),
        ..MongoConfig::address(at.clone())
    };
    let client = MongoClient::connect(config)
        .await
        .unwrap_or_else(|e| panic!("no MongoDB replica set at {at}: {e}"));
    let provider = MongoProvider::new(client.clone());
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let name = format!("repo_{}_{n}", std::process::id());
    let db = provider.set_database(&name).await.unwrap();
    let outcome = AssertUnwindSafe(case(db)).catch_unwind().await;
    provider.drop_database(&name).await.unwrap();
    client.close().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

crate::repository_suite!(run);
crate::repository_filters!(run);
crate::repository_races!(run);
