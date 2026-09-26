//! Runnable Redis walkthrough for the complete public cache surface.
//!
//! Start the repository's isolated Redis service before running this example:
//!
//! ```text
//! docker compose -p runtime-cache-live -f cache/docker/compose.live.yaml up -d --wait redis
//! cargo run -p runtime-cache --example redis
//! docker compose -p runtime-cache-live -f cache/docker/compose.live.yaml down
//! ```
//!
//! `RUNTIME_CACHE_REDIS_ADDRESS` overrides the default `127.0.0.1:16379`.

use std::{env, sync::Arc, time::Duration};

use futures::FutureExt;
use runtime_cache::{
    CacheError, Config, Provider, Result, chain,
    core::{Loader, Options},
    drivers::redis::{RedisClient, RedisConfig, RedisProvider},
    typed, with_logging_middleware, with_retry_middleware,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Named key namespace removed after the walkthrough.
const NAMESPACE: &str = "orders";
/// Finite fallback lease protects interrupted runs from leaving permanent data.
const EXAMPLE_TTL: Duration = Duration::from_secs(60);

/// Small application model stored through the typed Document view.
#[derive(Debug, Deserialize, Serialize)]
struct User {
    /// Display name used by the walkthrough.
    name: String,
    /// Secondary lookup value used by Indexed.
    email: String,
}

/// Demonstrates every strategy over one selected Redis database.
async fn exercise(db: &runtime_cache::DB) -> Result<()> {
    let options = Options::default();
    let alice = User {
        name: "Alice".to_owned(),
        email: "alice@example.com".to_owned(),
    };

    // The final middleware is outermost: logging observes the complete retry
    // sequence, while Typed performs JSON conversion outside raw middleware.
    let users = typed::<User>(chain(db.document.clone(), [
        with_retry_middleware(3, Duration::from_millis(25)),
        with_logging_middleware(),
    ]));
    let document_id = users.create(&alice, &options).await?;
    let _loaded = users.get(&document_id).await?;
    let document_ttl = users.ttl(&document_id).await?;
    println!(
        "Document: read typed value under generated ID {document_id} ({document_ttl:?} remaining)"
    );

    db.volatile
        .set("session:alice", b"active", &options)
        .await?;
    db.volatile
        .touch("session:alice", Duration::from_secs(90))
        .await?;
    let mut session = Vec::new();
    db.volatile.get("session:alice", &mut session).await?;
    let session_ttl = db.volatile.ttl("session:alice").await?;
    let sessions = db.volatile.scan("session:*").await?;
    println!(
        "Volatile: read {} bytes and found {} session key(s) ({session_ttl:?} remaining)",
        session.len(),
        sessions.len()
    );

    let encoded = serde_json::to_vec(&alice)
        .map_err(|error| CacheError::Internal(format!("Redis example: encode user: {error}")))?;
    let indexed_id = db
        .indexed
        .create(
            &encoded,
            &Options::default().with_index("email", &alice.email),
        )
        .await?;
    let matches = db.indexed.by_index("email", "alice@example.com").await?;
    println!(
        "Indexed: found {} value(s) for {indexed_id} by e-mail",
        matches.len()
    );
    let removed = db
        .indexed
        .delete_by_index("email", "alice@example.com")
        .await?;
    println!("Indexed: removed {removed} value(s) by e-mail");

    let loads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let loader_count = loads.clone();
    let loader: Loader = Arc::new(move |id: String| {
        let loader_count = loader_count.clone();
        async move {
            loader_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(format!(r#"{{"name":"{id}"}}"#).into_bytes())
        }
        .boxed()
    });
    let profiles = db.aside(loader);
    let mut first = Vec::new();
    profiles.get_or_load("bob", &mut first, &options).await?;
    let mut second = Vec::new();
    profiles.get_or_load("bob", &mut second, &options).await?;
    if first != second || loads.load(std::sync::atomic::Ordering::SeqCst) != 1 {
        return Err(CacheError::Internal(
            "Redis example: Aside cache hit did not reuse the loaded value".to_owned(),
        ));
    }
    profiles.refresh("bob", &options).await?;
    profiles.invalidate("bob").await?;
    println!(
        "Aside: two reads plus refresh caused {} loader call(s)",
        loads.load(std::sync::atomic::Ordering::SeqCst)
    );

    Ok(())
}

/// Returns an address override without treating a non-Unicode value as absent.
fn redis_address() -> Result<String> {
    match env::var("RUNTIME_CACHE_REDIS_ADDRESS") {
        Ok(address) => Ok(address),
        Err(env::VarError::NotPresent) => Ok("127.0.0.1:16379".to_owned()),
        Err(error) => Err(CacheError::Internal(format!(
            "Redis example: invalid RUNTIME_CACHE_REDIS_ADDRESS: {error}"
        ))),
    }
}

/// Chooses the final result while keeping the primary operation error intact.
fn finish(operation: Result<()>, close: Result<()>, cleanup: Result<usize>) -> Result<()> {
    if let Err(error) = operation {
        // Cleanup is best effort only after the walkthrough has already failed.
        // Report its failures without replacing the semantic operation error.
        if let Err(cleanup_error) = cleanup {
            eprintln!("Redis example cleanup also failed: {cleanup_error}");
        }
        if let Err(close_error) = close {
            eprintln!("Redis example database close also failed: {close_error}");
        }
        return Err(error);
    }

    match (cleanup, close) {
        (Ok(_), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(cleanup_error), Err(close_error)) => Err(CacheError::Internal(format!(
            "Redis example cleanup failed ({cleanup_error}) and database close failed ({close_error})"
        ))),
    }
}

/// Connects, runs the walkthrough, and explicitly releases owned resources.
#[tokio::main]
async fn main() -> Result<()> {
    let client = Arc::new(
        RedisClient::connect(RedisConfig {
            address: redis_address()?,
            ..RedisConfig::default()
        })
        .await?,
    );
    let provider = RedisProvider::new(client.clone(), Config {
        prefix: format!("runtime-cache-example-{}", Uuid::new_v4()),
        default_ttl: EXAMPLE_TTL,
        require_ttl: true,
        databases: vec![NAMESPACE.to_owned()],
        ..Config::default()
    });
    let db = match provider.set_database(NAMESPACE).await {
        Ok(db) => db,
        Err(error) => {
            client.close().await;
            return Err(error);
        }
    };

    let operation = exercise(&db).await;
    // Drain Aside refresh work before deleting the namespace so a late refresh
    // cannot repopulate it after cleanup has scanned past that key.
    let close = db.close().await;
    let cleanup = provider.drop_database(NAMESPACE).await;
    client.close().await;
    finish(operation, close, cleanup)
}
