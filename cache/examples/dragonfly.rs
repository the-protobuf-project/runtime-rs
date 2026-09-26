//! Runnable Dragonfly walkthrough for the shared RESP capability surface.
//!
//! Start the repository's isolated Dragonfly service before running:
//!
//! ```text
//! docker compose -p runtime-cache-dragonfly-live -f cache/docker/compose.dragonfly.live.yaml up -d --wait dragonfly
//! cargo run -p runtime-cache --example dragonfly
//! docker compose -p runtime-cache-dragonfly-live -f cache/docker/compose.dragonfly.live.yaml down
//! ```
//!
//! `RUNTIME_CACHE_DRAGONFLY_ADDRESS` overrides `127.0.0.1:16380`.

use std::{env, sync::Arc, time::Duration};

use futures::FutureExt;
use runtime_cache::{
    CacheError, Config, Provider, Result,
    core::{Loader, Options},
    drivers::dragonfly::{DragonflyClient, DragonflyConfig, DragonflyProvider},
};
use uuid::Uuid;

/// Named key namespace removed after the walkthrough.
const NAMESPACE: &str = "orders";
/// Finite fallback lease protects interrupted runs from permanent data.
const EXAMPLE_TTL: Duration = Duration::from_secs(60);

/// Demonstrates the RESP capabilities that distinguish Dragonfly from Memcached.
async fn exercise(db: &runtime_cache::DB) -> Result<()> {
    if db.backend != "dragonfly" {
        return Err(CacheError::Internal(format!(
            "Dragonfly example: selected DB reported backend {}",
            db.backend
        )));
    }

    let options = Options::default();
    let indexed_id = db
        .indexed
        .create(
            br#"{"name":"Alice"}"#,
            &Options::default().with_index("tenant", "acme"),
        )
        .await?;
    let matches = db.indexed.by_index("tenant", "acme").await?;
    if matches.len() != 1 {
        return Err(CacheError::Internal(format!(
            "Dragonfly example: Indexed lookup returned {} values",
            matches.len()
        )));
    }
    let ttl = db.indexed.ttl(&indexed_id).await?;
    let removed = db.indexed.delete_by_index("tenant", "acme").await?;
    println!(
        "Indexed: found and removed {removed} value under generated ID {indexed_id} ({ttl:?} remaining)"
    );

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
            "Dragonfly example: Aside hit did not reuse the loaded value".to_owned(),
        ));
    }
    profiles.invalidate("bob").await?;
    println!("Aside: two reads caused one loader call through the shared RESP path");

    Ok(())
}

/// Returns an address override without treating non-Unicode input as absent.
fn dragonfly_address() -> Result<String> {
    match env::var("RUNTIME_CACHE_DRAGONFLY_ADDRESS") {
        Ok(address) => Ok(address),
        Err(env::VarError::NotPresent) => Ok("127.0.0.1:16380".to_owned()),
        Err(error) => Err(CacheError::Internal(format!(
            "Dragonfly example: invalid RUNTIME_CACHE_DRAGONFLY_ADDRESS: {error}"
        ))),
    }
}

/// Chooses the final result while retaining a primary walkthrough failure.
fn finish(operation: Result<()>, close: Result<()>, cleanup: Result<usize>) -> Result<()> {
    if let Err(error) = operation {
        // Cleanup remains best effort only after the walkthrough has failed;
        // report its errors without replacing the semantic operation failure.
        if let Err(close_error) = close {
            eprintln!("Dragonfly example database close also failed: {close_error}");
        }
        if let Err(cleanup_error) = cleanup {
            eprintln!("Dragonfly example cleanup also failed: {cleanup_error}");
        }
        return Err(error);
    }

    match (close, cleanup) {
        (Ok(()), Ok(_)) => Ok(()),
        (Err(error), Ok(_)) | (Ok(()), Err(error)) => Err(error),
        (Err(close_error), Err(cleanup_error)) => Err(CacheError::Internal(format!(
            "Dragonfly example database close failed ({close_error}) and cleanup failed ({cleanup_error})"
        ))),
    }
}

/// Connects, runs the walkthrough, and explicitly releases owned resources.
#[tokio::main]
async fn main() -> Result<()> {
    let client = Arc::new(
        DragonflyClient::connect(DragonflyConfig {
            address: dragonfly_address()?,
            ..DragonflyConfig::default()
        })
        .await?,
    );
    let provider = DragonflyProvider::new(client.clone(), Config {
        prefix: format!("runtime-cache-dragonfly-example-{}", Uuid::new_v4()),
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
    // Drain refresh work before deleting the namespace so it cannot be
    // repopulated after cleanup has scanned past an Aside key.
    let close = db.close().await;
    let cleanup = provider.drop_database(NAMESPACE).await;
    client.close().await;
    finish(operation, close, cleanup)
}
