//! Live Redis verification for the public runtime-cache contract.
//!
//! This target is selected explicitly rather than marked ignored. Once the
//! `live-tests` feature is enabled, an unavailable or incompatible service is a
//! real test failure; the test never returns early as a fake pass.
//!
//! Start the isolated, non-persistent service from the repository root:
//!
//! ```text
//! docker compose -p runtime-cache-live -f cache/docker/compose.live.yaml up -d --wait redis
//! ```
//!
//! Run the test:
//!
//! ```text
//! cargo test -p runtime-cache --features live-tests --test redis_live
//! ```
//!
//! Then remove only this Compose project's resources:
//!
//! ```text
//! docker compose -p runtime-cache-live -f cache/docker/compose.live.yaml down
//! ```
//!
//! `RUNTIME_CACHE_REDIS_ADDRESS` defaults to `127.0.0.1:16379`. Optional
//! `RUNTIME_CACHE_REDIS_USERNAME`, `RUNTIME_CACHE_REDIS_PASSWORD`, and
//! `RUNTIME_CACHE_REDIS_DATABASE` values support a caller-supplied service.
//! Credentials are passed directly to the client and never printed. Every run
//! uses a UUID prefix and expiring entries, and cleanup scans only that prefix;
//! neither FLUSHDB nor FLUSHALL is used.

use std::{
    env,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures::FutureExt;
use runtime_cache::{
    CacheError, Config, Provider, Result,
    core::{DB, Loader, Options},
    drivers::redis::{RedisClient, RedisConfig, RedisProvider},
};
use uuid::Uuid;

/// Namespace deliberately contains no dynamic input; the UUID belongs in the
/// prefix so Provider namespace validation is exercised normally.
const NAMESPACE: &str = "live-contract";
/// Short fallback lease ensures abandoned test data disappears automatically.
const TEST_TTL: Duration = Duration::from_secs(30);

/// Exercises all public strategies against one selected live Redis database.
async fn exercise(provider: &RedisProvider, db: &DB, expected_index: usize) -> Result<()> {
    ensure(db.backend == "redis", "DB reported the wrong backend")?;
    ensure(db.name == NAMESPACE, "DB reported the wrong namespace")?;
    ensure(
        db.index == expected_index,
        "DB reported the wrong native index",
    )?;

    let options = Options::default();

    db.volatile
        .set("session:one", b"volatile-one", &options)
        .await?;
    let mut volatile = Vec::new();
    db.volatile.get("session:one", &mut volatile).await?;
    ensure(volatile == b"volatile-one", "Volatile GET changed bytes")?;
    db.volatile
        .touch("session:one", Duration::from_secs(45))
        .await?;
    let volatile_ttl = db.volatile.ttl("session:one").await?;
    ensure(
        volatile_ttl > Duration::ZERO && volatile_ttl <= Duration::from_secs(45),
        "Volatile TTL fell outside the touched lease",
    )?;
    let scanned = db.volatile.scan("session:*").await?;
    ensure(
        scanned
            .iter()
            .any(|key| key.ends_with("cache:vol:session:one")),
        "Volatile SCAN omitted the stored key",
    )?;
    db.volatile.delete("session:one").await?;
    ensure_not_found(
        db.volatile.get("session:one", &mut Vec::new()).await,
        "Volatile DELETE left the key live",
    )?;

    let document_id = db
        .document
        .create(b"document-one", &Options::default().with_id("document-one"))
        .await?;
    ensure(
        document_id == "document-one",
        "Document changed explicit ID",
    )?;
    let mut document = Vec::new();
    db.document.get(&document_id, &mut document).await?;
    ensure(document == b"document-one", "Document GET changed bytes")?;
    db.document
        .update(&document_id, b"document-two", &options)
        .await?;
    ensure(
        db.document.keys().await?.contains(&document_id),
        "Document keys omitted the live ID",
    )?;
    ensure(
        db.document
            .list()
            .await?
            .contains(&b"document-two".to_vec()),
        "Document list omitted the updated value",
    )?;
    ensure(
        db.document.ttl(&document_id).await? > Duration::ZERO,
        "Document TTL did not report a live lease",
    )?;
    db.document.delete(&document_id).await?;
    ensure_not_found(
        db.document.get(&document_id, &mut Vec::new()).await,
        "Document DELETE left the value live",
    )?;

    let indexed_id = db
        .indexed
        .create(
            b"indexed-one",
            &Options::default()
                .with_id("indexed-one")
                .with_index("tenant", "acme"),
        )
        .await?;
    ensure(
        db.indexed.ids_by_index("tenant", "acme").await? == vec![indexed_id.clone()],
        "Indexed lookup omitted the filed ID",
    )?;
    ensure(
        db.indexed.by_index("tenant", "acme").await? == vec![b"indexed-one".to_vec()],
        "Indexed lookup changed the filed value",
    )?;
    db.indexed
        .update(
            &indexed_id,
            b"indexed-two",
            &Options::default().with_index("tenant", "beta"),
        )
        .await?;
    ensure(
        db.indexed.ids_by_index("tenant", "acme").await?.is_empty(),
        "Indexed refiling retained the old membership",
    )?;
    ensure(
        db.indexed.ids_by_index("tenant", "beta").await? == vec![indexed_id.clone()],
        "Indexed refiling omitted the new membership",
    )?;
    ensure(
        db.indexed.delete_by_index("tenant", "beta").await? == 1,
        "Indexed group deletion reported the wrong count",
    )?;
    ensure_not_found(
        db.indexed.get(&indexed_id, &mut Vec::new()).await,
        "Indexed group deletion left the value live",
    )?;

    let loads = Arc::new(AtomicUsize::new(0));
    let loader_count = loads.clone();
    let loader: Loader = Arc::new(move |_| {
        let load = loader_count.fetch_add(1, Ordering::SeqCst) + 1;
        async move { Ok(format!(r#"{{"load":{load}}}"#).into_bytes()) }.boxed()
    });
    let aside = db.aside(loader);
    let mut first = Vec::new();
    aside.get_or_load("aside-one", &mut first, &options).await?;
    ensure(first == br#"{"load":1}"#, "Aside miss returned wrong load")?;
    let mut hit = Vec::new();
    aside.get_or_load("aside-one", &mut hit, &options).await?;
    ensure(hit == first, "Aside hit changed the cached value")?;
    ensure(loads.load(Ordering::SeqCst) == 1, "Aside hit reran loader")?;
    aside.refresh("aside-one", &options).await?;
    let mut refreshed = Vec::new();
    aside
        .get_or_load("aside-one", &mut refreshed, &options)
        .await?;
    ensure(
        refreshed == br#"{"load":2}"#,
        "Aside refresh did not replace value",
    )?;
    aside.invalidate("aside-one").await?;
    aside
        .get_or_load("aside-one", &mut refreshed, &options)
        .await?;
    ensure(
        loads.load(Ordering::SeqCst) == 3,
        "Aside invalidation did not force another load",
    )?;

    db.volatile.set("drop-me", b"drop-me", &options).await?;
    let deleted = provider.drop_database(NAMESPACE).await?;
    ensure(deleted > 0, "database drop reported no deleted keys")?;
    Ok(())
}

/// Returns a semantic test failure without panicking past asynchronous cleanup.
fn ensure(condition: bool, message: &str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(CacheError::Internal(format!("live Redis test: {message}")))
    }
}

/// Requires one public operation to report an ordinary cache miss.
fn ensure_not_found<T>(result: Result<T>, message: &str) -> Result<()> {
    match result {
        Err(CacheError::NotFound) => Ok(()),
        Ok(_) => Err(CacheError::Internal(format!("live Redis test: {message}"))),
        Err(error) => Err(error),
    }
}

/// Reads live-test connection settings without logging credential values.
fn redis_config() -> Result<RedisConfig> {
    let address =
        env::var("RUNTIME_CACHE_REDIS_ADDRESS").unwrap_or_else(|_| "127.0.0.1:16379".to_owned());
    let username = env::var("RUNTIME_CACHE_REDIS_USERNAME").unwrap_or_default();
    let password = env::var("RUNTIME_CACHE_REDIS_PASSWORD").unwrap_or_default();
    let database = match env::var("RUNTIME_CACHE_REDIS_DATABASE") {
        Ok(value) => value.parse::<usize>().map_err(|error| {
            CacheError::Internal(format!(
                "live Redis test: invalid RUNTIME_CACHE_REDIS_DATABASE: {error}"
            ))
        })?,
        Err(_) => 0,
    };
    Ok(RedisConfig {
        address,
        username,
        password,
        database,
        ..RedisConfig::default()
    })
}

/// Preserves every cleanup failure alongside the primary exercise result.
fn finish(exercised: Result<()>, closed: Result<()>, cleaned: Result<usize>) -> Result<()> {
    let mut failures = Vec::new();
    if let Err(error) = exercised {
        failures.push(format!("exercise failed: {error}"));
    }
    if let Err(error) = closed {
        failures.push(format!("database close failed: {error}"));
    }
    if let Err(error) = cleaned {
        failures.push(format!("cleanup failed: {error}"));
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(CacheError::Internal(failures.join("; ")))
    }
}

#[tokio::test]
async fn test_redis_live_public_cache_contract() -> Result<()> {
    let redis_config = redis_config()?;
    let expected_index = redis_config.database;
    let client = Arc::new(RedisClient::connect(redis_config).await?);
    let prefix = format!("runtime-cache-live-{}", Uuid::new_v4());
    let provider = RedisProvider::new(client.clone(), Config {
        prefix,
        default_ttl: TEST_TTL,
        require_ttl: true,
        databases: vec![NAMESPACE.to_owned()],
        ..Config::default()
    });

    if let Err(error) = provider.drop_database(NAMESPACE).await {
        client.close().await;
        return Err(error);
    }
    let db = match provider.set_database(NAMESPACE).await {
        Ok(db) => db,
        Err(error) => {
            client.close().await;
            return Err(error);
        }
    };

    let exercised = exercise(&provider, &db, expected_index).await;
    let closed = db.close().await;
    let cleaned = provider.drop_database(NAMESPACE).await;
    client.close().await;
    finish(exercised, closed, cleaned)
}
