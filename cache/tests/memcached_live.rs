//! Live Memcached verification for its supported public cache contract.
//!
//! This target is selected explicitly rather than marked ignored. Once the
//! `live-tests` feature is enabled, an unavailable or incompatible service is
//! a real failure; the test never returns early as a fake pass.
//!
//! Start the isolated, non-persistent service from the repository root:
//!
//! ```text
//! docker compose -p runtime-cache-memcached-live -f cache/docker/compose.memcached.live.yaml up -d --wait memcached
//! ```
//!
//! Run the test:
//!
//! ```text
//! cargo test -p runtime-cache --features live-tests --test memcached_live
//! ```
//!
//! Then remove only this Compose project's resources:
//!
//! ```text
//! docker compose -p runtime-cache-memcached-live -f cache/docker/compose.memcached.live.yaml down
//! ```
//!
//! `RUNTIME_CACHE_MEMCACHED_SERVERS` defaults to `127.0.0.1:11212` and accepts
//! comma-separated routing slots. Every run uses a UUID prefix and expiring
//! entries. Cleanup deletes every known test entry; `flush_all` is never used.

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
    core::{Aside, DB, Loader, Options},
    drivers::memcached::{MemcachedClient, MemcachedConfig, MemcachedProvider},
};
use uuid::Uuid;

/// Stable namespace beneath the unique per-run prefix.
const NAMESPACE: &str = "live-contract";
/// Emulated numeric database used to verify keyspace separation.
const NUMERIC_DATABASE: usize = 7;
/// Short fallback lease bounds abandoned data when explicit cleanup cannot run.
const TEST_TTL: Duration = Duration::from_secs(30);
/// IDs remain fixed so cleanup can address them without a Scanner capability.
const VOLATILE_ID: &str = "volatile-one";
const DOCUMENT_ID: &str = "document-one";
const INDEXED_ID: &str = "indexed-one";
const REJECTED_INDEXED_ID: &str = "indexed-with-membership";
const ASIDE_ID: &str = "aside-one";
const ISOLATION_ID: &str = "isolation-one";
const POST_CLOSE_ID: &str = "post-close";

/// Exercises supported strategy behavior and explicit capability failures.
async fn exercise(
    provider: &MemcachedProvider,
    named: &DB,
    numeric: &DB,
    aside: &dyn Aside,
    loads: &AtomicUsize,
) -> Result<()> {
    ensure(
        named.backend == "memcached",
        "named DB reported wrong backend",
    )?;
    ensure(named.name == NAMESPACE, "named DB reported wrong namespace")?;
    ensure(named.index == 0, "named DB reported wrong index")?;
    ensure(
        numeric.backend == "memcached",
        "numeric DB reported wrong backend",
    )?;
    ensure(
        numeric.name.is_empty(),
        "numeric DB unexpectedly has a name",
    )?;
    ensure(
        numeric.index == NUMERIC_DATABASE,
        "numeric DB reported wrong index",
    )?;

    let options = Options::default();

    named
        .volatile
        .set(VOLATILE_ID, b"volatile-one", &options)
        .await?;
    let mut volatile = Vec::new();
    named.volatile.get(VOLATILE_ID, &mut volatile).await?;
    ensure(volatile == b"volatile-one", "Volatile GET changed bytes")?;
    named
        .volatile
        .set(VOLATILE_ID, b"volatile-two", &options)
        .await?;
    named
        .volatile
        .touch(VOLATILE_ID, Duration::from_secs(45))
        .await?;
    named.volatile.get(VOLATILE_ID, &mut volatile).await?;
    ensure(
        volatile == b"volatile-two",
        "Volatile replacement changed bytes",
    )?;
    ensure_unsupported(
        named.volatile.ttl(VOLATILE_ID).await,
        "Volatile TTL unexpectedly succeeded",
    )?;
    ensure_unsupported(
        named.volatile.scan("*").await,
        "Volatile scan unexpectedly succeeded",
    )?;
    named.volatile.delete(VOLATILE_ID).await?;
    ensure_not_found(
        named.volatile.get(VOLATILE_ID, &mut Vec::new()).await,
        "Volatile delete left the value live",
    )?;

    let document_id = named
        .document
        .create(b"document-one", &options.clone().with_id(DOCUMENT_ID))
        .await?;
    ensure(document_id == DOCUMENT_ID, "Document changed explicit ID")?;
    let mut document = Vec::new();
    named.document.get(DOCUMENT_ID, &mut document).await?;
    ensure(document == b"document-one", "Document GET changed bytes")?;
    named
        .document
        .update(DOCUMENT_ID, b"document-two", &options)
        .await?;
    named.document.get(DOCUMENT_ID, &mut document).await?;
    ensure(document == b"document-two", "Document update changed bytes")?;
    ensure_unsupported(
        named.document.keys().await,
        "Document enumeration unexpectedly succeeded",
    )?;
    ensure_unsupported(
        named.document.list().await,
        "Document listing unexpectedly succeeded",
    )?;
    ensure_unsupported(
        named.document.ttl(DOCUMENT_ID).await,
        "Document TTL unexpectedly succeeded",
    )?;
    named.document.delete(DOCUMENT_ID).await?;
    ensure_not_found(
        named.document.get(DOCUMENT_ID, &mut Vec::new()).await,
        "Document delete left the value live",
    )?;

    let indexed_id = named
        .indexed
        .create(b"indexed-one", &options.clone().with_id(INDEXED_ID))
        .await?;
    ensure(indexed_id == INDEXED_ID, "Indexed changed explicit ID")?;
    let mut indexed = Vec::new();
    named.indexed.get(INDEXED_ID, &mut indexed).await?;
    ensure(indexed == b"indexed-one", "Indexed GET changed bytes")?;
    named
        .indexed
        .update(INDEXED_ID, b"indexed-two", &options)
        .await?;
    named.indexed.get(INDEXED_ID, &mut indexed).await?;
    ensure(indexed == b"indexed-two", "Indexed update changed bytes")?;
    ensure_unsupported(
        named
            .indexed
            .create(
                b"must-not-store",
                &options
                    .clone()
                    .with_id(REJECTED_INDEXED_ID)
                    .with_index("tenant", "acme"),
            )
            .await,
        "Indexed create with membership unexpectedly succeeded",
    )?;
    ensure_not_found(
        named
            .indexed
            .get(REJECTED_INDEXED_ID, &mut Vec::new())
            .await,
        "rejected Indexed create stored a value",
    )?;
    ensure_unsupported(
        named.indexed.ids_by_index("tenant", "acme").await,
        "Indexed ID lookup unexpectedly succeeded",
    )?;
    ensure_unsupported(
        named.indexed.by_index("tenant", "acme").await,
        "Indexed value lookup unexpectedly succeeded",
    )?;
    ensure_unsupported(
        named.indexed.delete_by_index("tenant", "acme").await,
        "Indexed group deletion unexpectedly succeeded",
    )?;
    named.indexed.delete(INDEXED_ID).await?;
    ensure_not_found(
        named.indexed.get(INDEXED_ID, &mut Vec::new()).await,
        "Indexed delete left the value live",
    )?;

    let mut first = Vec::new();
    aside.get_or_load(ASIDE_ID, &mut first, &options).await?;
    ensure(first == br#"{"load":1}"#, "Aside miss returned wrong load")?;
    let mut hit = Vec::new();
    aside.get_or_load(ASIDE_ID, &mut hit, &options).await?;
    ensure(hit == first, "Aside hit changed cached value")?;
    ensure(loads.load(Ordering::SeqCst) == 1, "Aside hit reran loader")?;
    aside.refresh(ASIDE_ID, &options).await?;
    aside.get_or_load(ASIDE_ID, &mut hit, &options).await?;
    ensure(hit == br#"{"load":2}"#, "Aside refresh retained old value")?;
    aside.invalidate(ASIDE_ID).await?;
    aside.get_or_load(ASIDE_ID, &mut hit, &options).await?;
    ensure(
        loads.load(Ordering::SeqCst) == 3,
        "Aside invalidation did not force another load",
    )?;

    named.volatile.set(ISOLATION_ID, b"named", &options).await?;
    numeric
        .volatile
        .set(ISOLATION_ID, b"numeric", &options)
        .await?;
    let mut named_value = Vec::new();
    let mut numeric_value = Vec::new();
    named.volatile.get(ISOLATION_ID, &mut named_value).await?;
    numeric
        .volatile
        .get(ISOLATION_ID, &mut numeric_value)
        .await?;
    ensure(named_value == b"named", "named database value collided")?;
    ensure(
        numeric_value == b"numeric",
        "numeric database value collided",
    )?;

    ensure_unsupported(
        provider.drop_database(NAMESPACE).await,
        "Memcached database drop unexpectedly succeeded",
    )?;
    Ok(())
}

/// Deletes every known entry without stopping after an earlier cleanup error.
async fn cleanup(named: &DB, numeric: &DB, aside: &dyn Aside) -> Result<()> {
    let mut failures = Vec::new();
    collect_failure(
        &mut failures,
        "named Volatile cleanup",
        named.volatile.delete(VOLATILE_ID).await,
    );
    collect_failure(
        &mut failures,
        "named isolation cleanup",
        named.volatile.delete(ISOLATION_ID).await,
    );
    collect_failure(
        &mut failures,
        "Document cleanup",
        named.document.delete(DOCUMENT_ID).await,
    );
    collect_failure(
        &mut failures,
        "Indexed cleanup",
        named.indexed.delete(INDEXED_ID).await,
    );
    collect_failure(
        &mut failures,
        "rejected Indexed cleanup",
        named.indexed.delete(REJECTED_INDEXED_ID).await,
    );
    collect_failure(
        &mut failures,
        "Aside cleanup",
        aside.invalidate(ASIDE_ID).await,
    );
    collect_failure(
        &mut failures,
        "numeric isolation cleanup",
        numeric.volatile.delete(ISOLATION_ID).await,
    );
    failures_result("cleanup", failures)
}

/// Confirms closing one DB does not close the shared caller-owned client.
async fn verify_root_remains_live(named: &DB, numeric: &DB) -> Result<()> {
    named.close().await?;
    numeric
        .volatile
        .set(POST_CLOSE_ID, b"still-live", &Options::default())
        .await?;
    let mut value = Vec::new();
    numeric.volatile.get(POST_CLOSE_ID, &mut value).await?;
    ensure(
        value == b"still-live",
        "closing named DB closed the root client",
    )
}

/// Returns a semantic failure without panicking past asynchronous cleanup.
fn ensure(condition: bool, message: &str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(CacheError::Internal(format!(
            "live Memcached test: {message}"
        )))
    }
}

/// Requires one public operation to report an ordinary cache miss.
fn ensure_not_found<T>(result: Result<T>, message: &str) -> Result<()> {
    match result {
        Err(CacheError::NotFound) => Ok(()),
        Ok(_) => Err(CacheError::Internal(format!(
            "live Memcached test: {message}"
        ))),
        Err(error) => Err(error),
    }
}

/// Requires a deliberately unavailable Memcached capability to fail clearly.
fn ensure_unsupported<T>(result: Result<T>, message: &str) -> Result<()> {
    match result {
        Err(CacheError::Unsupported) => Ok(()),
        Ok(_) => Err(CacheError::Internal(format!(
            "live Memcached test: {message}"
        ))),
        Err(error) => Err(error),
    }
}

/// Records one labeled failure while allowing later cleanup to continue.
fn collect_failure(failures: &mut Vec<String>, label: &str, result: Result<()>) {
    if let Err(error) = result {
        failures.push(format!("{label} failed: {error}"));
    }
}

/// Turns accumulated failures into one cache error without losing context.
fn failures_result(context: &str, failures: Vec<String>) -> Result<()> {
    if failures.is_empty() {
        Ok(())
    } else {
        Err(CacheError::Internal(format!(
            "live Memcached {context}: {}",
            failures.join("; ")
        )))
    }
}

/// Parses caller-supplied routing slots without accepting empty addresses.
fn memcached_config() -> Result<MemcachedConfig> {
    let configured = env::var("RUNTIME_CACHE_MEMCACHED_SERVERS")
        .unwrap_or_else(|_| "127.0.0.1:11212".to_owned());
    let servers: Vec<String> = configured
        .split(',')
        .map(str::trim)
        .filter(|server| !server.is_empty())
        .map(str::to_owned)
        .collect();
    ensure(!servers.is_empty(), "server list is empty")?;
    Ok(MemcachedConfig {
        servers,
        ..MemcachedConfig::default()
    })
}

/// Preserves exercise, cleanup, lifecycle, and close failures together.
fn finish(results: Vec<(&'static str, Result<()>)>) -> Result<()> {
    let failures = results
        .into_iter()
        .filter_map(|(label, result)| result.err().map(|error| format!("{label}: {error}")))
        .collect();
    failures_result("run", failures)
}

#[tokio::test]
async fn test_memcached_live_public_cache_contract() -> Result<()> {
    let client = Arc::new(MemcachedClient::connect(memcached_config()?).await?);
    let prefix = format!("runtime-cache-live-{}", Uuid::new_v4());
    let provider = MemcachedProvider::new(client.clone(), Config {
        prefix,
        default_ttl: TEST_TTL,
        require_ttl: true,
        databases: vec![NAMESPACE.to_owned()],
        ..Config::default()
    });

    let named = match provider.set_database(NAMESPACE).await {
        Ok(db) => db,
        Err(error) => {
            client.close().await;
            return Err(error);
        }
    };
    let numeric = match provider.select_index(NUMERIC_DATABASE).await {
        Ok(db) => db,
        Err(error) => {
            let closed = named.close().await;
            client.close().await;
            return finish(vec![
                ("numeric DB selection failed", Err(error)),
                ("named DB close failed", closed),
            ]);
        }
    };

    let loads = Arc::new(AtomicUsize::new(0));
    let loader_count = loads.clone();
    let loader: Loader = Arc::new(move |_| {
        let load = loader_count.fetch_add(1, Ordering::SeqCst) + 1;
        async move { Ok(format!(r#"{{"load":{load}}}"#).into_bytes()) }.boxed()
    });
    let aside = named.aside(loader);

    let exercised = exercise(&provider, &named, &numeric, aside.as_ref(), loads.as_ref()).await;
    let cleaned = cleanup(&named, &numeric, aside.as_ref()).await;
    let root_live = verify_root_remains_live(&named, &numeric).await;
    let post_close_cleaned = numeric.volatile.delete(POST_CLOSE_ID).await;
    let named_closed_again = named.close().await;
    let numeric_closed = numeric.close().await;
    client.close().await;

    finish(vec![
        ("exercise failed", exercised),
        ("cleanup failed", cleaned),
        ("shared lifecycle check failed", root_live),
        ("post-close cleanup failed", post_close_cleaned),
        ("repeated named DB close failed", named_closed_again),
        ("numeric DB close failed", numeric_closed),
    ])
}
