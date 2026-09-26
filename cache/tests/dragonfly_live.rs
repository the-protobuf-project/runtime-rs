//! Live Dragonfly verification for the public RESP-compatible cache contract.
//!
//! This target is selected explicitly rather than marked ignored. Once the
//! `live-tests` feature is enabled, an unavailable or incompatible service is a
//! real failure; the test never returns early as a fake pass.
//!
//! Start the isolated, non-persistent service from the repository root:
//!
//! ```text
//! docker compose -p runtime-cache-dragonfly-live -f cache/docker/compose.dragonfly.live.yaml up -d --wait dragonfly
//! ```
//!
//! Run the test:
//!
//! ```text
//! cargo test -p runtime-cache --features live-tests --test dragonfly_live
//! ```
//!
//! Then remove only this Compose project's resources:
//!
//! ```text
//! docker compose -p runtime-cache-dragonfly-live -f cache/docker/compose.dragonfly.live.yaml down
//! ```
//!
//! `RUNTIME_CACHE_DRAGONFLY_ADDRESS` defaults to `127.0.0.1:16380`. Optional
//! username, password, and database variables support a caller-supplied
//! standalone service without logging credentials. Every run uses a UUID
//! prefix and prefix-scoped cleanup; neither FLUSHDB nor FLUSHALL is used.

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
    drivers::dragonfly::{DragonflyClient, DragonflyConfig, DragonflyProvider},
};
use uuid::Uuid;

/// Stable named namespace beneath the unique per-run prefix.
const NAMESPACE: &str = "live-contract";
/// Short fallback lease bounds abandoned value entries.
const TEST_TTL: Duration = Duration::from_secs(30);
/// Known IDs allow direct fallback cleanup if a namespace scan fails.
const VOLATILE_ID: &str = "volatile-one";
const DOCUMENT_ID: &str = "document-one";
const INDEXED_ID: &str = "indexed-one";
const ASIDE_ID: &str = "aside-one";
const ISOLATION_ID: &str = "isolation-one";
const POST_CLOSE_ID: &str = "post-close";

/// Picks a different valid index within Dragonfly's configured 16 databases.
fn derived_index(root_index: usize) -> usize {
    if root_index == 0 { 1 } else { 0 }
}

/// Exercises Dragonfly identity, capabilities, and native-index isolation.
async fn exercise(
    provider: &DragonflyProvider,
    named: &DB,
    derived: &DB,
    root_index: usize,
    alternate_index: usize,
    aside: &dyn Aside,
    loads: &AtomicUsize,
) -> Result<()> {
    ensure(
        provider.backend() == "dragonfly",
        "Provider reported wrong backend",
    )?;
    ensure(
        named.backend == "dragonfly",
        "named DB reported wrong backend",
    )?;
    ensure(named.name == NAMESPACE, "named DB reported wrong namespace")?;
    ensure(named.index == root_index, "named DB reported wrong index")?;
    ensure(
        derived.backend == "dragonfly",
        "derived DB reported wrong backend",
    )?;
    ensure(
        derived.name.is_empty(),
        "derived DB unexpectedly has a name",
    )?;
    ensure(
        derived.index == alternate_index,
        "derived DB reported wrong index",
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
        .touch(VOLATILE_ID, Duration::from_secs(45))
        .await?;
    let volatile_ttl = named.volatile.ttl(VOLATILE_ID).await?;
    ensure(
        volatile_ttl > Duration::ZERO && volatile_ttl <= Duration::from_secs(45),
        "Volatile PTTL fell outside the touched lease",
    )?;
    ensure(
        named
            .volatile
            .scan("volatile-*")
            .await?
            .iter()
            .any(|key| key.ends_with("cache:vol:volatile-one")),
        "Volatile scan omitted the stored key",
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
    ensure(
        named.document.keys().await?.contains(&document_id),
        "Document keys omitted the live ID",
    )?;
    ensure(
        named
            .document
            .list()
            .await?
            .contains(&b"document-two".to_vec()),
        "Document list omitted the updated value",
    )?;
    ensure(
        named.document.ttl(DOCUMENT_ID).await? > Duration::ZERO,
        "Document TTL did not report a live lease",
    )?;
    named.document.delete(DOCUMENT_ID).await?;
    ensure_not_found(
        named.document.get(DOCUMENT_ID, &mut Vec::new()).await,
        "Document delete left the value live",
    )?;

    let indexed_id = named
        .indexed
        .create(
            b"indexed-one",
            &options
                .clone()
                .with_id(INDEXED_ID)
                .with_index("tenant", "acme"),
        )
        .await?;
    ensure(
        named.indexed.ids_by_index("tenant", "acme").await? == vec![indexed_id.clone()],
        "Indexed lookup omitted the filed ID",
    )?;
    ensure(
        named.indexed.by_index("tenant", "acme").await? == vec![b"indexed-one".to_vec()],
        "Indexed lookup changed the filed value",
    )?;
    named
        .indexed
        .update(
            &indexed_id,
            b"indexed-two",
            &options.clone().with_index("tenant", "beta"),
        )
        .await?;
    ensure(
        named
            .indexed
            .ids_by_index("tenant", "acme")
            .await?
            .is_empty(),
        "Indexed refiling retained the old membership",
    )?;
    ensure(
        named.indexed.ids_by_index("tenant", "beta").await? == vec![indexed_id.clone()],
        "Indexed refiling omitted the new membership",
    )?;
    ensure(
        named.indexed.keys().await?.contains(&indexed_id),
        "Indexed enumeration omitted the live ID",
    )?;
    ensure(
        named.indexed.delete_by_index("tenant", "beta").await? == 1,
        "Indexed group deletion reported wrong count",
    )?;
    ensure_not_found(
        named.indexed.get(INDEXED_ID, &mut Vec::new()).await,
        "Indexed group deletion left the value live",
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

    named.volatile.set(ISOLATION_ID, b"root", &options).await?;
    derived
        .volatile
        .set(ISOLATION_ID, b"derived", &options)
        .await?;
    let mut root_value = Vec::new();
    let mut derived_value = Vec::new();
    named.volatile.get(ISOLATION_ID, &mut root_value).await?;
    derived
        .volatile
        .get(ISOLATION_ID, &mut derived_value)
        .await?;
    ensure(root_value == b"root", "root database value collided")?;
    ensure(
        derived_value == b"derived",
        "derived database value collided",
    )?;

    let deleted = provider.drop_database(NAMESPACE).await?;
    ensure(deleted > 0, "database drop reported no deleted keys")?;
    ensure_not_found(
        named.volatile.get(ISOLATION_ID, &mut Vec::new()).await,
        "database drop left a namespaced key live",
    )?;
    derived
        .volatile
        .get(ISOLATION_ID, &mut derived_value)
        .await?;
    ensure(
        derived_value == b"derived",
        "named database drop crossed into another native index",
    )
}

/// Attempts prefix cleanup plus direct cleanup for known entries and index DB.
async fn cleanup(
    provider: &DragonflyProvider,
    named: &DB,
    derived: &DB,
    aside: &dyn Aside,
) -> Result<()> {
    let mut failures = Vec::new();
    collect_failure(
        &mut failures,
        "namespace cleanup",
        provider.drop_database(NAMESPACE).await.map(|_| ()),
    );
    collect_failure(
        &mut failures,
        "Volatile fallback cleanup",
        named.volatile.delete(VOLATILE_ID).await,
    );
    collect_failure(
        &mut failures,
        "Document fallback cleanup",
        named.document.delete(DOCUMENT_ID).await,
    );
    collect_failure(
        &mut failures,
        "Indexed fallback cleanup",
        named.indexed.delete(INDEXED_ID).await,
    );
    collect_failure(
        &mut failures,
        "Aside fallback cleanup",
        aside.invalidate(ASIDE_ID).await,
    );
    collect_failure(
        &mut failures,
        "derived-index cleanup",
        derived.volatile.delete(ISOLATION_ID).await,
    );
    failures_result("cleanup", failures)
}

/// Proves closing a derived index leaves the root connection operational.
async fn verify_root_remains_live(derived: &DB, named: &DB) -> Result<()> {
    derived.close().await?;
    named
        .volatile
        .set(POST_CLOSE_ID, b"still-live", &Options::default())
        .await?;
    let mut value = Vec::new();
    named.volatile.get(POST_CLOSE_ID, &mut value).await?;
    ensure(
        value == b"still-live",
        "closing derived DB closed the root connection",
    )
}

/// Returns a semantic failure without panicking past asynchronous cleanup.
fn ensure(condition: bool, message: &str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(CacheError::Internal(format!(
            "live Dragonfly test: {message}"
        )))
    }
}

/// Requires one public operation to report an ordinary cache miss.
fn ensure_not_found<T>(result: Result<T>, message: &str) -> Result<()> {
    match result {
        Err(CacheError::NotFound) => Ok(()),
        Ok(_) => Err(CacheError::Internal(format!(
            "live Dragonfly test: {message}"
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

/// Converts accumulated failures into one contextual cache error.
fn failures_result(context: &str, failures: Vec<String>) -> Result<()> {
    if failures.is_empty() {
        Ok(())
    } else {
        Err(CacheError::Internal(format!(
            "live Dragonfly {context}: {}",
            failures.join("; ")
        )))
    }
}

/// Reads connection settings without exposing authentication values.
fn dragonfly_config() -> Result<DragonflyConfig> {
    let address = env::var("RUNTIME_CACHE_DRAGONFLY_ADDRESS")
        .unwrap_or_else(|_| "127.0.0.1:16380".to_owned());
    let username = env::var("RUNTIME_CACHE_DRAGONFLY_USERNAME").unwrap_or_default();
    let password = env::var("RUNTIME_CACHE_DRAGONFLY_PASSWORD").unwrap_or_default();
    let database = match env::var("RUNTIME_CACHE_DRAGONFLY_DATABASE") {
        Ok(value) => value.parse::<usize>().map_err(|error| {
            CacheError::Internal(format!(
                "live Dragonfly test: invalid RUNTIME_CACHE_DRAGONFLY_DATABASE: {error}"
            ))
        })?,
        Err(_) => 0,
    };
    Ok(DragonflyConfig {
        address,
        username,
        password,
        database,
        ..DragonflyConfig::default()
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
async fn test_dragonfly_live_public_cache_contract() -> Result<()> {
    let dragonfly_config = dragonfly_config()?;
    let root_index = dragonfly_config.database;
    let alternate_index = derived_index(root_index);
    let client = Arc::new(DragonflyClient::connect(dragonfly_config).await?);
    let prefix = format!("runtime-cache-live-{}", Uuid::new_v4());
    let provider = DragonflyProvider::new(
        client.clone(),
        Config {
            prefix,
            default_ttl: TEST_TTL,
            require_ttl: true,
            databases: vec![NAMESPACE.to_owned()],
            ..Config::default()
        },
    );

    if let Err(error) = provider.drop_database(NAMESPACE).await {
        client.close().await;
        return Err(error);
    }
    let named = match provider.set_database(NAMESPACE).await {
        Ok(db) => db,
        Err(error) => {
            client.close().await;
            return Err(error);
        }
    };
    let derived = match provider.select_index(alternate_index).await {
        Ok(db) => db,
        Err(error) => {
            let closed = named.close().await;
            client.close().await;
            return finish(vec![
                ("derived DB selection failed", Err(error)),
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

    let exercised = exercise(
        &provider,
        &named,
        &derived,
        root_index,
        alternate_index,
        aside.as_ref(),
        loads.as_ref(),
    )
    .await;
    let cleaned = cleanup(&provider, &named, &derived, aside.as_ref()).await;
    let root_live = verify_root_remains_live(&derived, &named).await;
    let post_close_cleaned = named.volatile.delete(POST_CLOSE_ID).await;
    let derived_closed_again = derived.close().await;
    let named_closed = named.close().await;
    client.close().await;

    finish(vec![
        ("exercise failed", exercised),
        ("cleanup failed", cleaned),
        ("root lifecycle check failed", root_live),
        ("post-close cleanup failed", post_close_cleaned),
        ("repeated derived DB close failed", derived_closed_again),
        ("named DB close failed", named_closed),
    ])
}
