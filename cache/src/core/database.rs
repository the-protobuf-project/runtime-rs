//! Construction and lifecycle for one selected cache database.

use std::{sync::Arc, time::Duration};

use futures::future::BoxFuture;
use tokio::sync::{Mutex, watch};

use crate::strategies::document::default_new_id;
use crate::strategies::{AsideImpl, DocumentImpl, IndexedImpl, VolatileImpl};
use crate::strategies::{flight::Flight, refresher::Refresher};
use crate::{CacheError, Result};

use super::{Aside, Capabilities, Document, Driver, Indexed, Keyspace, Loader, NewId, Volatile};

const LOAD_TIMEOUT: Duration = Duration::from_secs(30);
const NEGATIVE_TTL: Duration = Duration::from_secs(30);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const REFRESH_BUDGET: usize = 64;
const FLIGHT_BUDGET: usize = 2048;
const FLIGHT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_CONCURRENCY: usize = 16;

/// Asynchronous cleanup for resources derived while selecting one database.
pub type Release = Box<dyn FnOnce() -> BoxFuture<'static, Result<()>> + Send + 'static>;

/// Facts known by a backend Provider after selecting one database.
///
/// The backend supplies connection and namespace facts; core uses them to wire
/// strategy policy exactly once. `release` owns only resources derived for this
/// selection and must never close a caller-owned root client.
pub struct DatabaseSpec {
    /// Prefix shared by every key in this cache instance.
    pub prefix: String,
    /// Named database namespace, empty for numeric selection.
    pub namespace: String,
    /// Selected backend or emulated database index.
    pub database: usize,
    /// Whether the numeric index must be encoded into every backend key.
    pub embed_db: bool,
    /// TTL used when an operation supplies none.
    pub default_ttl: Duration,
    /// Aside stale window used when an operation supplies none.
    pub default_stale: Duration,
    /// Bound for multi-entry Driver fan-out; zero selects the core default.
    pub concurrency: usize,
    /// Whether an implicit permanent write is rejected.
    pub require_ttl: bool,
    /// Generates IDs for Document and Indexed creates that supply none.
    ///
    /// One instance is shared by both strategies. `None` uses UUID v4, the
    /// existing Rust default.
    pub new_id: Option<NewId>,
    /// Cleanup for resources derived by this database selection.
    pub release: Option<Release>,
}

impl Default for DatabaseSpec {
    fn default() -> Self {
        Self {
            prefix: String::new(),
            namespace: String::new(),
            database: 0,
            embed_db: false,
            default_ttl: Duration::ZERO,
            default_stale: Duration::ZERO,
            concurrency: 0,
            require_ttl: false,
            new_id: None,
            release: None,
        }
    }
}

struct AsideFactory {
    driver: Arc<dyn Driver>,
    keyspace: Keyspace,
    default_ttl: Duration,
    default_stale: Duration,
    require_ttl: bool,
    flight: Arc<Flight>,
    refresher: Arc<Refresher>,
}

struct CloseState {
    release: Option<Release>,
    receiver: Option<watch::Receiver<Option<Result<()>>>>,
}

struct Lifecycle {
    refresher: Arc<Refresher>,
    state: Mutex<CloseState>,
}

/// One selected database with every cache strategy wired over one Driver.
///
/// Document, Volatile, and Indexed are stable views. [`DB::aside`] creates
/// loader-specific read-through views that share this database's load and
/// refresh budgets. Closing drains those background refreshes before releasing
/// only resources derived for this selection.
pub struct DB {
    /// Enumerable ID-addressed values.
    pub document: Arc<dyn Document>,
    /// Direct key/value storage with no enumeration index.
    pub volatile: Arc<dyn Volatile>,
    /// Document storage with secondary field/value lookup.
    pub indexed: Arc<dyn Indexed>,
    /// Driver implementation name used for diagnostics.
    pub backend: String,
    /// Selected namespace, empty for numeric selection.
    pub name: String,
    /// Selected backend or emulated database index.
    pub index: usize,
    aside: AsideFactory,
    lifecycle: Lifecycle,
}

impl DB {
    /// Creates a read-through view over a caller-provided Loader.
    ///
    /// **Cost**: Local allocation only; no backend round trip.
    /// **Concurrency**: Every view from this DB shares one Flight and Refresher.
    /// **Side effects**: None until the returned Aside performs an operation.
    pub fn aside(&self, loader: Loader) -> Arc<dyn Aside> {
        Arc::new(AsideImpl::new(
            self.aside.driver.clone(),
            self.aside.keyspace.clone(),
            loader,
            self.aside.default_ttl,
            self.aside.default_stale,
            self.aside.require_ttl,
            self.aside.flight.clone(),
            self.aside.refresher.clone(),
            NEGATIVE_TTL,
        ))
    }

    /// Drains background refreshes, then releases derived backend resources.
    ///
    /// Close is idempotent: concurrent or repeated callers observe the same
    /// stored result and the release callback runs at most once. Drain/release
    /// runs independently of the first waiter, so cancelling that caller does
    /// not lose cleanup. Rust Drop cannot await, so owners of derived resources
    /// must call this explicitly.
    pub async fn close(&self) -> Result<()> {
        let mut receiver = {
            let mut state = self.lifecycle.state.lock().await;
            match &state.receiver {
                Some(receiver) => receiver.clone(),
                None => {
                    let release = state.release.take();
                    let refresher = self.lifecycle.refresher.clone();
                    let (sender, receiver) = watch::channel(None);
                    state.receiver = Some(receiver.clone());
                    tokio::spawn(async move {
                        let drained = refresher.drain(DRAIN_TIMEOUT).await;
                        let released = match release {
                            Some(release) => release().await,
                            None => Ok(()),
                        };
                        let _ = sender.send(Some(combine_close_results(drained, released)));
                    });
                    receiver
                }
            }
        };

        loop {
            if let Some(result) = receiver.borrow().clone() {
                return result;
            }
            receiver.changed().await.map_err(|_| {
                CacheError::Internal("database close ended without publishing a result".to_owned())
            })?;
        }
    }
}

fn combine_close_results(drained: Result<()>, released: Result<()>) -> Result<()> {
    match (drained, released) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(drain), Err(release)) => Err(CacheError::Internal(format!(
            "database close failed while draining ({drain}) and releasing ({release})"
        ))),
    }
}

/// Wires a selected Driver into all four cache strategies.
///
/// Capabilities are resolved from the explicit bundle once here. One Flight
/// and one Refresher belong to the returned database and are shared by every
/// Aside view.
pub fn build_database(
    driver: Arc<dyn Driver>,
    capabilities: Capabilities,
    spec: DatabaseSpec,
) -> DB {
    let concurrency = if spec.concurrency == 0 {
        DEFAULT_CONCURRENCY
    } else {
        spec.concurrency
    };
    let keyspace = Keyspace::new(&spec.prefix, &spec.namespace, spec.database, spec.embed_db);
    let sets = capabilities.sets();
    let new_id = match spec.new_id {
        Some(new_id) => new_id,
        None => default_new_id(),
    };
    let flight = Arc::new(Flight::new(FLIGHT_BUDGET, FLIGHT_TIMEOUT));
    let refresher = Arc::new(Refresher::new(REFRESH_BUDGET, LOAD_TIMEOUT));

    let document: Arc<dyn Document> = Arc::new(DocumentImpl::new_with_id(
        driver.clone(),
        sets.clone(),
        keyspace.clone(),
        spec.default_ttl,
        spec.require_ttl,
        new_id.clone(),
    ));
    let volatile: Arc<dyn Volatile> = Arc::new(VolatileImpl::new(
        driver.clone(),
        keyspace.clone(),
        spec.default_ttl,
        spec.require_ttl,
    ));
    let indexed: Arc<dyn Indexed> = Arc::new(IndexedImpl::new_with_id(
        driver.clone(),
        sets,
        keyspace.clone(),
        spec.default_ttl,
        spec.require_ttl,
        concurrency,
        new_id,
    ));

    DB {
        document,
        volatile,
        indexed,
        backend: driver.name().to_owned(),
        name: spec.namespace,
        index: spec.database,
        aside: AsideFactory {
            driver,
            keyspace,
            default_ttl: spec.default_ttl,
            default_stale: spec.default_stale,
            require_ttl: spec.require_ttl,
            flight,
            refresher: refresher.clone(),
        },
        lifecycle: Lifecycle {
            refresher,
            state: Mutex::new(CloseState {
                release: spec.release,
                receiver: None,
            }),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use futures::FutureExt;

    use super::*;
    use crate::core::{MemoryDriver, MemorySets, Options};

    #[tokio::test]
    async fn test_database_build_wires_metadata_and_all_fixed_strategies() {
        let driver = Arc::new(MemoryDriver::new());
        let capabilities = Capabilities::new().with_sets(Arc::new(MemorySets::new()));
        let db = build_database(driver, capabilities, DatabaseSpec {
            prefix: "app".to_owned(),
            namespace: "orders".to_owned(),
            database: 3,
            default_ttl: Duration::from_secs(60),
            ..DatabaseSpec::default()
        });

        assert_eq!(db.backend, "memory");
        assert_eq!(db.name, "orders");
        assert_eq!(db.index, 3);

        db.volatile
            .set("session", b"volatile", &Options::default())
            .await
            .unwrap();
        let document_id = db
            .document
            .create(b"document", &Options::default().with_id("document-id"))
            .await
            .unwrap();
        db.indexed
            .create(
                b"indexed",
                &Options::default()
                    .with_id("indexed-id")
                    .with_index("tenant", "acme"),
            )
            .await
            .unwrap();

        assert_eq!(document_id, "document-id");
        assert_eq!(
            db.indexed.ids_by_index("tenant", "acme").await.unwrap(),
            vec!["indexed-id".to_owned()]
        );
    }

    #[tokio::test]
    async fn test_database_build_keeps_unsupported_strategy_fields_present() {
        let db = build_database(
            Arc::new(MemoryDriver::new()),
            Capabilities::new(),
            DatabaseSpec::default(),
        );

        assert!(matches!(
            db.document.keys().await,
            Err(CacheError::Unsupported)
        ));
        assert!(matches!(
            db.indexed.ids_by_index("tenant", "acme").await,
            Err(CacheError::Unsupported)
        ));
    }

    #[tokio::test]
    async fn test_database_build_shares_custom_id_generator_across_strategies() {
        let sequence = Arc::new(AtomicUsize::new(1));
        let next = sequence.clone();
        let db = build_database(
            Arc::new(MemoryDriver::new()),
            Capabilities::new(),
            DatabaseSpec {
                new_id: Some(Arc::new(move || {
                    format!("generated-{}", next.fetch_add(1, Ordering::SeqCst))
                })),
                ..DatabaseSpec::default()
            },
        );

        let document_id = db
            .document
            .create(b"document", &Options::default())
            .await
            .unwrap();
        let indexed_id = db
            .indexed
            .create(b"indexed", &Options::default())
            .await
            .unwrap();

        assert_eq!(document_id, "generated-1");
        assert_eq!(indexed_id, "generated-2");
        assert_eq!(sequence.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_database_build_preserves_caller_selected_id_without_generation() {
        let calls = Arc::new(AtomicUsize::new(0));
        let generated = calls.clone();
        let db = build_database(
            Arc::new(MemoryDriver::new()),
            Capabilities::new(),
            DatabaseSpec {
                new_id: Some(Arc::new(move || {
                    generated.fetch_add(1, Ordering::SeqCst);
                    "unused".to_owned()
                })),
                ..DatabaseSpec::default()
            },
        );

        let document_id = db
            .document
            .create(b"document", &Options::default().with_id("document-id"))
            .await
            .unwrap();
        let indexed_id = db
            .indexed
            .create(b"indexed", &Options::default().with_id("indexed-id"))
            .await
            .unwrap();

        assert_eq!(document_id, "document-id");
        assert_eq!(indexed_id, "indexed-id");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_database_aside_views_share_one_flight_group() {
        let db = build_database(
            Arc::new(MemoryDriver::new()),
            Capabilities::new(),
            DatabaseSpec {
                default_ttl: Duration::from_secs(60),
                ..DatabaseSpec::default()
            },
        );
        let loads = Arc::new(AtomicUsize::new(0));
        let first_loads = loads.clone();
        let first: Loader = Arc::new(move |_| {
            let loads = first_loads.clone();
            async move {
                loads.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(br#""value""#.to_vec())
            }
            .boxed()
        });
        let second_loads = loads.clone();
        let second: Loader = Arc::new(move |_| {
            let loads = second_loads.clone();
            async move {
                loads.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(br#""value""#.to_vec())
            }
            .boxed()
        });
        let first = db.aside(first);
        let second = db.aside(second);
        let options = Options::default();
        let mut first_value = Vec::new();
        let mut second_value = Vec::new();

        let (first_result, second_result) = tokio::join!(
            first.get_or_load("same", &mut first_value, &options),
            second.get_or_load("same", &mut second_value, &options),
        );

        first_result.unwrap();
        second_result.unwrap();
        assert_eq!(first_value, br#""value""#);
        assert_eq!(second_value, br#""value""#);
        assert_eq!(loads.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_database_close_releases_once_when_called_repeatedly() {
        let releases = Arc::new(AtomicUsize::new(0));
        let release_count = releases.clone();
        let db = build_database(
            Arc::new(MemoryDriver::new()),
            Capabilities::new(),
            DatabaseSpec {
                release: Some(Box::new(move || {
                    let releases = release_count.clone();
                    async move {
                        releases.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                    .boxed()
                })),
                ..DatabaseSpec::default()
            },
        );

        db.close().await.unwrap();
        db.close().await.unwrap();

        assert_eq!(releases.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_database_close_caller_cancellation_does_not_cancel_release() {
        let releases = Arc::new(AtomicUsize::new(0));
        let release_started = Arc::new(tokio::sync::Notify::new());
        let allow_release = Arc::new(tokio::sync::Notify::new());
        let release_count = releases.clone();
        let started = release_started.clone();
        let allowed = allow_release.clone();
        let db = Arc::new(build_database(
            Arc::new(MemoryDriver::new()),
            Capabilities::new(),
            DatabaseSpec {
                release: Some(Box::new(move || {
                    let releases = release_count.clone();
                    let started = started.clone();
                    let allowed = allowed.clone();
                    async move {
                        started.notify_one();
                        allowed.notified().await;
                        releases.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                    .boxed()
                })),
                ..DatabaseSpec::default()
            },
        ));
        let closing_db = db.clone();
        let first_waiter = tokio::spawn(async move { closing_db.close().await });
        release_started.notified().await;

        first_waiter.abort();
        allow_release.notify_one();
        db.close().await.unwrap();

        assert_eq!(releases.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_database_close_drains_refresh_before_release() {
        let refresh_finished = Arc::new(AtomicBool::new(false));
        let released_after_refresh = Arc::new(AtomicBool::new(false));
        let loader_runs = Arc::new(AtomicUsize::new(0));
        let loader_finished = refresh_finished.clone();
        let runs = loader_runs.clone();
        let loader: Loader = Arc::new(move |_| {
            let finished = loader_finished.clone();
            let runs = runs.clone();
            async move {
                let run = runs.fetch_add(1, Ordering::SeqCst);
                if run > 0 {
                    tokio::time::sleep(Duration::from_millis(40)).await;
                    finished.store(true, Ordering::SeqCst);
                }
                Ok(br#""value""#.to_vec())
            }
            .boxed()
        });
        let release_finished = refresh_finished.clone();
        let release_observed = released_after_refresh.clone();
        let db = build_database(
            Arc::new(MemoryDriver::new()),
            Capabilities::new(),
            DatabaseSpec {
                release: Some(Box::new(move || {
                    let finished = release_finished.clone();
                    let observed = release_observed.clone();
                    async move {
                        observed.store(finished.load(Ordering::SeqCst), Ordering::SeqCst);
                        Ok(())
                    }
                    .boxed()
                })),
                ..DatabaseSpec::default()
            },
        );
        let aside = db.aside(loader);
        let options = Options::default()
            .with_ttl(Duration::from_millis(10))
            .with_stale(Duration::from_secs(1));
        aside
            .get_or_load("item", &mut Vec::new(), &options)
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        aside
            .get_or_load("item", &mut Vec::new(), &options)
            .await
            .unwrap();

        db.close().await.unwrap();

        assert!(refresh_finished.load(Ordering::SeqCst));
        assert!(released_after_refresh.load(Ordering::SeqCst));
    }

    #[test]
    fn test_database_close_combines_drain_and_release_errors() {
        let result = combine_close_results(
            Err(CacheError::Internal("drain failed".to_owned())),
            Err(CacheError::Internal("release failed".to_owned())),
        );

        match result {
            Err(CacheError::Internal(message)) => {
                assert!(message.contains("drain failed"));
                assert!(message.contains("release failed"));
            }
            other => panic!("expected combined close failure, got {other:?}"),
        }
    }
}
