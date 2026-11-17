//! [`wrap`]: the cached driver with the database's capabilities carried across, its
//! transactions invalidating what they committed. Go's `cached.Wrap` and `cachedTx`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use futures::FutureExt;
use futures::future::BoxFuture;
use prost_reflect::DynamicMessage;

use super::bulk::{Touch, Touched, bulk};
use super::{Cache, Driver};
use crate::Result;
use crate::records::{
    self, Batcher, Capabilities, DB, ListOptions, ListResult, ReleaseFn, Resource, Transactional,
    TxWork, WriteResult, key_of,
};

/// `db` with its driver read through `cache`, keeping everything else. Go's `cached.Wrap`.
///
/// Schema and watch are carried across unchanged; transactions are wrapped so writes
/// committed inside one invalidate what they touched; bulk writes, where the backend has
/// them, invalidate what they wrote. The backend reads `<backend>+cache`, the name stays,
/// and closing the result closes `db`.
///
/// Go's `Wrap` loses `Batcher` and `Watcher`, which Go finds by asserting on the driver and
/// the cached driver does not implement: here they survive, as the decorator promises.
pub fn wrap(db: &DB, cache: Arc<dyn Cache>) -> DB {
    let driver = Arc::new(Driver::new(db.driver().clone(), cache));
    let tx = Arc::new(CachedTx {
        next: db.tx.clone(),
        driver: driver.clone(),
    });
    let batch = bulk(db, Touch::Forget(driver.clone()));
    rebuild(db, suffixed(db.backend), driver, tx, batch)
}

/// `db` with a different driver, transaction runner and bulk writer, and its schema, watch,
/// name and release kept.
fn rebuild(
    db: &DB,
    backend: &'static str,
    driver: Arc<dyn records::Driver>,
    tx: Arc<dyn Transactional>,
    batch: Option<Arc<dyn Batcher>>,
) -> DB {
    let inner = db.clone();
    let release: ReleaseFn = Arc::new(move || {
        let inner = inner.clone();
        async move { inner.close().await }.boxed()
    });
    let caps = Capabilities {
        driver,
        tx: Some(tx),
        schema: Some(db.schema.clone()),
        batch,
        watch: Some(db.watch.clone()),
    };
    DB::build(backend, db.name.clone(), caps, Some(release))
}

/// `<backend>+cache`, interned: [`DB::build`] takes a `&'static str`, and a process wraps
/// few distinct backends, so each name is allocated once.
fn suffixed(backend: &'static str) -> &'static str {
    static NAMES: OnceLock<Mutex<HashMap<&'static str, &'static str>>> = OnceLock::new();
    let mut names = NAMES
        .get_or_init(Default::default)
        .lock()
        .expect("names lock");
    names
        .entry(backend)
        .or_insert_with(|| Box::leak(format!("{backend}+cache").into_boxed_str()))
}

/// Runs a transaction on the underlying database and invalidates what it wrote once it
/// commits. Reads inside go straight to the backing store: a transaction has to see its own
/// writes, and a cache that has not observed them cannot show them. Writes are recorded
/// rather than invalidated as they happen, because a rollback would otherwise have thrown
/// away entries that are still correct. Go's `cachedTx`.
struct CachedTx {
    next: Arc<dyn Transactional>,
    driver: Arc<Driver>,
}

#[async_trait::async_trait]
impl Transactional for CachedTx {
    async fn run(&self, work: TxWork) -> Result<()> {
        let touched = Touched::default();
        let recording = touched.clone();
        let body: TxWork = Box::new(move |inner: &DB| -> BoxFuture<'_, Result<()>> {
            // The body's driver records what it wrote; every other capability is the
            // transaction's own.
            let driver = Arc::new(Recorder {
                next: inner.driver().clone(),
                touched: recording.clone(),
            });
            let batch = bulk(inner, Touch::Record(recording));
            let bound = rebuild(inner, inner.backend, driver, inner.tx.clone(), batch);
            async move { work(&bound).await }.boxed()
        });
        self.next.run(body).await?;
        // Committed, so what it wrote is now what a reader should see.
        let touched = std::mem::take(&mut *touched.lock().expect("touched lock"));
        for (res, key) in touched {
            self.driver.forget(&res, &key).await;
        }
        Ok(())
    }
}

/// The driver handed to a transaction body: every write passes through to the transaction's
/// driver and is remembered for after the commit. Go's `recorder`.
struct Recorder {
    next: Arc<dyn records::Driver>,
    touched: Touched,
}

impl Recorder {
    fn wrote(&self, res: &Resource, key: Result<String>) {
        if let Ok(key) = key {
            let mut touched = self.touched.lock().expect("touched lock");
            touched.push((res.clone(), key));
        }
    }
}

#[async_trait::async_trait]
impl records::Driver for Recorder {
    async fn create(&self, res: &Resource, msg: &DynamicMessage) -> Result<WriteResult> {
        let out = self.next.create(res, msg).await?;
        self.wrote(res, key_of(res, &out.message));
        Ok(out)
    }

    async fn get(&self, res: &Resource, key: &str) -> Result<DynamicMessage> {
        self.next.get(res, key).await
    }

    async fn update(&self, res: &Resource, msg: &DynamicMessage) -> Result<WriteResult> {
        let out = self.next.update(res, msg).await?;
        self.wrote(res, key_of(res, msg));
        Ok(out)
    }

    async fn delete(&self, res: &Resource, key: &str) -> Result<()> {
        self.next.delete(res, key).await?;
        self.wrote(res, Ok(key.to_owned()));
        Ok(())
    }

    async fn list(&self, res: &Resource, opts: &ListOptions) -> Result<ListResult> {
        self.next.list(res, opts).await
    }

    async fn count(&self, res: &Resource, opts: &ListOptions) -> Result<i64> {
        self.next.count(res, opts).await
    }

    async fn exists(&self, res: &Resource, key: &str) -> Result<bool> {
        self.next.exists(res, key).await
    }
}
