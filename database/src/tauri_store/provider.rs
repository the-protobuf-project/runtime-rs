//! Selecting a database from a Tauri store.

use std::path::Path;
use std::sync::Arc;

use tauri::{AppHandle, Runtime};
use tauri_plugin_store::{Store, StoreBuilder};

use super::TauriStoreDriver;
use super::driver::Inner;
use super::keys::Keys;
use crate::records::{Capabilities, DB, Provider, ReleaseFn, check_database_name};
use crate::{Error, Result};

/// Records in one Tauri store file. A selected database is a key prefix in that file, so one
/// file serves many tenants; open another provider for another file.
pub struct TauriStoreProvider<R: Runtime> {
    store: Arc<Store<R>>,
    lock: Arc<tokio::sync::Mutex<()>>,
    durable: bool,
}

impl<R: Runtime> TauriStoreProvider<R> {
    /// Records in the store at `path`, relative to the app's data directory unless absolute,
    /// with the plugin's autosave off: the driver saves after each write, and a transaction
    /// once at its end, so no half-finished transaction reaches the file. The app must have
    /// `tauri_plugin_store::Builder::default().build()` registered.
    pub fn open(app: &AppHandle<R>, path: impl AsRef<Path>) -> Result<Self> {
        let store = StoreBuilder::new(app, path.as_ref())
            .disable_auto_save()
            .build()
            .map_err(|e| Error::Backend {
                backend: "tauri-store",
                source: Box::new(e),
            })?;
        Ok(Self::new(store))
    }

    /// Records in a store the caller built. With the plugin's autosave on, a transaction's
    /// writes can reach the file before it commits; [`TauriStoreProvider::open`] turns it off.
    pub fn new(store: Arc<Store<R>>) -> Self {
        Self {
            store,
            lock: Arc::default(),
            durable: true,
        }
    }

    /// Whether each write is saved before it returns (the default). Off, writes stay in
    /// memory until the plugin's autosave, an explicit save, or [`DB::close`].
    pub fn durable(mut self, durable: bool) -> Self {
        self.durable = durable;
        self
    }

    /// The store.
    pub fn store(&self) -> &Arc<Store<R>> {
        &self.store
    }
}

#[async_trait::async_trait]
impl<R: Runtime> Provider for TauriStoreProvider<R> {
    /// The driver over database `name`, a prefix on its keys; empty selects the default. It
    /// supports transactions, schema and bulk; watching is refused. Closing the result saves
    /// the file and never closes the store, which the app owns.
    async fn set_database(&self, name: &str) -> Result<DB> {
        check_database_name(name)?;
        let inner = Arc::new(Inner {
            store: self.store.clone(),
            keys: Keys::new(name),
            name: name.to_owned(),
            lock: self.lock.clone(),
            durable: self.durable,
        });
        let driver = Arc::new(TauriStoreDriver {
            inner: inner.clone(),
            tx: None,
        });
        let caps = Capabilities {
            tx: Some(driver.clone()),
            schema: Some(driver.clone()),
            batch: Some(driver.clone()),
            ..Capabilities::new(driver)
        };
        let release: ReleaseFn = Arc::new(move || {
            let inner = inner.clone();
            Box::pin(async move {
                let _held = inner.lock.lock().await;
                inner.save()
            })
        });
        Ok(crate::records::observe::instrument_default(DB::build(
            "tauri-store",
            name,
            caps,
            Some(release),
        )))
    }

    fn backend(&self) -> &'static str {
        "tauri-store"
    }
}
