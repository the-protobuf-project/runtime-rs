//! Selecting a database from a client, and the [`DB`] that results: the driver plus every
//! capability, each either the backend's or a refusal naming it. Go's `store/provider.go`.

use std::sync::Arc;

use futures::future::BoxFuture;

use super::refuses::Refuses;
use super::{Batcher, Driver, Migrator, Transactional, Watcher};
use crate::{Error, Result};

/// Chooses a database from a client the caller owns. Go's `store.Provider`.
#[async_trait::async_trait]
pub trait Provider: Send + Sync {
    /// The driver over database `name`, which overrides [`Resource::schema`](super::Resource::schema) for every
    /// resource driven through it; empty selects the client's default.
    async fn set_database(&self, name: &str) -> Result<DB>;
    /// The backend's name, e.g. `orm`.
    fn backend(&self) -> &'static str;
}

/// Frees what a [`DB`] holds: it never closes a client the caller supplied.
pub type ReleaseFn = Arc<dyn Fn() -> BoxFuture<'static, Result<()>> + Send + Sync>;

/// What a backend can do. `driver` is required; a capability left `None` becomes a refusal.
#[derive(Clone)]
pub struct Capabilities {
    /// CRUD.
    pub driver: Arc<dyn Driver>,
    /// Transactions.
    pub tx: Option<Arc<dyn Transactional>>,
    /// Schema.
    pub schema: Option<Arc<dyn Migrator>>,
    /// Bulk reads and writes.
    pub batch: Option<Arc<dyn Batcher>>,
    /// Change streams.
    pub watch: Option<Arc<dyn Watcher>>,
}

impl Capabilities {
    /// CRUD alone; add capabilities with struct update syntax.
    pub fn new(driver: Arc<dyn Driver>) -> Self {
        Self {
            driver,
            tx: None,
            schema: None,
            batch: None,
            watch: None,
        }
    }
}

/// One selected database: the driver, dereferenced to for CRUD as Go embeds it, and every
/// capability, never missing: one the backend lacks refuses with
/// [`Error::Unimplemented`] naming it. Go's `store.DB`.
#[derive(Clone)]
pub struct DB {
    driver: Arc<dyn Driver>,
    /// Transactions.
    pub tx: Arc<dyn Transactional>,
    /// Schema.
    pub schema: Arc<dyn Migrator>,
    /// Bulk reads and writes.
    pub batch: Arc<dyn Batcher>,
    /// Change streams.
    pub watch: Arc<dyn Watcher>,
    /// The backend's name.
    pub backend: &'static str,
    /// The selected database's name.
    pub name: String,
    batch_native: bool,
    pub(crate) release: Option<ReleaseFn>,
    /// The backend's own DB when this one observes it, so observing again replaces the
    /// observation rather than stacking a second one.
    pub(crate) raw: Option<Arc<DB>>,
}

impl DB {
    /// Wires `caps`, filling each missing capability with a refusal naming `backend`. Go's
    /// `store.Build`.
    pub fn build(
        backend: &'static str,
        name: impl Into<String>,
        caps: Capabilities,
        release: Option<ReleaseFn>,
    ) -> Self {
        let refuses = Arc::new(Refuses(backend));
        Self {
            driver: caps.driver,
            tx: caps.tx.unwrap_or_else(|| refuses.clone()),
            schema: caps.schema.unwrap_or_else(|| refuses.clone()),
            batch_native: caps.batch.is_some(),
            batch: caps.batch.unwrap_or_else(|| refuses.clone()),
            watch: caps.watch.unwrap_or(refuses),
            backend,
            name: name.into(),
            release,
            raw: None,
        }
    }

    /// The driver.
    pub fn driver(&self) -> &Arc<dyn Driver> {
        &self.driver
    }

    /// Whether the backend reads and writes in bulk, rather than refusing to.
    pub fn has_batch(&self) -> bool {
        self.batch_native
    }

    /// Frees what this selection holds. A no-op without a release.
    pub async fn close(&self) -> Result<()> {
        match &self.release {
            Some(release) => release().await,
            None => Ok(()),
        }
    }
}

impl std::ops::Deref for DB {
    type Target = dyn Driver;

    fn deref(&self) -> &Self::Target {
        self.driver.as_ref()
    }
}

impl std::fmt::Debug for DB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DB")
            .field("backend", &self.backend)
            .field("name", &self.name)
            .finish()
    }
}

/// Fails unless `name` can go where an identifier goes: letters, digits after the first,
/// and underscores, at most 63 of them. Empty selects the default. Go's
/// `store.CheckDatabaseName`.
pub fn check_database_name(name: &str) -> Result<()> {
    if name.len() > 63 {
        return Err(Error::Invalid(format!(
            "database name {name:?} is longer than 63 characters"
        )));
    }
    for (i, c) in name.chars().enumerate() {
        if !(c.is_ascii_alphabetic() || c == '_' || (i > 0 && c.is_ascii_digit())) {
            return Err(Error::Invalid(format!(
                "database name {name:?} cannot contain {c:?}: it goes into an identifier \
                 position, so only letters, digits and underscore are accepted"
            )));
        }
    }
    Ok(())
}
