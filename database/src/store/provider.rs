//! Database selection and the capability bundle every DB carries.

use super::refuses::Refuses;
use super::{Batcher, Driver, Migrator, Release, Result, SeriesReader, TimeSeries, Transactional};
use std::sync::Arc;

/// A backend bound to a client the caller owns. It has no CRUD of its own and
/// no close: until a database is selected there is nothing to read or write,
/// and the provider did not open the client.
#[async_trait::async_trait]
pub trait Provider: Send + Sync {
    /// Selects a database and returns the driver over it, reaching the server
    /// first so a bad address or a missing database fails here.
    async fn set_database(&self, name: &str) -> Result<DB>;
    /// Implementation name, for messages: "influxdb3", "arrow".
    fn backend(&self) -> &'static str;
}

/// What a backend actually has. Missing entries become refusals in [`DB::build`].
#[derive(Clone, Default)]
pub struct Capabilities {
    /// CRUD.
    pub driver: Option<Arc<dyn Driver>>,
    /// Transactions.
    pub tx: Option<Arc<dyn Transactional>>,
    /// Schema creation and inspection.
    pub schema: Option<Arc<dyn Migrator>>,
    /// Bulk writes and reads.
    pub batch: Option<Arc<dyn Batcher>>,
    /// Time windows and reductions.
    pub series: Option<Arc<dyn TimeSeries>>,
    /// Streaming window reads.
    pub streams: Option<Arc<dyn SeriesReader>>,
    /// What [`DB::close`] frees.
    pub release: Option<Arc<dyn Release>>,
}

/// One selected database: the CRUD driver plus every capability, each always
/// present. Where a backend lacks one, its methods return
/// [`Error::Unimplemented`](super::Error::Unimplemented) naming the backend,
/// rather than a missing field failing far from the wiring that caused it.
///
/// `DB` dereferences to its [`Driver`], so `db.create(..)` reads as a call on
/// the database. Clones share everything, including what `close` releases.
#[derive(Clone)]
pub struct DB {
    /// CRUD.
    pub driver: Arc<dyn Driver>,
    /// Transactions.
    pub tx: Arc<dyn Transactional>,
    /// Schema creation and inspection.
    pub schema: Arc<dyn Migrator>,
    /// Bulk writes and reads.
    pub batch: Arc<dyn Batcher>,
    /// Time windows and reductions.
    pub series: Arc<dyn TimeSeries>,
    /// Streaming window reads, a Rust extension.
    pub streams: Arc<dyn SeriesReader>,
    /// Implementation behind this database.
    pub backend: &'static str,
    /// The selected database name.
    pub name: String,
    pub(crate) raw: Capabilities,
    pub(crate) telemetry: telemetry::Telemetry,
}

impl DB {
    /// Assembles a DB from what the backend has, refusing the rest by name.
    /// The one place a backend needs, as runtime-go's `store.Build`.
    pub fn build(backend: &'static str, name: impl Into<String>, raw: Capabilities) -> Self {
        let refuses = Arc::new(Refuses(backend));
        macro_rules! or_refuse {
            ($field:ident) => {
                raw.$field.clone().unwrap_or_else(|| refuses.clone())
            };
        }
        Self {
            driver: or_refuse!(driver),
            tx: or_refuse!(tx),
            schema: or_refuse!(schema),
            batch: or_refuse!(batch),
            series: or_refuse!(series),
            streams: or_refuse!(streams),
            backend,
            name: name.into(),
            raw,
            telemetry: telemetry::Telemetry::default(),
        }
    }

    /// The backend's own capabilities, before refusals or instrumentation.
    pub fn capabilities(&self) -> &Capabilities {
        &self.raw
    }

    /// Drains and frees what selecting this database allocated: queued
    /// writes, open files. The caller's client stays open. Idempotent.
    pub async fn close(&self) -> Result<()> {
        match &self.raw.release {
            Some(release) => release.release().await,
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
