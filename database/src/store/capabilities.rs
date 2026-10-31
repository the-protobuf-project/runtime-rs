//! Capabilities beyond CRUD that a backend has only if it can, as runtime-go.

use super::{DB, Record, Resource, Result, WriteResult};
use futures::future::BoxFuture;

/// Work run inside a transaction, given the transaction's own DB.
pub type TxWork = Box<dyn for<'a> FnOnce(&'a DB) -> BoxFuture<'a, Result<()>> + Send>;

/// Several operations applied atomically.
#[async_trait::async_trait]
pub trait Transactional: Send + Sync {
    /// Runs `work` and commits when it returns Ok, rolling back otherwise.
    async fn run(&self, work: TxWork) -> Result<()>;
}

/// Creates and inspects the backend structures a [`Resource`] describes.
#[async_trait::async_trait]
pub trait Migrator: Send + Sync {
    /// Creates the table, collection or directory when it is missing.
    async fn ensure_schema(&self, res: &Resource) -> Result<()>;
    /// Removes the structure and everything stored in it.
    async fn drop_schema(&self, res: &Resource) -> Result<()>;
    /// Whether the structure exists.
    async fn has_schema(&self, res: &Resource) -> Result<bool>;
}

/// Many records in one round trip.
#[async_trait::async_trait]
pub trait Batcher: Send + Sync {
    /// Stores every record. A failure part-way may leave earlier ones written;
    /// backends say whether a batch is atomic.
    async fn create_many(&self, res: &Resource, records: &[Record]) -> Result<Vec<WriteResult>>;
    /// The records under `keys`, in order.
    async fn get_many(&self, res: &Resource, keys: &[String]) -> Result<Vec<Record>>;
}

/// Frees what selecting a database allocated. Called by [`DB::close`].
#[async_trait::async_trait]
pub trait Release: Send + Sync {
    /// Drains and closes; the caller's own clients stay open.
    async fn release(&self) -> Result<()>;
}
