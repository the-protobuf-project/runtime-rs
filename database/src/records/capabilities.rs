//! What a backend may or may not do beyond CRUD: transactions, schema, bulk reads and
//! writes, change streams. Go's `store/capabilities.go`.

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use prost_reflect::DynamicMessage;

use super::{DB, Resource, WriteResult};
use crate::Result;

/// Work run inside a transaction, given the transaction-scoped [`DB`].
pub type TxWork = Box<dyn for<'a> FnOnce(&'a DB) -> BoxFuture<'a, Result<()>> + Send>;

/// Runs work atomically: committed when it returns `Ok`, rolled back otherwise. Go's
/// `store.Transactional`.
#[async_trait::async_trait]
pub trait Transactional: Send + Sync {
    /// Runs `work` in one transaction.
    async fn run(&self, work: TxWork) -> Result<()>;
}

/// Creates and inspects the table, collection or keyspace a resource describes. Go's
/// `store.Migrator`.
#[async_trait::async_trait]
pub trait Migrator: Send + Sync {
    /// Creates what `res` needs, its indexes included; a no-op when it exists.
    async fn ensure_schema(&self, res: &Resource) -> Result<()>;
    /// Drops what `ensure_schema` created.
    async fn drop_schema(&self, res: &Resource) -> Result<()>;
    /// Whether what `res` needs exists.
    async fn has_schema(&self, res: &Resource) -> Result<bool>;
}

/// Bulk writes and reads in one round trip. Go's `store.Batcher`.
#[async_trait::async_trait]
pub trait Batcher: Send + Sync {
    /// Creates every message.
    async fn create_many(
        &self,
        res: &Resource,
        msgs: &[DynamicMessage],
    ) -> Result<Vec<WriteResult>>;
    /// The records under `keys`, in order; `None` where a key has no record.
    async fn get_many(
        &self,
        res: &Resource,
        keys: &[String],
    ) -> Result<Vec<Option<DynamicMessage>>>;
}

/// Streams changes to a resource's records. Go's `store.Watcher`.
#[async_trait::async_trait]
pub trait Watcher: Send + Sync {
    /// Changes from now, or from `opts.resume`.
    async fn watch(
        &self,
        res: &Resource,
        opts: WatchOptions,
    ) -> Result<BoxStream<'static, Result<Change>>>;
}

/// Where a watch starts. Go's `store.WatchOptions`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WatchOptions {
    /// A [`Change::resume`] token to continue after; empty starts from now.
    pub resume: String,
}

/// What happened to a record. Go's `store.ChangeKind`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ChangeKind {
    /// Not one of the below.
    #[default]
    Unknown,
    /// Created.
    Created,
    /// Updated.
    Updated,
    /// Deleted.
    Deleted,
}

/// One change to one record. Go's `store.Change`.
#[derive(Clone, Debug)]
pub struct Change {
    /// What happened.
    pub kind: ChangeKind,
    /// The record's key.
    pub key: String,
    /// The record after the change; `None` for a delete.
    pub message: Option<DynamicMessage>,
    /// A token a later watch resumes after.
    pub resume: String,
}
