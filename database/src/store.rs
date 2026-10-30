//! Storage contracts and explicit capability composition.

use crate::{Aggregate, Buckets, Error, Range, Record, Resource, Result};
use std::sync::Arc;

/// Point-oriented storage. Rewriting a point identity may merge field values.
#[async_trait::async_trait]
pub trait TimeSeries: Send + Sync {
    /// Writes a validated batch. This is not a rollback transaction; failures
    /// after a server acknowledgement may leave committed points.
    async fn write(&self, resource: &Resource, records: &[Record]) -> Result<()>;
    /// Returns a bounded, ordered time window. Does not count the whole series.
    async fn range(&self, resource: &Resource, range: &Range) -> Result<Vec<Record>>;
    /// Reduces on the server rather than transferring raw observations.
    async fn aggregate(&self, resource: &Resource, query: &Aggregate) -> Result<Buckets>;
}

/// Record CRUD for future SQL/document drivers, addressed by Resource::key_column.
/// InfluxDB does not implement it.
#[async_trait::async_trait]
pub trait RecordStore: Send + Sync {
    /// Insert only, returning AlreadyExists on duplicate identity.
    async fn create(&self, resource: &Resource, record: &Record) -> Result<Record>;
    /// Read a key, returning NotFound when absent.
    async fn get(&self, resource: &Resource, key: &str) -> Result<Record>;
    /// Replace an existing record, returning NotFound when absent.
    async fn update(&self, resource: &Resource, record: &Record) -> Result<Record>;
    /// Delete an existing record, returning NotFound when absent.
    async fn delete(&self, resource: &Resource, key: &str) -> Result<()>;
}

/// Capabilities are explicit handles, never discovered by downcasting traits.
#[derive(Clone, Default)]
pub struct Capabilities {
    /// Streaming and keyset pagination, when provided.
    pub reader: Option<Arc<dyn crate::SeriesReader>>,
    /// Whole-table schema and data deletion, when provided.
    pub maintenance: Option<Arc<dyn crate::SeriesMaintenance>>,
    /// Selective historical row deletion, when provided.
    pub row_deletion: Option<Arc<dyn crate::RowDeletion>>,
    /// Present only when the backend provides record semantics.
    pub records: Option<Arc<dyn RecordStore>>,
    /// Present only when the backend provides time-series operations.
    pub series: Option<Arc<dyn TimeSeries>>,
}

/// One immutable database selection. Cloning preserves every capability.
/// Dropping a handle releases its references without closing another client's
/// pool. Explicitly created BatchWriters have their own draining close.
#[derive(Clone)]
pub struct DB {
    pub(crate) backend: &'static str,
    name: String,
    pub(crate) capabilities: Capabilities,
}

impl DB {
    /// Constructs a selection with all its declared capabilities.
    pub fn new(backend: &'static str, name: String, capabilities: Capabilities) -> Self {
        Self {
            backend,
            name,
            capabilities,
        }
    }

    /// Implementation name used in diagnostics.
    pub fn backend(&self) -> &'static str {
        self.backend
    }

    /// Selected database name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Record access, or a structured refusal naming this backend.
    pub fn records(&self) -> Result<&dyn RecordStore> {
        self.capabilities
            .records
            .as_deref()
            .ok_or(Error::Unsupported {
                backend: self.backend,
                capability: "record CRUD",
            })
    }

    /// Time-series access, or a structured refusal naming this backend.
    pub fn series(&self) -> Result<&dyn TimeSeries> {
        self.capabilities
            .series
            .as_deref()
            .ok_or(Error::Unsupported {
                backend: self.backend,
                capability: "time series",
            })
    }
}

/// Selects databases without mutating other selections.
#[async_trait::async_trait]
pub trait Provider: Send + Sync {
    /// Selects and probes an existing database; never implicitly creates one.
    async fn set_database(&self, name: &str) -> Result<DB>;
}
