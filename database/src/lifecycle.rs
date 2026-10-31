//! Explicit lifecycle capabilities; broad deletion is never a range fallback.
use crate::{Predicate, Resource, Result};
use std::time::Duration;

/// Database-wide expiry selected explicitly during database creation.
#[derive(Clone, Copy, Debug)]
pub enum Retention {
    /// Keep data indefinitely.
    Infinite,
    /// Retain this period. InfluxDB Core requires whole minutes of at least
    /// one hour and cannot change the period after database creation.
    For(Duration),
}

/// Time selection for backends that support row-level historical deletion.
#[derive(Clone, Debug)]
pub struct DeleteRange {
    /// Inclusive epoch-nanosecond lower bound.
    pub start: i64,
    /// Exclusive epoch-nanosecond upper bound.
    pub end: i64,
    /// Additional comparisons, ANDed; no pagination or result-size limit.
    pub predicates: Vec<Predicate>,
}

/// Explicit table removal, separate from selective row deletion and record CRUD.
#[async_trait::async_trait]
pub trait SeriesMaintenance: Send + Sync {
    /// Removes the table schema and ALL observations across every tag set.
    /// Physical reclamation follows the server's soft-deletion/grace policy.
    /// This is never a fallback for selective historical deletion.
    async fn drop_table(&self, resource: &Resource) -> Result<()>;
}

/// Implement only when the backend supports selective row deletion.
/// InfluxDB 3 Core does not; no broad-delete emulation is permitted.
#[async_trait::async_trait]
pub trait RowDeletion: Send + Sync {
    /// Deletes only observations selected by this half-open window/predicates.
    async fn delete_range(&self, resource: &Resource, range: &DeleteRange) -> Result<()>;
}
