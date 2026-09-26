//! Public contract for enumerable, ID-addressed cache documents.
//!
//! Implementations live in the strategy layer. This module defines only the
//! backend-independent operations exposed through [`crate::core::DB`].

use std::{sync::Arc, time::Duration};

use crate::Result;

use super::Options;

/// Generates an ID for Document or Indexed creation when none is supplied.
///
/// A database shares one generator across both strategies, matching the Go
/// core builder. Implementations must be thread-safe because creates may run
/// concurrently.
pub type NewId = Arc<dyn Fn() -> String + Send + Sync + 'static>;

/// Enumerable cache storage for whole encoded values addressed by ID.
///
/// Document maintains an enumeration index when the backend supplies Sets.
/// Direct create/get/update/delete can still work without Sets, while `keys`
/// and `list` return `Unsupported` rather than emulating an unsafe index.
///
/// **Trade-offs**: Enumeration costs an additional shared index write compared
/// with Volatile. Choose Document when callers need to discover entries by ID.
#[async_trait::async_trait]
pub trait Document: Send + Sync {
    /// Stores a value under an explicit or generated ID and returns that ID.
    ///
    /// **Cost**: One value write plus index-capability writes when available.
    /// **Side effects**: Creates/replaces no existing ID; applies resolved TTL.
    async fn create(&self, value: &[u8], opts: &Options) -> Result<String>;

    /// Copies the encoded value for `id` into `dest`.
    ///
    /// **Cost**: One Driver read. **Side effects**: Replaces `dest` contents.
    async fn get(&self, id: &str, dest: &mut Vec<u8>) -> Result<()>;

    /// Replaces an existing value and resolves a new lease from `opts`.
    ///
    /// **Cost**: One conditional Driver write. Missing IDs return `NotFound`.
    async fn update(&self, id: &str, value: &[u8], opts: &Options) -> Result<()>;

    /// Removes one value and its enumeration membership when supported.
    ///
    /// Missing IDs are harmless; backend failures are propagated.
    async fn delete(&self, id: &str) -> Result<()>;

    /// Returns IDs of all currently live enumerable entries.
    ///
    /// Requires Sets. Expired/missing values may be swept from the index.
    async fn keys(&self) -> Result<Vec<String>>;

    /// Returns encoded values for all currently live enumerable entries.
    ///
    /// Requires Sets and may issue bounded concurrent reads after enumeration.
    async fn list(&self) -> Result<Vec<Vec<u8>>>;

    /// Reports the remaining lease for one entry.
    ///
    /// Zero represents a live permanent entry. Backends without TTL reporting
    /// return `Unsupported`; a missing entry returns `NotFound`.
    async fn ttl(&self, id: &str) -> Result<Duration>;
}
