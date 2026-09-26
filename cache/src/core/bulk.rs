//! Optional ordered multi-key reads sharing backend round trips.

use crate::Result;

/// Backend quality capability for reading many keys efficiently.
///
/// Bulk does not unlock behavior: strategies retain bounded per-key fallbacks
/// when it is absent. Implementations must return exactly one ordered result per
/// input key. A missing value is `None`, while backend failures remain errors.
///
/// **Trade-offs**: Batching reduces network round trips and syscalls but is not
/// transactional; keys can still change while a batch is processed.
#[async_trait::async_trait]
pub trait Bulk: Send + Sync {
    /// Returns ordered values, using `None` for missing or expired keys.
    ///
    /// **Cost**: One backend bulk/pipeline round trip. **Side effects**: None.
    async fn get_many(&self, keys: &[String]) -> Result<Vec<Option<Vec<u8>>>>;

    /// Returns ordered liveness flags for the supplied keys.
    ///
    /// **Cost**: One backend bulk/pipeline round trip. **Side effects**: None.
    async fn exists_many(&self, keys: &[String]) -> Result<Vec<bool>>;
}
