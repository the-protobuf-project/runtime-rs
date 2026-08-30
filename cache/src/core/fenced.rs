//! Conditional deletion for safely releasing leased ownership keys.

use crate::Result;

/// Deletes a key only while it still contains an expected byte value.
///
/// This capability exists to release leased distributed locks safely. A plain
/// GET followed by DEL is invalid because the lease may expire between those
/// commands and another owner may acquire the same key before DEL runs.
///
/// **Trade-offs**: Adds one compare-and-delete round trip to lock release, but
/// prevents a late owner from deleting its successor's lock.
/// **Scalability**: Operations are independent per key and require no global
/// coordination beyond the backend's atomic primitive.
/// **Use when**: A strategy owns a leased key identified by an unpredictable
/// token and must release only that exact ownership generation.
#[async_trait::async_trait]
pub trait Fenced: Send + Sync {
    /// Deletes `key` atomically only when its current bytes equal `expected`.
    ///
    /// Returns `true` only when this call deleted the matching live key.
    /// Missing, expired, or mismatched keys return `false` without mutation.
    ///
    /// **Cost**: Exactly one backend round trip or atomic critical section.
    /// **Side effects**: May delete the matching key; never changes a mismatch.
    /// **Concurrency**: Comparison and deletion must be indivisible.
    /// **Use when**: Releasing a lease whose ownership may have changed.
    async fn delete_if(&self, key: &str, expected: &[u8]) -> Result<bool>;
}
