//! Optional reporting of a live entry's remaining expiry lease.

use std::time::Duration;

use crate::Result;

/// Backend capability for reading how long a key will remain live.
///
/// Expiry support alone does not imply this capability: Memcached accepts
/// expiry on writes but cannot report the remaining duration. Strategies must
/// therefore return [`crate::CacheError::Unsupported`] when Leases is absent
/// rather than interpreting an invented value.
///
/// A zero result represents a live permanent key. Missing or expired keys
/// return [`crate::CacheError::NotFound`].
#[async_trait::async_trait]
pub trait Leases: Send + Sync {
    /// Returns the remaining lease for one backend-qualified key.
    ///
    /// **Cost**: One backend read round trip. **Side effects**: None.
    /// **When to use**: Renewal decisions and operational inspection, not
    /// ordinary reads where fetching the value already establishes liveness.
    async fn ttl(&self, key: &str) -> Result<Duration>;
}
