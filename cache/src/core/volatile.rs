//! Public contract for direct, non-enumerated cache entries.
//!
//! Volatile avoids shared indexes: callers must already know each key. The
//! strategy layer owns Keyspace qualification and TTL resolution.

use std::time::Duration;

use crate::Result;

use super::Options;

/// TTL-oriented key/value cache with no enumeration index.
///
/// **Trade-offs**: Each operation touches only its entry, avoiding Document's
/// shared index and scaling well for independent keys. Without an index, the
/// cache cannot portably enumerate entries; backend scanning is optional.
///
/// **Best for**: Sessions, ephemeral tokens, counters encoded by callers, and
/// values always retrieved by a known key.
#[async_trait::async_trait]
pub trait Volatile: Send + Sync {
    /// Writes a value unconditionally using the resolved operation lease.
    ///
    /// **Cost**: One Driver write. **Side effects**: Replaces value and lease.
    async fn set(&self, key: &str, value: &[u8], opts: &Options) -> Result<()>;

    /// Copies one encoded value into `dest` in one Driver read.
    ///
    /// Missing/expired keys return `NotFound`; `dest` is replaced on success.
    async fn get(&self, key: &str, dest: &mut Vec<u8>) -> Result<()>;

    /// Removes one entry in one Driver delete; missing keys are harmless.
    async fn delete(&self, key: &str) -> Result<()>;

    /// Replaces a live entry's lease without transferring its value.
    ///
    /// **Cost**: One Driver round trip. Zero makes the entry permanent.
    async fn touch(&self, key: &str, ttl: Duration) -> Result<()>;

    /// Reports remaining lease; zero means a live permanent entry.
    ///
    /// Missing entries return `NotFound`; unsupported backends return
    /// `Unsupported`.
    async fn ttl(&self, key: &str) -> Result<Duration>;

    /// Returns qualified backend keys matching a backend glob pattern.
    ///
    /// Requires Scanner and may walk the complete backend keyspace. Prefer
    /// known-key access for ordinary application operations.
    async fn scan(&self, pattern: &str) -> Result<Vec<String>>;
}
