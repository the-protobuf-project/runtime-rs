//! Minimal, backend-neutral storage primitives.
//!
//! Drivers translate these operations into backend commands. Cross-key policy,
//! key qualification, TTL defaults, serialization, and indexing belong to the
//! strategy layer.

use crate::Result;
use std::time::Duration;

/// Legacy marker for a low-level storage miss.
///
/// Current [`Driver`] implementations report misses as
/// [`crate::CacheError::NotFound`]. This public marker remains for source
/// compatibility and should not be returned by new drivers.
#[derive(Debug)]
pub struct ErrMiss;

impl std::fmt::Display for ErrMiss {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "driver: miss")
    }
}

impl std::error::Error for ErrMiss {}

/// Atomic, single-command storage operations required by cache strategies.
///
/// Each asynchronous method represents one logical backend round trip and must
/// be safe for concurrent calls. Optional functionality such as Sets, scanning,
/// bulk access, and TTL inspection is exposed through separate capability
/// traits, keeping limited backends useful without silent emulation.
#[async_trait::async_trait]
pub trait Driver: Send + Sync {
    /// Returns the stable backend identity used for diagnostics.
    ///
    /// This is a local lookup with no I/O or side effects.
    fn name(&self) -> &str;

    /// Returns the stored bytes, or `NotFound` for a missing/expired key.
    ///
    /// **Cost**: One read round trip. **Side effects**: None.
    async fn get(&self, key: &str) -> Result<Vec<u8>>;

    /// Writes unconditionally, replacing both value and lease.
    ///
    /// A zero TTL means no expiry. **Cost**: One write round trip.
    async fn set(&self, key: &str, value: &[u8], ttl: Duration) -> Result<()>;

    /// Writes only when no live value exists.
    ///
    /// Returns `true` when written and `false` on a conditional conflict. A
    /// zero TTL means no expiry. **Cost**: One conditional-write round trip.
    async fn add(&self, key: &str, value: &[u8], ttl: Duration) -> Result<bool>;

    /// Replaces only an existing live value and its lease.
    ///
    /// Returns `true` when written and `false` when absent. A zero TTL means no
    /// expiry. **Cost**: One conditional-write round trip.
    async fn replace(&self, key: &str, value: &[u8], ttl: Duration) -> Result<bool>;

    /// Removes all supplied keys; absent keys are harmless.
    ///
    /// **Cost**: One batched delete round trip. Empty input is permitted.
    async fn delete(&self, keys: &[&str]) -> Result<()>;

    /// Reports whether a live key exists without returning its value.
    ///
    /// **Cost**: One read round trip. Strategies use this to sweep indexes.
    async fn exists(&self, key: &str) -> Result<bool>;

    /// Replaces the lease without transferring or rewriting the value.
    ///
    /// A zero TTL makes a live entry permanent. Missing/expired entries return
    /// `NotFound`. **Cost**: One write round trip.
    async fn touch(&self, key: &str, ttl: Duration) -> Result<()>;
}
