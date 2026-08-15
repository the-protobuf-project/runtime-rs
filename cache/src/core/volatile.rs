use std::time::Duration;

use crate::Result;

use super::Options;

/// Volatile is TTL-first storage with no index
#[async_trait::async_trait]
pub trait Volatile: Send + Sync {
    /// Set writes value under key with a lease
    async fn set(&self, key: &str, value: &[u8], opts: &Options) -> Result<()>;

    /// Get decodes the entry into dest
    async fn get(&self, key: &str, dest: &mut Vec<u8>) -> Result<()>;

    /// Delete removes an entry
    async fn delete(&self, key: &str) -> Result<()>;

    /// Touch extends an entry's lease without rewriting its value
    async fn touch(&self, key: &str, ttl: Duration) -> Result<()>;

    /// TTL reports how much longer an entry will live
    async fn ttl(&self, key: &str) -> Result<Duration>;

    /// Scan returns the keys matching a glob pattern
    async fn scan(&self, pattern: &str) -> Result<Vec<String>>;
}
