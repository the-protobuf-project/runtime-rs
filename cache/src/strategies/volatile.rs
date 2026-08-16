//! Volatile strategy: TTL-first storage with no index
//!
//! This is the simplest strategy - just Set/Get/Delete without tracking.
//! No enumeration, no hot keys, scales with cluster shards.

use crate::{
    Result,
    core::{Driver, Keyspace, Options, Volatile},
};
use std::time::Duration;

/// VolatileImpl is ephemeral key-value storage with TTL
pub struct VolatileImpl {
    driver: std::sync::Arc<dyn Driver>,
    keyspace: Keyspace,
    default_ttl: Duration,
    require_ttl: bool,
}

impl VolatileImpl {
    /// Create a new Volatile strategy
    pub fn new(
        driver: std::sync::Arc<dyn Driver>,
        keyspace: Keyspace,
        default_ttl: Duration,
        require_ttl: bool,
    ) -> Self {
        Self {
            driver,
            keyspace,
            default_ttl,
            require_ttl,
        }
    }

    /// Resolve TTL: use provided, fallback to default, or error if require_ttl
    fn resolve_ttl(&self, opts: &Options) -> Result<Duration> {
        if let Some(ttl) = opts.ttl {
            return Ok(ttl);
        }

        if opts.permanent {
            return Ok(Duration::ZERO); // No expiry
        }

        if !self.default_ttl.is_zero() {
            return Ok(self.default_ttl);
        }

        if self.require_ttl {
            return Err(crate::CacheError::NoTTL);
        }

        Ok(Duration::ZERO) // No expiry by default
    }
}

#[async_trait::async_trait]
impl Volatile for VolatileImpl {
    async fn set(&self, key: &str, value: &[u8], opts: &Options) -> Result<()> {
        let ttl = self.resolve_ttl(opts)?;
        let full_key = self.keyspace.vol_entry(key);
        self.driver.set(&full_key, value, ttl).await
    }

    async fn get(&self, key: &str, dest: &mut Vec<u8>) -> Result<()> {
        let full_key = self.keyspace.vol_entry(key);
        *dest = self.driver.get(&full_key).await?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let full_key = self.keyspace.vol_entry(key);
        self.driver.delete(&[&full_key]).await
    }

    async fn touch(&self, key: &str, ttl: Duration) -> Result<()> {
        let full_key = self.keyspace.vol_entry(key);
        self.driver.touch(&full_key, ttl).await
    }

    async fn ttl(&self, _key: &str) -> Result<Duration> {
        // Most drivers don't report TTL, so this returns Unsupported
        // A Redis driver could implement it with PTTL
        Err(crate::CacheError::Unsupported)
    }

    async fn scan(&self, _pattern: &str) -> Result<Vec<String>> {
        // Scan is best-effort, backends may refuse it
        Err(crate::CacheError::Unsupported)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::MemoryDriver;

    #[tokio::test]
    async fn test_volatile_set_get() {
        let driver = std::sync::Arc::new(MemoryDriver::new());
        let ks = Keyspace::new("test", "db", 0, false);
        let vol = VolatileImpl::new(driver, ks, Duration::from_secs(60), false);

        // Set a value
        let opts = Options::default().with_ttl(Duration::from_secs(30));
        vol.set("key1", b"hello", &opts).await.expect("set failed");

        // Get it back
        let mut dest = Vec::new();
        vol.get("key1", &mut dest).await.expect("get failed");
        assert_eq!(dest, b"hello");

        // Delete it
        vol.delete("key1").await.expect("delete failed");

        // Should be gone
        let mut dest = Vec::new();
        assert!(vol.get("key1", &mut dest).await.is_err());
    }
}
