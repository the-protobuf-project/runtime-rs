//! Direct TTL-oriented key/value strategy with no enumeration index.
//!
//! Each caller key maps through Keyspace to one Driver key. There is no shared
//! metadata hot key, so unrelated entries can scale with backend shards.

use crate::{
    Result,
    core::{Driver, Keyspace, Options, Volatile},
};
use std::time::Duration;

/// Thin key qualification and TTL-policy layer over one Driver.
///
/// Volatile performs no enumeration bookkeeping and is the lowest-write-cost
/// strategy. It is appropriate when callers already know keys. TTL reporting
/// and scanning remain unsupported until their optional capabilities are wired
/// into this implementation.
pub struct VolatileImpl {
    /// Required single-key storage primitives.
    driver: std::sync::Arc<dyn Driver>,
    /// Qualifies caller keys into the Volatile strategy segment.
    keyspace: Keyspace,
    /// Lease used when operation options do not select one.
    default_ttl: Duration,
    /// Rejects writes that resolve to implicit permanence.
    require_ttl: bool,
}

impl VolatileImpl {
    /// Wires a Volatile strategy without performing backend I/O.
    ///
    /// **Cost**: Local ownership moves only. **Side effects**: None.
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

    /// Resolves a write lease using the cache-wide priority contract.
    ///
    /// The order is explicit TTL, explicit permanence, configured default,
    /// required-TTL rejection, then implicit permanence. This performs no I/O.
    fn resolve_ttl(&self, opts: &Options) -> Result<Duration> {
        if let Some(ttl) = opts.ttl {
            return Ok(ttl);
        }

        if opts.permanent {
            return Ok(Duration::ZERO); // Explicit permanence.
        }

        if !self.default_ttl.is_zero() {
            return Ok(self.default_ttl);
        }

        if self.require_ttl {
            return Err(crate::CacheError::NoTTL);
        }

        Ok(Duration::ZERO) // Implicit permanence when allowed.
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
        // Lease reporting is not part of Driver and no capability is wired yet.
        Err(crate::CacheError::Unsupported)
    }

    async fn scan(&self, _pattern: &str) -> Result<Vec<String>> {
        // Scanner is a DB/provider capability and is not wired into Volatile.
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
