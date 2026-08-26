//! Direct TTL-oriented key/value strategy with no enumeration index.
//!
//! Each caller key maps through Keyspace to one Driver key. There is no shared
//! metadata hot key, so unrelated entries can scale with backend shards.

use crate::{
    Result,
    core::{Driver, Keyspace, Leases, Options, Volatile},
};
use std::sync::Arc;
use std::time::Duration;

/// Thin key qualification and TTL-policy layer over one Driver.
///
/// Volatile performs no enumeration bookkeeping and is the lowest-write-cost
/// strategy. It is appropriate when callers already know keys. TTL reporting
/// is available when DB construction supplies the optional Leases capability;
/// scanning remains unsupported.
pub struct VolatileImpl {
    /// Required single-key storage primitives.
    driver: Arc<dyn Driver>,
    /// Optional remaining-expiry reporting capability.
    leases: Option<Arc<dyn Leases>>,
    /// Qualifies caller keys into the Volatile strategy segment.
    keyspace: Keyspace,
    /// Lease used when operation options do not select one.
    default_ttl: Duration,
    /// Rejects writes that resolve to implicit permanence.
    require_ttl: bool,
}

impl VolatileImpl {
    /// Wires a Volatile strategy without optional capabilities or backend I/O.
    ///
    /// Direct construction leaves TTL reporting unsupported. Prefer DB/provider
    /// construction when the backend declares the optional Leases capability.
    ///
    /// **Cost**: Local ownership moves only. **Side effects**: None.
    pub fn new(
        driver: Arc<dyn Driver>,
        keyspace: Keyspace,
        default_ttl: Duration,
        require_ttl: bool,
    ) -> Self {
        Self::new_with_leases(driver, None, keyspace, default_ttl, require_ttl)
    }

    /// Wires the optional remaining-expiry capability resolved by DB creation.
    pub(crate) fn new_with_leases(
        driver: Arc<dyn Driver>,
        leases: Option<Arc<dyn Leases>>,
        keyspace: Keyspace,
        default_ttl: Duration,
        require_ttl: bool,
    ) -> Self {
        Self {
            driver,
            leases,
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

    async fn ttl(&self, key: &str) -> Result<Duration> {
        let leases = self.leases.as_ref().ok_or(crate::CacheError::Unsupported)?;
        leases.ttl(&self.keyspace.vol_entry(key)).await
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
