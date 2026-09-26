//! Direct TTL-oriented key/value strategy with no enumeration index.
//!
//! Each caller key maps through Keyspace to one Driver key. There is no shared
//! metadata hot key, so unrelated entries can scale with backend shards.

use crate::{
    CacheError, Result,
    core::{Driver, Keyspace, Leases, Options, Scanner, Volatile},
};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

/// Thin key qualification and TTL-policy layer over one Driver.
///
/// Volatile performs no enumeration bookkeeping and is the lowest-write-cost
/// strategy. It is appropriate when callers already know keys. TTL reporting
/// is available when DB construction supplies the optional Leases capability;
/// scanning is available when DB construction supplies Scanner.
pub struct VolatileImpl {
    /// Required single-key storage primitives.
    driver: Arc<dyn Driver>,
    /// Optional remaining-expiry reporting capability.
    leases: Option<Arc<dyn Leases>>,
    /// Optional cursor-based keyspace scanning capability.
    scanner: Option<Arc<dyn Scanner>>,
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
        Self::new_with_capabilities(driver, None, None, keyspace, default_ttl, require_ttl)
    }

    /// Wires optional backend capabilities resolved by DB creation.
    ///
    /// **Cost**: Local reference ownership only. **Side effects**: None.
    pub(crate) fn new_with_capabilities(
        driver: Arc<dyn Driver>,
        leases: Option<Arc<dyn Leases>>,
        scanner: Option<Arc<dyn Scanner>>,
        keyspace: Keyspace,
        default_ttl: Duration,
        require_ttl: bool,
    ) -> Self {
        Self {
            driver,
            leases,
            scanner,
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

    async fn scan(&self, pattern: &str) -> Result<Vec<String>> {
        let scanner = self.scanner.as_ref().ok_or(CacheError::Unsupported)?;
        let prefix = self.keyspace.vol_prefix();
        let scanned = scanner.scan(&self.keyspace.vol_pattern(pattern)).await?;
        if scanned.iter().any(|key| !key.starts_with(&prefix)) {
            return Err(CacheError::Internal(
                "scanner returned a key outside the Volatile keyspace".to_owned(),
            ));
        }

        let mut seen = HashSet::with_capacity(scanned.len());
        Ok(scanned
            .into_iter()
            .filter(|key| seen.insert(key.clone()))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::MemoryDriver;
    use tokio::sync::Mutex;

    struct FixedScanner {
        keys: Vec<String>,
        patterns: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl Scanner for FixedScanner {
        async fn scan(&self, pattern: &str) -> Result<Vec<String>> {
            self.patterns.lock().await.push(pattern.to_owned());
            Ok(self.keys.clone())
        }
    }

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

    #[tokio::test]
    async fn test_volatile_scan_without_scanner_returns_unsupported() {
        let vol = VolatileImpl::new(
            Arc::new(MemoryDriver::new()),
            Keyspace::new("app", "orders", 0, false),
            Duration::ZERO,
            false,
        );

        assert!(matches!(
            vol.scan("session:*").await,
            Err(CacheError::Unsupported)
        ));
    }

    #[tokio::test]
    async fn test_volatile_scan_qualifies_pattern_and_deduplicates_results() {
        let scanner = Arc::new(FixedScanner {
            keys: vec![
                "app:orders:cache:vol:session:one".to_owned(),
                "app:orders:cache:vol:session:one".to_owned(),
                "app:orders:cache:vol:session:two".to_owned(),
            ],
            patterns: Mutex::new(Vec::new()),
        });
        let vol = VolatileImpl::new_with_capabilities(
            Arc::new(MemoryDriver::new()),
            None,
            Some(scanner.clone()),
            Keyspace::new("app", "orders", 0, false),
            Duration::ZERO,
            false,
        );

        let keys = vol.scan("session:*").await.unwrap();

        assert_eq!(
            keys,
            [
                "app:orders:cache:vol:session:one",
                "app:orders:cache:vol:session:two"
            ]
        );
        assert_eq!(
            scanner.patterns.lock().await.as_slice(),
            ["app:orders:cache:vol:session:*"]
        );
    }

    #[tokio::test]
    async fn test_volatile_scan_rejects_key_outside_strategy_prefix() {
        let scanner = Arc::new(FixedScanner {
            keys: vec!["app:orders:cache:doc:entry:one".to_owned()],
            patterns: Mutex::new(Vec::new()),
        });
        let vol = VolatileImpl::new_with_capabilities(
            Arc::new(MemoryDriver::new()),
            None,
            Some(scanner),
            Keyspace::new("app", "orders", 0, false),
            Duration::ZERO,
            false,
        );

        let result = vol.scan("*").await;

        assert!(
            matches!(result, Err(CacheError::Internal(message)) if message.contains("outside the Volatile keyspace"))
        );
    }
}
