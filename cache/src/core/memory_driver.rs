//! Process-local Driver implementation for tests and examples.
//!
//! Values live in a lock-protected map and expire lazily when accessed. This
//! preserves Driver semantics without external services, but provides neither
//! persistence nor coordination across processes.

use super::{Driver, Leases};
use crate::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Stored bytes paired with an optional monotonic expiry deadline.
struct Entry {
    /// Owned payload returned by `get`.
    value: Vec<u8>,
    /// `None` denotes a permanent entry.
    expires_at: Option<Instant>,
}

impl Entry {
    /// Compares the deadline with the current monotonic clock.
    fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(expiry) => Instant::now() >= expiry,
            None => false,
        }
    }
}

/// Concurrent, process-local key/value implementation of [`Driver`].
///
/// **Trade-offs**: Operations are fast and deterministic, but all tasks share
/// one map lock and expired entries are retained until overwritten/deleted.
/// **Best for**: Unit tests, examples, and small single-process caches.
pub struct MemoryDriver {
    /// Shared storage; individual clones are performed while holding a read lock.
    data: Arc<RwLock<HashMap<String, Entry>>>,
    /// Stable diagnostic backend name.
    name: String,
}

impl MemoryDriver {
    /// Creates an empty driver with backend name `memory`.
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            name: "memory".to_string(),
        }
    }
}

impl Default for MemoryDriver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Driver for MemoryDriver {
    fn name(&self) -> &str {
        &self.name
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        let data = self.data.read().await;

        match data.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    Err(crate::CacheError::NotFound)
                } else {
                    Ok(entry.value.clone())
                }
            }
            None => Err(crate::CacheError::NotFound),
        }
    }

    async fn set(&self, key: &str, value: &[u8], ttl: Duration) -> Result<()> {
        let expires_at = if ttl.is_zero() {
            None
        } else {
            Some(Instant::now() + ttl)
        };

        let mut data = self.data.write().await;
        data.insert(key.to_string(), Entry {
            value: value.to_vec(),
            expires_at,
        });
        Ok(())
    }

    async fn add(&self, key: &str, value: &[u8], ttl: Duration) -> Result<bool> {
        let expires_at = if ttl.is_zero() {
            None
        } else {
            Some(Instant::now() + ttl)
        };

        let mut data = self.data.write().await;

        // Check if key exists and is not expired
        if let Some(entry) = data.get(key)
            && !entry.is_expired()
        {
            return Ok(false); // Key exists, so Add fails
        }

        data.insert(key.to_string(), Entry {
            value: value.to_vec(),
            expires_at,
        });
        Ok(true)
    }

    async fn replace(&self, key: &str, value: &[u8], ttl: Duration) -> Result<bool> {
        let expires_at = if ttl.is_zero() {
            None
        } else {
            Some(Instant::now() + ttl)
        };

        let mut data = self.data.write().await;

        // Check if key exists and is not expired
        if let Some(entry) = data.get(key) {
            if entry.is_expired() {
                return Ok(false); // Entry expired, so Replace fails
            }
        } else {
            return Ok(false); // Key doesn't exist, so Replace fails
        }

        data.insert(key.to_string(), Entry {
            value: value.to_vec(),
            expires_at,
        });
        Ok(true)
    }

    async fn delete(&self, keys: &[&str]) -> Result<()> {
        let mut data = self.data.write().await;
        for key in keys {
            data.remove(*key);
        }
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let data = self.data.read().await;

        match data.get(key) {
            Some(entry) => {
                if entry.is_expired() {
                    Ok(false)
                } else {
                    Ok(true)
                }
            }
            None => Ok(false),
        }
    }

    async fn touch(&self, key: &str, ttl: Duration) -> Result<()> {
        let mut data = self.data.write().await;

        if let Some(entry) = data.get_mut(key) {
            if entry.is_expired() {
                return Err(crate::CacheError::NotFound);
            }
            entry.expires_at = if ttl.is_zero() {
                None
            } else {
                Some(Instant::now() + ttl)
            };
            Ok(())
        } else {
            Err(crate::CacheError::NotFound)
        }
    }
}

#[async_trait::async_trait]
impl Leases for MemoryDriver {
    /// Reports a live entry's lease from the process monotonic clock.
    ///
    /// Permanent entries return zero. Missing and expired entries return
    /// `NotFound`; checking the lease does not eagerly remove expired storage.
    async fn ttl(&self, key: &str) -> Result<Duration> {
        let data = self.data.read().await;
        let entry = data.get(key).ok_or(crate::CacheError::NotFound)?;
        let Some(expires_at) = entry.expires_at else {
            return Ok(Duration::ZERO);
        };
        let now = Instant::now();
        if expires_at <= now {
            return Err(crate::CacheError::NotFound);
        }
        Ok(expires_at.duration_since(now))
    }
}

#[cfg(test)]
mod lease_tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_driver_ttl_reports_positive_permanent_and_missing() {
        let driver = MemoryDriver::new();
        driver
            .set("leased", b"value", Duration::from_secs(1))
            .await
            .unwrap();
        driver
            .set("permanent", b"value", Duration::ZERO)
            .await
            .unwrap();

        let remaining = Leases::ttl(&driver, "leased").await.unwrap();

        assert!(remaining > Duration::ZERO);
        assert!(remaining <= Duration::from_secs(1));
        assert_eq!(
            Leases::ttl(&driver, "permanent").await.unwrap(),
            Duration::ZERO
        );
        assert!(matches!(
            Leases::ttl(&driver, "missing").await,
            Err(crate::CacheError::NotFound)
        ));
    }

    #[tokio::test]
    async fn test_memory_driver_ttl_expired_returns_not_found() {
        let driver = MemoryDriver::new();
        driver
            .set("expired", b"value", Duration::from_millis(1))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;

        assert!(matches!(
            Leases::ttl(&driver, "expired").await,
            Err(crate::CacheError::NotFound)
        ));
    }
}
