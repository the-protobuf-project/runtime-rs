//! In-memory driver for testing and learning
//! 
//! This is the simplest Driver implementation - all data in a HashMap with TTL.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use crate::Result;
use super::Driver;

/// Entry stores the value and expiry time
struct Entry {
    value: Vec<u8>,
    expires_at: Option<Instant>,
}

impl Entry {
    /// Check if this entry has expired
    fn is_expired(&self) -> bool {
        match self.expires_at {
            Some(expiry) => Instant::now() >= expiry,
            None => false, // No expiry
        }
    }
}

/// MemoryDriver stores all data in memory using a HashMap
/// Suitable for testing, learning, and single-process caches
pub struct MemoryDriver {
    data: Arc<RwLock<HashMap<String, Entry>>>,
    name: String,
}

impl MemoryDriver {
    /// Create a new in-memory driver
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
        data.insert(
            key.to_string(),
            Entry {
                value: value.to_vec(),
                expires_at,
            },
        );
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
        if let Some(entry) = data.get(key) {
            if !entry.is_expired() {
                return Ok(false); // Key exists, so Add fails
            }
        }
        
        data.insert(
            key.to_string(),
            Entry {
                value: value.to_vec(),
                expires_at,
            },
        );
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
        
        data.insert(
            key.to_string(),
            Entry {
                value: value.to_vec(),
                expires_at,
            },
        );
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
