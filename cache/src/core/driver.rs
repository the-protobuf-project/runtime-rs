//! Low-level storage driver interface
//! 
//! A Driver is the minimal contract a backend must provide.
//! Each method is a single round-trip against one key, with no strategy.

use std::time::Duration;
use crate::Result;

/// ErrMiss indicates a key was not found in storage
/// (Driver's sentinel, different from cache::ErrNotFound)
#[derive(Debug)]
pub struct ErrMiss;

impl std::fmt::Display for ErrMiss {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "driver: miss")
    }
}

impl std::error::Error for ErrMiss {}

/// Driver is the storage abstraction a backend must provide.
/// 
/// Every method is a single round-trip. Anything requiring two keys or 
/// cross-key decisions is a strategy, not a driver method.
/// 
/// All methods must be safe for concurrent use by many tasks.
#[async_trait::async_trait]
pub trait Driver: Send + Sync {
    /// Name identifies the backend: "redis", "memcache", etc.
    fn name(&self) -> &str;
    
    /// Get returns the stored bytes, or Err(ErrMiss)
    async fn get(&self, key: &str) -> Result<Vec<u8>>;
    
    /// Set writes unconditionally. ttl of zero means no expiry.
    async fn set(&self, key: &str, value: &[u8], ttl: Duration) -> Result<()>;
    
    /// Add writes only if the key is absent.
    /// Returns Ok(true) if written, Ok(false) if key already existed.
    async fn add(&self, key: &str, value: &[u8], ttl: Duration) -> Result<bool>;
    
    /// Replace writes only if the key exists.
    /// Returns Ok(true) if written, Ok(false) if key didn't exist.
    async fn replace(&self, key: &str, value: &[u8], ttl: Duration) -> Result<bool>;
    
    /// Delete removes keys. Removing absent keys is not an error.
    async fn delete(&self, keys: &[&str]) -> Result<()>;
    
    /// Exists reports whether a key is live (without fetching its value).
    /// Important for sweeping indexes.
    async fn exists(&self, key: &str) -> Result<bool>;
    
    /// Touch extends a lease without rewriting the value.
    async fn touch(&self, key: &str, ttl: Duration) -> Result<()>;
}
