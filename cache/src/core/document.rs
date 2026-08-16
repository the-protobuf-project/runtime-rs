use std::time::Duration;

use crate::Result;

use super::Options;

/// Document is ephemeral storage for whole values, enumerable
#[async_trait::async_trait]
pub trait Document: Send + Sync {
    /// Create stores value under a generated id
    async fn create(&self, value: &[u8], opts: &Options) -> Result<String>;
    
    /// Get decodes the entry into dest
    async fn get(&self, id: &str, dest: &mut Vec<u8>) -> Result<()>;
    
    /// Update replaces the value stored under id
    async fn update(&self, id: &str, value: &[u8], opts: &Options) -> Result<()>;
    
    /// Delete removes an entry
    async fn delete(&self, id: &str) -> Result<()>;
    
    /// Keys returns the ids of every live entry
    async fn keys(&self) -> Result<Vec<String>>;
    
    /// List decodes every live entry
    async fn list(&self) -> Result<Vec<Vec<u8>>>;
    
    /// TTL reports how much longer an entry will live
    async fn ttl(&self, id: &str) -> Result<Duration>;
}
