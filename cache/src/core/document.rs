use std::{sync::Arc, time::Duration};

use crate::Result;

use super::Options;

/// Generates an ID for Document or Indexed creation when none is supplied.
///
/// A database shares one generator across both strategies, matching the Go
/// core builder. Implementations must be thread-safe because creates may run
/// concurrently.
pub type NewId = Arc<dyn Fn() -> String + Send + Sync + 'static>;

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
