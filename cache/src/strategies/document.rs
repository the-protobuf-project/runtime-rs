//! Document strategy: enumerable storage with index
//!
//! Stores whole values and maintains an index so they can be listed.
//! Used for things like a catalog of orders, products, etc.
//!
//! Trade-offs:
//! - Every Create/Delete touches the index (2 writes)
//! - Reading keys() walks the index (O(entries))
//! - Does NOT shard - the index is one hot key on all backends

use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

use crate::{
    Result,
    core::{Document, Driver, Keyspace, Options, Sets},
};

/// DocumentImpl stores whole encoded values with enumeration
pub struct DocumentImpl {
    driver: Arc<dyn Driver>,
    sets: Arc<dyn Sets>,
    keyspace: Keyspace,
    default_ttl: Duration,
    require_ttl: bool,
}

impl DocumentImpl {
    /// Create a new Document strategy
    pub fn new(
        driver: Arc<dyn Driver>,
        sets: Arc<dyn Sets>,
        keyspace: Keyspace,
        default_ttl: Duration,
        require_ttl: bool,
    ) -> Self {
        Self {
            driver,
            sets,
            keyspace,
            default_ttl,
            require_ttl,
        }
    }

    /// Resolve TTL (same as Volatile)
    fn resolve_ttl(&self, opts: &Options) -> Result<Duration> {
        if let Some(ttl) = opts.ttl {
            return Ok(ttl);
        }

        if opts.permanent {
            return Ok(Duration::ZERO);
        }

        if !self.default_ttl.is_zero() {
            return Ok(self.default_ttl);
        }

        if self.require_ttl {
            return Err(crate::CacheError::NoTTL);
        }

        Ok(Duration::ZERO)
    }

    /// Generate a new ID
    fn new_id(&self) -> String {
        Uuid::new_v4().to_string()
    }
}

#[async_trait::async_trait]
impl Document for DocumentImpl {
    async fn create(&self, value: &[u8], opts: &Options) -> Result<String> {
        let ttl = self.resolve_ttl(opts)?;

        // Use provided ID or generate one
        let id = if let Some(ref custom_id) = opts.id {
            custom_id.clone()
        } else {
            self.new_id()
        };

        // Index first - if the index write fails, we don't store the value
        let index_key = self.keyspace.doc_index();
        self.sets.set_add(&index_key, &[&id]).await?;

        // Then store the value
        let entry_key = self.keyspace.doc_entry(&id);
        self.driver.set(&entry_key, value, ttl).await?;

        Ok(id)
    }

    async fn get(&self, id: &str, dest: &mut Vec<u8>) -> Result<()> {
        let entry_key = self.keyspace.doc_entry(id);
        *dest = self.driver.get(&entry_key).await?;
        Ok(())
    }

    async fn update(&self, id: &str, value: &[u8], opts: &Options) -> Result<()> {
        let ttl = self.resolve_ttl(opts)?;
        let entry_key = self.keyspace.doc_entry(id);

        // Replace only if key exists (otherwise update fails)
        let ok = self.driver.replace(&entry_key, value, ttl).await?;
        if !ok {
            return Err(crate::CacheError::NotFound);
        }
        Ok(())
    }

    async fn delete(&self, id: &str) -> Result<()> {
        let entry_key = self.keyspace.doc_entry(id);
        self.driver.delete(&[&entry_key]).await?;

        // Remove from index
        let index_key = self.keyspace.doc_index();
        self.sets.set_remove(&index_key, &[id]).await?;

        Ok(())
    }

    async fn keys(&self) -> Result<Vec<String>> {
        let index_key = self.keyspace.doc_index();
        self.sets.set_members(&index_key).await
    }

    async fn list(&self) -> Result<Vec<Vec<u8>>> {
        let keys = self.keys().await?;

        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            match self.driver.get(&self.keyspace.doc_entry(&key)).await {
                Ok(value) => results.push(value),
                Err(_) => {
                    // Entry expired or disappeared, skip it
                    // In real code, might want to clean up the index
                }
            }
        }
        Ok(results)
    }

    async fn ttl(&self, _id: &str) -> Result<Duration> {
        // Most drivers don't report TTL
        Err(crate::CacheError::Unsupported)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{MemoryDriver, MemorySets};

    #[tokio::test]
    async fn test_document_create_and_get() {
        let driver = Arc::new(MemoryDriver::new());
        let sets = Arc::new(MemorySets::new());
        let ks = Keyspace::new("test", "db", 0, false);

        let doc = DocumentImpl::new(driver, sets, ks, Duration::from_secs(60), false);

        // Create an entry
        let opts = Options::default().with_ttl(Duration::from_secs(30));
        let id = doc
            .create(b"entry-data", &opts)
            .await
            .expect("create failed");

        assert!(!id.is_empty());

        // Get it back
        let mut dest = Vec::new();
        doc.get(&id, &mut dest).await.expect("get failed");
        assert_eq!(dest, b"entry-data");

        // Check it's in keys
        let keys = doc.keys().await.expect("keys failed");
        assert!(keys.contains(&id));
    }

    #[tokio::test]
    async fn test_document_custom_id() {
        let driver = Arc::new(MemoryDriver::new());
        let sets = Arc::new(MemorySets::new());
        let ks = Keyspace::new("test", "db", 0, false);

        let doc = DocumentImpl::new(driver, sets, ks, Duration::from_secs(60), false);

        // Create with custom ID
        let opts = Options::default()
            .with_id("custom-123")
            .with_ttl(Duration::from_secs(30));

        let id = doc.create(b"data", &opts).await.expect("create failed");
        assert_eq!(id, "custom-123");
    }
}
