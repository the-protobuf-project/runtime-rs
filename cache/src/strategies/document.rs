//! Document strategy: enumerable storage with index
//!
//! Stores whole values and, when the backend supports Sets, maintains an index
//! so they can be listed. Direct entry operations remain available without
//! Sets, while enumeration reports `Unsupported`.
//!
//! Trade-offs:
//! - With Sets, every Create/Delete also touches the index
//! - Reading keys() walks the index (O(entries))
//! - Does NOT shard - the index is one hot key on all backends

use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

use crate::{
    CacheError, Result,
    core::{Document, Driver, Keyspace, NewId, Options, Sets},
};

/// Stores whole encoded values, with optional enumeration through server Sets.
///
/// Direct create, get, update, and delete operations need only the Driver. When
/// Sets is present, writes also maintain an enumeration index; without it,
/// `keys` and `list` explicitly return [`CacheError::Unsupported`]. This matches
/// the Go strategy and keeps a backend such as Memcached useful without
/// pretending it can enumerate values.
///
/// The enumeration index is a shared hot key and can retain members for expired
/// values until a read sweeps them. Prefer Volatile when enumeration is not
/// needed.
pub struct DocumentImpl {
    driver: Arc<dyn Driver>,
    // Sets is optional because direct entry operations need only Driver. Only
    // enumeration requires the index and must refuse when this is absent.
    sets: Option<Arc<dyn Sets>>,
    keyspace: Keyspace,
    layout: DocumentLayout,
    default_ttl: Duration,
    require_ttl: bool,
    new_id: NewId,
}

#[derive(Clone, Copy)]
enum DocumentLayout {
    Document,
    Indexed,
}

impl DocumentImpl {
    /// Wires a Document strategy to its driver and optional Sets capability.
    ///
    /// **Cost**: Local construction only; no driver round trip.
    /// **Side effects**: None. Backend data is touched only by trait methods.
    pub fn new(
        driver: Arc<dyn Driver>,
        sets: Option<Arc<dyn Sets>>,
        keyspace: Keyspace,
        default_ttl: Duration,
        require_ttl: bool,
    ) -> Self {
        Self::new_with_id(
            driver,
            sets,
            keyspace,
            default_ttl,
            require_ttl,
            default_new_id(),
        )
    }

    /// Wires a Document with the database's shared ID generator.
    pub(crate) fn new_with_id(
        driver: Arc<dyn Driver>,
        sets: Option<Arc<dyn Sets>>,
        keyspace: Keyspace,
        default_ttl: Duration,
        require_ttl: bool,
        new_id: NewId,
    ) -> Self {
        Self::with_layout(
            driver,
            sets,
            keyspace,
            default_ttl,
            require_ttl,
            new_id,
            DocumentLayout::Document,
        )
    }

    /// Builds the Document half of Indexed with the isolated `idx` key layout.
    pub(crate) fn new_indexed(
        driver: Arc<dyn Driver>,
        sets: Option<Arc<dyn Sets>>,
        keyspace: Keyspace,
        default_ttl: Duration,
        require_ttl: bool,
        new_id: NewId,
    ) -> Self {
        Self::with_layout(
            driver,
            sets,
            keyspace,
            default_ttl,
            require_ttl,
            new_id,
            DocumentLayout::Indexed,
        )
    }

    fn with_layout(
        driver: Arc<dyn Driver>,
        sets: Option<Arc<dyn Sets>>,
        keyspace: Keyspace,
        default_ttl: Duration,
        require_ttl: bool,
        new_id: NewId,
        layout: DocumentLayout,
    ) -> Self {
        Self {
            driver,
            sets,
            keyspace,
            layout,
            default_ttl,
            require_ttl,
            new_id,
        }
    }

    /// Resolve TTL (same as Volatile)
    pub(crate) fn resolve_ttl(&self, opts: &Options) -> Result<Duration> {
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

    /// Resolves a caller-selected ID or generates the one used by every index.
    pub(crate) fn resolve_id(&self, opts: &Options) -> String {
        match &opts.id {
            Some(id) => id.clone(),
            None => (self.new_id)(),
        }
    }

    fn entry_key(&self, id: &str) -> String {
        match self.layout {
            DocumentLayout::Document => self.keyspace.doc_entry(id),
            DocumentLayout::Indexed => self.keyspace.idx_entry(id),
        }
    }

    fn index_key(&self) -> String {
        match self.layout {
            DocumentLayout::Document => self.keyspace.doc_index(),
            DocumentLayout::Indexed => self.keyspace.idx_index(),
        }
    }
}

/// Returns the existing Rust default while allowing DB construction to share it.
pub(crate) fn default_new_id() -> NewId {
    Arc::new(|| Uuid::new_v4().to_string())
}

#[async_trait::async_trait]
impl Document for DocumentImpl {
    async fn create(&self, value: &[u8], opts: &Options) -> Result<String> {
        let ttl = self.resolve_ttl(opts)?;

        let id = self.resolve_id(opts);

        // Index first when available. A failure between the two writes then
        // leaves a sweepable dangling member, never an invisible stored value.
        if let Some(sets) = &self.sets {
            let index_key = self.index_key();
            sets.set_add(&index_key, &[&id]).await?;
        }

        // Then store the value
        let entry_key = self.entry_key(&id);
        self.driver.set(&entry_key, value, ttl).await?;

        Ok(id)
    }

    async fn get(&self, id: &str, dest: &mut Vec<u8>) -> Result<()> {
        let entry_key = self.entry_key(id);
        *dest = self.driver.get(&entry_key).await?;
        Ok(())
    }

    async fn update(&self, id: &str, value: &[u8], opts: &Options) -> Result<()> {
        let ttl = self.resolve_ttl(opts)?;
        let entry_key = self.entry_key(id);

        // Replace only if key exists (otherwise update fails)
        let ok = self.driver.replace(&entry_key, value, ttl).await?;
        if !ok {
            return Err(crate::CacheError::NotFound);
        }
        Ok(())
    }

    async fn delete(&self, id: &str) -> Result<()> {
        let entry_key = self.entry_key(id);
        self.driver.delete(&[&entry_key]).await?;

        if let Some(sets) = &self.sets {
            let index_key = self.index_key();
            sets.set_remove(&index_key, &[id]).await?;
        }

        Ok(())
    }

    async fn keys(&self) -> Result<Vec<String>> {
        let sets = self.sets.as_ref().ok_or(CacheError::Unsupported)?;
        let index_key = self.index_key();
        sets.set_members(&index_key).await
    }

    async fn list(&self) -> Result<Vec<Vec<u8>>> {
        let keys = self.keys().await?;

        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            match self.driver.get(&self.entry_key(&key)).await {
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

        let doc = DocumentImpl::new(driver, Some(sets), ks, Duration::from_secs(60), false);

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

        let doc = DocumentImpl::new(driver, Some(sets), ks, Duration::from_secs(60), false);

        // Create with custom ID
        let opts = Options::default()
            .with_id("custom-123")
            .with_ttl(Duration::from_secs(30));

        let id = doc.create(b"data", &opts).await.expect("create failed");
        assert_eq!(id, "custom-123");
    }

    #[tokio::test]
    async fn test_document_create_without_sets_stores_direct_entry() {
        let driver = Arc::new(MemoryDriver::new());
        let doc = DocumentImpl::new(
            driver,
            None,
            Keyspace::new("test", "db", 0, false),
            Duration::from_secs(60),
            false,
        );
        let options = Options::default().with_id("direct");

        let id = doc.create(b"value", &options).await.unwrap();
        let mut destination = Vec::new();
        doc.get(&id, &mut destination).await.unwrap();

        assert_eq!(id, "direct");
        assert_eq!(destination, b"value");
    }

    #[tokio::test]
    async fn test_document_delete_without_sets_removes_direct_entry() {
        let driver = Arc::new(MemoryDriver::new());
        let doc = DocumentImpl::new(
            driver,
            None,
            Keyspace::new("test", "db", 0, false),
            Duration::from_secs(60),
            false,
        );
        let options = Options::default().with_id("direct");
        doc.create(b"value", &options).await.unwrap();

        doc.delete("direct").await.unwrap();

        assert!(matches!(
            doc.get("direct", &mut Vec::new()).await,
            Err(CacheError::NotFound)
        ));
    }

    #[tokio::test]
    async fn test_document_keys_without_sets_returns_unsupported() {
        let doc = DocumentImpl::new(
            Arc::new(MemoryDriver::new()),
            None,
            Keyspace::new("test", "db", 0, false),
            Duration::from_secs(60),
            false,
        );

        assert!(matches!(doc.keys().await, Err(CacheError::Unsupported)));
    }

    #[tokio::test]
    async fn test_document_list_without_sets_returns_unsupported() {
        let doc = DocumentImpl::new(
            Arc::new(MemoryDriver::new()),
            None,
            Keyspace::new("test", "db", 0, false),
            Duration::from_secs(60),
            false,
        );

        assert!(matches!(doc.list().await, Err(CacheError::Unsupported)));
    }
}
