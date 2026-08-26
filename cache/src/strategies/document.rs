//! Enumerable ID-addressed storage over Driver and optional Sets.
//!
//! Stores whole values and, when the backend supports Sets, maintains an index
//! so they can be listed. Direct entry operations remain available without
//! Sets, while enumeration reports `Unsupported`.
//!
//! Trade-offs:
//! - With Sets, every Create/Delete also touches the index.
//! - Reading `keys` walks the index in O(entries).
//! - The shared enumeration set is one hot key and is not sharded.

use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

use crate::{
    CacheError, Result,
    core::{Document, Driver, Keyspace, Leases, NewId, Options, Sets},
};

/// Stores whole encoded values, with optional enumeration through server Sets.
///
/// Direct create, get, update, and delete operations need only the Driver. When
/// Sets is present, writes also maintain an enumeration index; without it,
/// `keys` and `list` explicitly return [`CacheError::Unsupported`]. This matches
/// the Go strategy and keeps a backend such as Memcached useful without
/// pretending it can enumerate values.
///
/// The enumeration index is a shared hot key. The current Rust implementation
/// does not sweep expired members during `keys` or `list`, so it can retain
/// stale IDs. Prefer Volatile when enumeration is not needed.
pub struct DocumentImpl {
    /// Stores and conditionally replaces encoded entry values.
    driver: Arc<dyn Driver>,
    /// Optional enumeration index capability.
    ///
    /// Direct entry operations need only Driver. Enumeration refuses with
    /// `Unsupported` when this handle is absent.
    sets: Option<Arc<dyn Sets>>,
    /// Optional remaining-expiry reporting capability.
    leases: Option<Arc<dyn Leases>>,
    /// Selected database's centralized key builder.
    keyspace: Keyspace,
    /// Chooses Document or Indexed key families for the shared algorithm.
    layout: DocumentLayout,
    /// Lease used when operation options do not select one.
    default_ttl: Duration,
    /// Rejects writes that resolve to implicit permanence.
    require_ttl: bool,
    /// Database-wide ID source shared with Indexed.
    new_id: NewId,
}

/// Key-family selection for the shared ID-addressed implementation.
///
/// Indexed delegates its base value operations to DocumentImpl but must never
/// collide with ordinary Document entries or enumeration membership.
#[derive(Clone, Copy)]
enum DocumentLayout {
    /// Uses `doc:*` entry and enumeration keys.
    Document,
    /// Uses `idx:*` entry and enumeration keys.
    Indexed,
}

impl DocumentImpl {
    /// Wires a Document strategy to its driver and optional Sets capability.
    ///
    /// Direct construction leaves TTL reporting unsupported. Prefer DB/provider
    /// construction when the backend declares the optional Leases capability.
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
            None,
            keyspace,
            default_ttl,
            require_ttl,
            default_new_id(),
        )
    }

    /// Wires a Document with database capabilities and its shared ID generator.
    ///
    /// **Cost**: Local reference ownership only. **Side effects**: None.
    pub(crate) fn new_with_id(
        driver: Arc<dyn Driver>,
        sets: Option<Arc<dyn Sets>>,
        leases: Option<Arc<dyn Leases>>,
        keyspace: Keyspace,
        default_ttl: Duration,
        require_ttl: bool,
        new_id: NewId,
    ) -> Self {
        Self::with_layout(
            driver,
            sets,
            leases,
            keyspace,
            default_ttl,
            require_ttl,
            new_id,
            DocumentLayout::Document,
        )
    }

    /// Builds the Document half of Indexed with the isolated `idx` key layout.
    ///
    /// This is the only path that selects `DocumentLayout::Indexed`, ensuring
    /// all delegated operations use Indexed keys consistently.
    pub(crate) fn new_indexed(
        driver: Arc<dyn Driver>,
        sets: Option<Arc<dyn Sets>>,
        leases: Option<Arc<dyn Leases>>,
        keyspace: Keyspace,
        default_ttl: Duration,
        require_ttl: bool,
        new_id: NewId,
    ) -> Self {
        Self::with_layout(
            driver,
            sets,
            leases,
            keyspace,
            default_ttl,
            require_ttl,
            new_id,
            DocumentLayout::Indexed,
        )
    }

    /// Central constructor that binds the algorithm to exactly one key family.
    fn with_layout(
        driver: Arc<dyn Driver>,
        sets: Option<Arc<dyn Sets>>,
        leases: Option<Arc<dyn Leases>>,
        keyspace: Keyspace,
        default_ttl: Duration,
        require_ttl: bool,
        new_id: NewId,
        layout: DocumentLayout,
    ) -> Self {
        Self {
            driver,
            sets,
            leases,
            keyspace,
            layout,
            default_ttl,
            require_ttl,
            new_id,
        }
    }

    /// Resolves a write lease using the cache-wide TTL priority contract.
    ///
    /// Explicit TTL wins, followed by explicit permanence, configured default,
    /// required-TTL rejection, and finally implicit permanence. This is local
    /// validation and performs no Driver I/O.
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

    /// Qualifies an ID through the key family selected at construction.
    fn entry_key(&self, id: &str) -> String {
        match self.layout {
            DocumentLayout::Document => self.keyspace.doc_entry(id),
            DocumentLayout::Indexed => self.keyspace.idx_entry(id),
        }
    }

    /// Returns the enumeration-set key for the selected key family.
    fn index_key(&self) -> String {
        match self.layout {
            DocumentLayout::Document => self.keyspace.doc_index(),
            DocumentLayout::Indexed => self.keyspace.idx_index(),
        }
    }
}

/// Returns the UUID-v4 Rust default as a thread-safe shared ID source.
///
/// DB construction calls this once and gives the same handle to Document and
/// Indexed. Callers needing Go's sortable ULID representation can install a
/// custom `DatabaseSpec::new_id` generator.
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

        // Store only after publishing the sweepable enumeration member.
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

        // Driver Replace keeps Update from creating an unindexed new entry.
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
                // Listing is a non-transactional snapshot. The current
                // algorithm suppresses any per-entry read failure after the
                // index read; it does not sweep that member here.
                Err(_) => {}
            }
        }
        Ok(results)
    }

    async fn ttl(&self, id: &str) -> Result<Duration> {
        let leases = self.leases.as_ref().ok_or(CacheError::Unsupported)?;
        leases.ttl(&self.entry_key(id)).await
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
