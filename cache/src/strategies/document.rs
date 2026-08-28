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

use super::batch::{DEFAULT_CONCURRENCY, get_all, live_members};

/// Stores whole encoded values, with optional enumeration through server Sets.
///
/// Direct create, get, update, and delete operations need only the Driver. When
/// Sets is present, writes also maintain an enumeration index; without it,
/// `keys` and `list` explicitly return [`CacheError::Unsupported`]. This matches
/// the Go strategy and keeps a backend such as Memcached useful without
/// pretending it can enumerate values.
///
/// The enumeration index is a shared hot key. Reads sweep expired members with
/// bounded Driver fan-out, but the operation remains O(entries). Prefer
/// Volatile when enumeration is not needed.
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
    /// Bound for parallel per-entry liveness and value reads; always >= 1.
    concurrency: usize,
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
            DEFAULT_CONCURRENCY,
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
        concurrency: usize,
        new_id: NewId,
    ) -> Self {
        Self::with_layout(
            driver,
            sets,
            leases,
            keyspace,
            default_ttl,
            require_ttl,
            concurrency,
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
        concurrency: usize,
        new_id: NewId,
    ) -> Self {
        Self::with_layout(
            driver,
            sets,
            leases,
            keyspace,
            default_ttl,
            require_ttl,
            concurrency,
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
        concurrency: usize,
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
            concurrency: concurrency.max(1),
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
        live_members(
            self.driver.clone(),
            sets,
            self.concurrency,
            &index_key,
            |id| self.entry_key(id),
        )
        .await
    }

    async fn list(&self) -> Result<Vec<Vec<u8>>> {
        let ids = self.keys().await?;
        let keys = ids.iter().map(|id| self.entry_key(id)).collect();
        get_all(self.driver.clone(), self.concurrency, keys).await
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

    #[derive(Clone, Copy)]
    enum GetOverride {
        NotFound,
        Failure,
    }

    struct ReadOverrideDriver {
        inner: MemoryDriver,
        key: String,
        get_override: GetOverride,
    }

    #[async_trait::async_trait]
    impl Driver for ReadOverrideDriver {
        fn name(&self) -> &str {
            "read-override"
        }

        async fn get(&self, key: &str) -> Result<Vec<u8>> {
            if key == self.key {
                return match self.get_override {
                    GetOverride::NotFound => Err(CacheError::NotFound),
                    GetOverride::Failure => {
                        Err(CacheError::Internal("injected read failure".to_owned()))
                    }
                };
            }
            self.inner.get(key).await
        }

        async fn set(&self, key: &str, value: &[u8], ttl: Duration) -> Result<()> {
            self.inner.set(key, value, ttl).await
        }

        async fn add(&self, key: &str, value: &[u8], ttl: Duration) -> Result<bool> {
            self.inner.add(key, value, ttl).await
        }

        async fn replace(&self, key: &str, value: &[u8], ttl: Duration) -> Result<bool> {
            self.inner.replace(key, value, ttl).await
        }

        async fn delete(&self, keys: &[&str]) -> Result<()> {
            self.inner.delete(keys).await
        }

        async fn exists(&self, key: &str) -> Result<bool> {
            if key == self.key {
                return Ok(true);
            }
            self.inner.exists(key).await
        }

        async fn touch(&self, key: &str, ttl: Duration) -> Result<()> {
            self.inner.touch(key, ttl).await
        }
    }

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

    #[tokio::test]
    async fn test_document_keys_sweeps_expired_member() {
        let driver = Arc::new(MemoryDriver::new());
        let sets = Arc::new(MemorySets::new());
        let keyspace = Keyspace::new("test", "db", 0, false);
        let doc = DocumentImpl::new(
            driver,
            Some(sets.clone()),
            keyspace.clone(),
            Duration::ZERO,
            false,
        );
        doc.create(
            b"short-lived",
            &Options::default()
                .with_id("expired")
                .with_ttl(Duration::from_millis(1)),
        )
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;

        let keys = doc.keys().await.unwrap();

        assert!(keys.is_empty());
        assert!(
            sets.set_members(&keyspace.doc_index())
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_document_list_skips_not_found_expiry_race() {
        let keyspace = Keyspace::new("test", "db", 0, false);
        let sets = Arc::new(MemorySets::new());
        sets.set_add(&keyspace.doc_index(), &["raced"])
            .await
            .unwrap();
        let driver = Arc::new(ReadOverrideDriver {
            inner: MemoryDriver::new(),
            key: keyspace.doc_entry("raced"),
            get_override: GetOverride::NotFound,
        });
        let doc = DocumentImpl::new(driver, Some(sets), keyspace, Duration::ZERO, false);

        assert!(doc.list().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_document_list_propagates_non_miss_driver_failure() {
        let keyspace = Keyspace::new("test", "db", 0, false);
        let sets = Arc::new(MemorySets::new());
        sets.set_add(&keyspace.doc_index(), &["broken"])
            .await
            .unwrap();
        let driver = Arc::new(ReadOverrideDriver {
            inner: MemoryDriver::new(),
            key: keyspace.doc_entry("broken"),
            get_override: GetOverride::Failure,
        });
        let doc = DocumentImpl::new(driver, Some(sets), keyspace, Duration::ZERO, false);

        let result = doc.list().await;

        assert!(
            matches!(result, Err(CacheError::Internal(message)) if message == "injected read failure")
        );
    }
}
