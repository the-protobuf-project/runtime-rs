// Indexed lookup and group deletion land in the following milestone. Until
// then, this module is exercised through its Document write-path tests.
#![cfg_attr(not(test), allow(dead_code))]

//! Secondary-index filing over the shared Document algorithm.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use crate::{
    CacheError, Result,
    core::{Document, Driver, Keyspace, Options, Sets},
};

use super::DocumentImpl;

/// Document storage that records caller-selected secondary memberships.
///
/// Indexed composes the same ID-based behavior as Document, but uses the
/// isolated `idx` Keyspace segment and maintains two additional structures:
/// field/value membership sets and a per-ID record used to undo those
/// memberships. These extra writes enable lookup and group invalidation at the
/// cost of hot index keys and ordered, non-transactional failure handling.
pub(crate) struct IndexedImpl {
    document: DocumentImpl,
    sets: Option<Arc<dyn Sets>>,
    keyspace: Keyspace,
}

impl IndexedImpl {
    /// Wires Indexed without performing backend I/O.
    pub(crate) fn new(
        driver: Arc<dyn Driver>,
        sets: Option<Arc<dyn Sets>>,
        keyspace: Keyspace,
        default_ttl: Duration,
        require_ttl: bool,
    ) -> Self {
        let document = DocumentImpl::new_indexed(
            driver.clone(),
            sets.clone(),
            keyspace.clone(),
            default_ttl,
            require_ttl,
        );
        Self {
            document,
            sets,
            keyspace,
        }
    }

    fn pair(field: &str, value: &str) -> String {
        format!("{field}={value}")
    }

    fn split_pair(pair: &str) -> (&str, &str) {
        match pair.split_once('=') {
            Some(parts) => parts,
            None => (pair, ""),
        }
    }

    async fn file(
        &self,
        sets: &Arc<dyn Sets>,
        id: &str,
        indexes: &HashMap<String, String>,
    ) -> Result<()> {
        if indexes.is_empty() {
            return Ok(());
        }

        let mut pairs = Vec::with_capacity(indexes.len());
        for (field, value) in indexes {
            let index_key = self.keyspace.idx_by_field(field, value);
            sets.set_add(&index_key, &[id]).await?;
            pairs.push(Self::pair(field, value));
        }

        let pair_refs: Vec<&str> = pairs.iter().map(String::as_str).collect();
        let fields_key = self.keyspace.idx_fields(id);
        sets.set_add(&fields_key, &pair_refs).await
    }

    async fn refile(
        &self,
        sets: &Arc<dyn Sets>,
        id: &str,
        indexes: &HashMap<String, String>,
    ) -> Result<()> {
        let wanted: HashSet<String> = indexes
            .iter()
            .map(|(field, value)| Self::pair(field, value))
            .collect();
        let fields_key = self.keyspace.idx_fields(id);
        let previous = sets.set_members(&fields_key).await?;
        let mut dropped = Vec::new();

        for pair in previous {
            if wanted.contains(&pair) {
                continue;
            }
            let (field, value) = Self::split_pair(&pair);
            let index_key = self.keyspace.idx_by_field(field, value);
            sets.set_remove(&index_key, &[id]).await?;
            dropped.push(pair);
        }

        if !dropped.is_empty() {
            let dropped_refs: Vec<&str> = dropped.iter().map(String::as_str).collect();
            // The real secondary memberships are already gone. Failure to
            // prune this undo metadata is safe: a later delete/refile merely
            // retries an idempotent removal, matching Go's best-effort cleanup.
            let cleanup = sets.set_remove(&fields_key, &dropped_refs).await;
            if cleanup.is_err() {
                // Deliberately suppressed for the idempotent reason above.
            }
        }

        self.file(sets, id, indexes).await
    }

    async fn unfile(&self, sets: &Arc<dyn Sets>, id: &str) -> Result<()> {
        let fields_key = self.keyspace.idx_fields(id);
        let previous = sets.set_members(&fields_key).await?;
        for pair in &previous {
            let (field, value) = Self::split_pair(&pair);
            let index_key = self.keyspace.idx_by_field(field, value);
            sets.set_remove(&index_key, &[id]).await?;
        }
        let previous_refs: Vec<&str> = previous.iter().map(String::as_str).collect();
        sets.set_remove(&fields_key, &previous_refs).await
    }
}

#[async_trait::async_trait]
impl Document for IndexedImpl {
    async fn create(&self, value: &[u8], opts: &Options) -> Result<String> {
        // Required-TTL rejection must happen before secondary index writes.
        self.document.resolve_ttl(opts)?;
        let id = self.document.resolve_id(opts);

        if let Some(indexes) = opts.indexes.as_ref().filter(|indexes| !indexes.is_empty()) {
            let sets = self.sets.as_ref().ok_or(CacheError::Unsupported)?;
            self.file(sets, &id, indexes).await?;
        }

        let mut settled = opts.clone();
        settled.id = Some(id);
        self.document.create(value, &settled).await
    }

    async fn get(&self, id: &str, dest: &mut Vec<u8>) -> Result<()> {
        self.document.get(id, dest).await
    }

    async fn update(&self, id: &str, value: &[u8], opts: &Options) -> Result<()> {
        if let Some(indexes) = opts.indexes.as_ref().filter(|indexes| !indexes.is_empty()) {
            let sets = self.sets.as_ref().ok_or(CacheError::Unsupported)?;
            self.document.update(id, value, opts).await?;
            return self.refile(sets, id, indexes).await;
        }

        self.document.update(id, value, opts).await
    }

    async fn delete(&self, id: &str) -> Result<()> {
        if let Some(sets) = &self.sets {
            self.unfile(sets, id).await?;
        }
        self.document.delete(id).await
    }

    async fn keys(&self) -> Result<Vec<String>> {
        self.document.keys().await
    }

    async fn list(&self) -> Result<Vec<Vec<u8>>> {
        self.document.list().await
    }

    async fn ttl(&self, id: &str) -> Result<Duration> {
        self.document.ttl(id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{MemoryDriver, MemorySets};

    fn indexed_with_sets() -> (IndexedImpl, Arc<MemoryDriver>, Arc<MemorySets>, Keyspace) {
        let driver = Arc::new(MemoryDriver::new());
        let sets = Arc::new(MemorySets::new());
        let keyspace = Keyspace::new("test", "db", 0, false);
        let indexed = IndexedImpl::new(
            driver.clone(),
            Some(sets.clone()),
            keyspace.clone(),
            Duration::from_secs(60),
            false,
        );
        (indexed, driver, sets, keyspace)
    }

    #[tokio::test]
    async fn test_indexed_create_files_members_before_storing_value() {
        let (indexed, driver, sets, keyspace) = indexed_with_sets();
        let options = Options::default()
            .with_id("order-42")
            .with_index("tenant", "acme");

        let id = indexed.create(b"value", &options).await.unwrap();

        assert_eq!(id, "order-42");
        assert_eq!(
            driver.get(&keyspace.idx_entry(&id)).await.unwrap(),
            b"value"
        );
        assert!(driver.get(&keyspace.doc_entry(&id)).await.is_err());
        assert_eq!(
            sets.set_members(&keyspace.idx_by_field("tenant", "acme"))
                .await
                .unwrap(),
            vec![id.clone()]
        );
        assert!(
            sets.set_members(&keyspace.idx_index())
                .await
                .unwrap()
                .contains(&id)
        );
        assert!(
            sets.set_members(&keyspace.idx_fields(&id))
                .await
                .unwrap()
                .contains(&"tenant=acme".to_owned())
        );
    }

    #[tokio::test]
    async fn test_indexed_create_without_sets_rejects_requested_indexes_before_write() {
        let driver = Arc::new(MemoryDriver::new());
        let keyspace = Keyspace::new("test", "db", 0, false);
        let indexed = IndexedImpl::new(
            driver.clone(),
            None,
            keyspace.clone(),
            Duration::from_secs(60),
            false,
        );
        let options = Options::default()
            .with_id("order-42")
            .with_index("tenant", "acme");

        let result = indexed.create(b"value", &options).await;

        assert!(matches!(result, Err(CacheError::Unsupported)));
        assert!(driver.get(&keyspace.idx_entry("order-42")).await.is_err());
    }

    #[tokio::test]
    async fn test_indexed_create_without_indexes_uses_driver_without_sets() {
        let driver = Arc::new(MemoryDriver::new());
        let keyspace = Keyspace::new("test", "db", 0, false);
        let indexed = IndexedImpl::new(driver, None, keyspace, Duration::from_secs(60), false);
        let options = Options::default().with_id("order-42");

        let id = indexed.create(b"value", &options).await.unwrap();
        let mut destination = Vec::new();
        indexed.get(&id, &mut destination).await.unwrap();

        assert_eq!(destination, b"value");
    }

    #[tokio::test]
    async fn test_indexed_create_required_ttl_rejects_before_filing() {
        let driver = Arc::new(MemoryDriver::new());
        let sets = Arc::new(MemorySets::new());
        let keyspace = Keyspace::new("test", "db", 0, false);
        let indexed = IndexedImpl::new(
            driver,
            Some(sets.clone()),
            keyspace.clone(),
            Duration::ZERO,
            true,
        );
        let options = Options::default()
            .with_id("order-42")
            .with_index("tenant", "acme");

        let result = indexed.create(b"value", &options).await;

        assert!(matches!(result, Err(CacheError::NoTTL)));
        assert!(
            sets.set_members(&keyspace.idx_by_field("tenant", "acme"))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            sets.set_members(&keyspace.idx_fields("order-42"))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_indexed_update_refiles_and_removes_old_memberships() {
        let (indexed, _, sets, keyspace) = indexed_with_sets();
        let create = Options::default()
            .with_id("order-42")
            .with_index("tenant", "old")
            .with_index("status", "active");
        indexed.create(b"old", &create).await.unwrap();
        let update = Options::default().with_index("tenant", "new");

        indexed.update("order-42", b"new", &update).await.unwrap();
        let mut destination = Vec::new();
        indexed.get("order-42", &mut destination).await.unwrap();

        assert_eq!(destination, b"new");
        assert!(
            sets.set_members(&keyspace.idx_by_field("tenant", "old"))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            sets.set_members(&keyspace.idx_by_field("status", "active"))
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            sets.set_members(&keyspace.idx_by_field("tenant", "new"))
                .await
                .unwrap(),
            vec!["order-42".to_owned()]
        );
        assert_eq!(
            sets.set_members(&keyspace.idx_fields("order-42"))
                .await
                .unwrap(),
            vec!["tenant=new".to_owned()]
        );
    }

    #[tokio::test]
    async fn test_indexed_update_without_indexes_preserves_memberships() {
        let (indexed, _, sets, keyspace) = indexed_with_sets();
        let create = Options::default()
            .with_id("order-42")
            .with_index("tenant", "acme");
        indexed.create(b"old", &create).await.unwrap();

        indexed
            .update("order-42", b"new", &Options::default())
            .await
            .unwrap();

        assert_eq!(
            sets.set_members(&keyspace.idx_by_field("tenant", "acme"))
                .await
                .unwrap(),
            vec!["order-42".to_owned()]
        );
    }

    #[tokio::test]
    async fn test_indexed_update_without_sets_rejects_before_value_change() {
        let driver = Arc::new(MemoryDriver::new());
        let keyspace = Keyspace::new("test", "db", 0, false);
        let indexed = IndexedImpl::new(driver, None, keyspace, Duration::from_secs(60), false);
        let create = Options::default().with_id("order-42");
        indexed.create(b"old", &create).await.unwrap();
        let update = Options::default().with_index("tenant", "acme");

        let result = indexed.update("order-42", b"new", &update).await;
        let mut destination = Vec::new();
        indexed.get("order-42", &mut destination).await.unwrap();

        assert!(matches!(result, Err(CacheError::Unsupported)));
        assert_eq!(destination, b"old");
    }

    #[tokio::test]
    async fn test_indexed_delete_unfiles_all_memberships_and_value() {
        let (indexed, driver, sets, keyspace) = indexed_with_sets();
        let options = Options::default()
            .with_id("order-42")
            .with_index("tenant", "acme")
            .with_index("status", "active");
        indexed.create(b"value", &options).await.unwrap();

        indexed.delete("order-42").await.unwrap();

        assert!(driver.get(&keyspace.idx_entry("order-42")).await.is_err());
        assert!(
            sets.set_members(&keyspace.idx_fields("order-42"))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            sets.set_members(&keyspace.idx_index())
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            sets.set_members(&keyspace.idx_by_field("tenant", "acme"))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            sets.set_members(&keyspace.idx_by_field("status", "active"))
                .await
                .unwrap()
                .is_empty()
        );
    }
}
