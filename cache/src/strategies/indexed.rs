//! Secondary-index filing over the shared Document algorithm.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use futures::{StreamExt, TryStreamExt, stream};

use crate::{
    CacheError, Result,
    core::{Document, Driver, Indexed, Keyspace, Options, Sets},
};

use super::DocumentImpl;

/// Document storage that records caller-selected secondary memberships.
///
/// Indexed composes the same ID-based behavior as Document, but uses the
/// isolated `idx` Keyspace segment and maintains two additional structures:
/// field/value membership sets and a per-ID record used to undo those
/// memberships. These extra writes enable lookup and group invalidation at the
/// cost of hot index keys and ordered, non-transactional failure handling.
pub struct IndexedImpl {
    document: DocumentImpl,
    driver: Arc<dyn Driver>,
    sets: Option<Arc<dyn Sets>>,
    keyspace: Keyspace,
    concurrency: usize,
}

impl IndexedImpl {
    /// Wires Indexed without performing backend I/O.
    #[allow(dead_code)]
    pub(crate) fn new(
        driver: Arc<dyn Driver>,
        sets: Option<Arc<dyn Sets>>,
        keyspace: Keyspace,
        default_ttl: Duration,
        require_ttl: bool,
        concurrency: usize,
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
            driver,
            sets,
            keyspace,
            concurrency: concurrency.max(1),
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

    /// Returns live IDs and best-effort sweeps expired members.
    async fn ids_by_index(&self, field: &str, value: &str) -> Result<Vec<String>> {
        let sets = self.sets.as_ref().ok_or(CacheError::Unsupported)?;
        let index_key = self.keyspace.idx_by_field(field, value);
        let members = sets.set_members(&index_key).await?;
        if members.is_empty() {
            return Ok(Vec::new());
        }

        let driver = self.driver.clone();
        let keyspace = self.keyspace.clone();
        let checked: Vec<(String, bool)> = stream::iter(members.into_iter().map(move |id| {
            let driver = driver.clone();
            let entry_key = keyspace.idx_entry(&id);
            async move {
                let exists = driver.exists(&entry_key).await?;
                Ok::<_, CacheError>((id, exists))
            }
        }))
        .buffered(self.concurrency)
        .try_collect()
        .await?;

        let mut live = Vec::with_capacity(checked.len());
        let mut stale = Vec::new();
        for (id, exists) in checked {
            if exists {
                live.push(id);
            } else {
                stale.push(id);
            }
        }

        if !stale.is_empty() {
            let stale_refs: Vec<&str> = stale.iter().map(String::as_str).collect();
            // A failed sweep must not turn a successful lookup into an error.
            // The stale members name no live values and the next read retries.
            let cleanup = sets.set_remove(&index_key, &stale_refs).await;
            if cleanup.is_err() {
                // Deliberately suppressed for the retryable reason above.
            }
        }

        Ok(live)
    }

    /// Returns values for live secondary members, tolerating expiry races.
    async fn by_index(&self, field: &str, value: &str) -> Result<Vec<Vec<u8>>> {
        let ids = self.ids_by_index(field, value).await?;
        let driver = self.driver.clone();
        let keyspace = self.keyspace.clone();
        let bodies: Vec<Option<Vec<u8>>> = stream::iter(ids.into_iter().map(move |id| {
            let driver = driver.clone();
            let entry_key = keyspace.idx_entry(&id);
            async move {
                match driver.get(&entry_key).await {
                    Ok(body) => Ok(Some(body)),
                    Err(CacheError::NotFound) => Ok(None),
                    Err(error) => Err(error),
                }
            }
        }))
        .buffered(self.concurrency)
        .try_collect()
        .await?;

        Ok(bodies.into_iter().flatten().collect())
    }

    /// Removes one live secondary group and every membership naming its IDs.
    async fn delete_by_index(&self, field: &str, value: &str) -> Result<usize> {
        let ids = self.ids_by_index(field, value).await?;
        if ids.is_empty() {
            return Ok(0);
        }
        let sets = self.sets.as_ref().ok_or(CacheError::Unsupported)?;

        let record_sets = sets.clone();
        let record_keyspace = self.keyspace.clone();
        let records: Vec<(String, Vec<String>)> =
            stream::iter(ids.iter().cloned().map(move |id| {
                let sets = record_sets.clone();
                let fields_key = record_keyspace.idx_fields(&id);
                async move {
                    let fields = sets.set_members(&fields_key).await?;
                    Ok::<_, CacheError>((id, fields))
                }
            }))
            .buffered(self.concurrency)
            .try_collect()
            .await?;

        // Group IDs by secondary key so each shared index is updated once even
        // when many deleted entries carry the same field/value membership.
        let mut removals: HashMap<String, Vec<String>> = HashMap::new();
        for (id, fields) in &records {
            for pair in fields {
                let (member_field, member_value) = Self::split_pair(pair);
                let index_key = self.keyspace.idx_by_field(member_field, member_value);
                removals.entry(index_key).or_default().push(id.clone());
            }
        }

        for (index_key, members) in removals {
            let member_refs: Vec<&str> = members.iter().map(String::as_str).collect();
            sets.set_remove(&index_key, &member_refs).await?;
        }

        let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        sets.set_remove(&self.keyspace.idx_index(), &id_refs)
            .await?;

        // Rust capabilities may be separate handles, as MemoryDriver and
        // MemorySets are. Clear set-backed field records through Sets instead
        // of assuming Driver deletion reaches the same concrete object.
        let cleanup_sets = sets.clone();
        let cleanup_keyspace = self.keyspace.clone();
        let cleared: Vec<()> = stream::iter(records.into_iter().map(move |(id, fields)| {
            let sets = cleanup_sets.clone();
            let fields_key = cleanup_keyspace.idx_fields(&id);
            async move {
                let field_refs: Vec<&str> = fields.iter().map(String::as_str).collect();
                sets.set_remove(&fields_key, &field_refs).await
            }
        }))
        .buffered(self.concurrency)
        .try_collect()
        .await?;
        drop(cleared);

        let entry_keys: Vec<String> = ids.iter().map(|id| self.keyspace.idx_entry(id)).collect();
        let entry_refs: Vec<&str> = entry_keys.iter().map(String::as_str).collect();
        self.driver.delete(&entry_refs).await?;

        Ok(ids.len())
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

#[async_trait::async_trait]
impl Indexed for IndexedImpl {
    async fn by_index(&self, field: &str, value: &str) -> Result<Vec<Vec<u8>>> {
        IndexedImpl::by_index(self, field, value).await
    }

    async fn ids_by_index(&self, field: &str, value: &str) -> Result<Vec<String>> {
        IndexedImpl::ids_by_index(self, field, value).await
    }

    async fn delete_by_index(&self, field: &str, value: &str) -> Result<usize> {
        IndexedImpl::delete_by_index(self, field, value).await
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
            16,
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
            16,
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
        let indexed = IndexedImpl::new(driver, None, keyspace, Duration::from_secs(60), false, 16);
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
            16,
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
        let indexed = IndexedImpl::new(driver, None, keyspace, Duration::from_secs(60), false, 16);
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
    async fn test_indexed_ids_by_index_returns_only_matching_live_ids() {
        let (indexed, _, _, _) = indexed_with_sets();
        for (id, tenant) in [("one", "acme"), ("two", "other"), ("three", "acme")] {
            let options = Options::default().with_id(id).with_index("tenant", tenant);
            indexed.create(id.as_bytes(), &options).await.unwrap();
        }

        let ids: HashSet<String> = indexed
            .ids_by_index("tenant", "acme")
            .await
            .unwrap()
            .into_iter()
            .collect();

        assert_eq!(ids, HashSet::from(["one".to_owned(), "three".to_owned()]));
    }

    #[tokio::test]
    async fn test_indexed_ids_by_index_sweeps_expired_member() {
        let (indexed, _, sets, keyspace) = indexed_with_sets();
        let options = Options::default()
            .with_id("expired")
            .with_ttl(Duration::from_millis(10))
            .with_index("tenant", "acme");
        indexed.create(b"value", &options).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let ids = indexed.ids_by_index("tenant", "acme").await.unwrap();

        assert!(ids.is_empty());
        assert!(
            sets.set_members(&keyspace.idx_by_field("tenant", "acme"))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_indexed_by_index_returns_matching_values() {
        let (indexed, _, _, _) = indexed_with_sets();
        for (id, body) in [("one", b"first".as_slice()), ("two", b"second".as_slice())] {
            let options = Options::default().with_id(id).with_index("tenant", "acme");
            indexed.create(body, &options).await.unwrap();
        }

        let mut values = indexed.by_index("tenant", "acme").await.unwrap();
        values.sort();

        assert_eq!(values, vec![b"first".to_vec(), b"second".to_vec()]);
    }

    #[tokio::test]
    async fn test_indexed_by_index_without_match_returns_empty_values() {
        let (indexed, _, _, _) = indexed_with_sets();

        let values = indexed.by_index("tenant", "missing").await.unwrap();

        assert!(values.is_empty());
    }

    #[tokio::test]
    async fn test_indexed_lookup_without_sets_returns_unsupported() {
        let indexed = IndexedImpl::new(
            Arc::new(MemoryDriver::new()),
            None,
            Keyspace::new("test", "db", 0, false),
            Duration::from_secs(60),
            false,
            16,
        );

        assert!(matches!(
            indexed.ids_by_index("tenant", "acme").await,
            Err(CacheError::Unsupported)
        ));
        assert!(matches!(
            indexed.by_index("tenant", "acme").await,
            Err(CacheError::Unsupported)
        ));
    }

    #[tokio::test]
    async fn test_indexed_delete_by_index_removes_group_and_preserves_others() {
        let (indexed, _, sets, keyspace) = indexed_with_sets();
        for (id, tenant) in [("one", "acme"), ("two", "acme"), ("three", "other")] {
            let options = Options::default()
                .with_id(id)
                .with_index("tenant", tenant)
                .with_index("status", "active");
            indexed.create(id.as_bytes(), &options).await.unwrap();
        }

        let deleted = indexed.delete_by_index("tenant", "acme").await.unwrap();

        assert_eq!(deleted, 2);
        assert!(indexed.get("one", &mut Vec::new()).await.is_err());
        assert!(indexed.get("two", &mut Vec::new()).await.is_err());
        let mut remaining = Vec::new();
        indexed.get("three", &mut remaining).await.unwrap();
        assert_eq!(remaining, b"three");
        assert!(
            sets.set_members(&keyspace.idx_by_field("tenant", "acme"))
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            sets.set_members(&keyspace.idx_by_field("status", "active"))
                .await
                .unwrap(),
            vec!["three".to_owned()]
        );
        assert_eq!(
            sets.set_members(&keyspace.idx_index()).await.unwrap(),
            vec!["three".to_owned()]
        );
        assert!(
            sets.set_members(&keyspace.idx_fields("one"))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            sets.set_members(&keyspace.idx_fields("two"))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_indexed_delete_by_index_without_match_returns_zero() {
        let (indexed, _, _, _) = indexed_with_sets();
        let options = Options::default()
            .with_id("one")
            .with_index("tenant", "other");
        indexed.create(b"value", &options).await.unwrap();

        let deleted = indexed.delete_by_index("tenant", "missing").await.unwrap();
        let mut destination = Vec::new();
        indexed.get("one", &mut destination).await.unwrap();

        assert_eq!(deleted, 0);
        assert_eq!(destination, b"value");
    }

    #[tokio::test]
    async fn test_indexed_delete_by_index_without_sets_returns_unsupported() {
        let indexed = IndexedImpl::new(
            Arc::new(MemoryDriver::new()),
            None,
            Keyspace::new("test", "db", 0, false),
            Duration::from_secs(60),
            false,
            16,
        );

        let result = indexed.delete_by_index("tenant", "acme").await;

        assert!(matches!(result, Err(CacheError::Unsupported)));
    }

    #[tokio::test]
    async fn test_indexed_public_trait_supports_lookup_and_group_deletion() {
        let (indexed, _, _, _) = indexed_with_sets();
        let options = Options::default()
            .with_id("one")
            .with_index("tenant", "acme");
        indexed.create(b"value", &options).await.unwrap();
        let contract: &dyn Indexed = &indexed;

        assert_eq!(
            contract.ids_by_index("tenant", "acme").await.unwrap(),
            vec!["one".to_owned()]
        );
        assert_eq!(contract.by_index("tenant", "acme").await.unwrap(), vec![
            b"value".to_vec()
        ]);
        assert_eq!(contract.delete_by_index("tenant", "acme").await.unwrap(), 1);
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
