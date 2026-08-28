//! Shared bounded fallbacks for strategy operations spanning many entries.
//!
//! These helpers preserve result order, use Bulk in bounded batches when
//! available, and cap concurrent Driver calls otherwise. SetScanner processes
//! large membership sets page by page without changing public behavior.

use std::{collections::HashSet, sync::Arc};

use futures::{StreamExt, TryStreamExt, stream};

use crate::{
    CacheError, Result,
    core::{Bulk, Driver, SetScanVisitor, SetScanner, Sets},
};

/// Fan-out used when database configuration does not select one.
pub(crate) const DEFAULT_CONCURRENCY: usize = 16;
/// Maximum keys passed through one Bulk round trip, matching Go core.
pub(crate) const BULK_BATCH_SIZE: usize = 256;

/// Returns live set members and best-effort removes stale ones.
///
/// Membership order is preserved. Existence failures abort the read, while a
/// stale-member cleanup failure is suppressed because the returned live view is
/// still correct and a later read can retry cleanup.
///
/// **Cost**: One Sets read or cursor walk, liveness reads through Bulk/fallback,
/// and at most one best-effort cleanup write per cursor page.
pub(crate) async fn live_members<F>(
    driver: Arc<dyn Driver>,
    bulk: Option<&dyn Bulk>,
    sets: &Arc<dyn Sets>,
    set_scanner: Option<&dyn SetScanner>,
    concurrency: usize,
    set_key: &str,
    entry_key: F,
) -> Result<Vec<String>>
where
    F: Fn(&str) -> String + Send + Sync,
{
    if let Some(set_scanner) = set_scanner {
        let mut visitor = LiveVisitor {
            driver,
            bulk,
            sets,
            concurrency,
            set_key,
            entry_key,
            seen: HashSet::new(),
            live: Vec::new(),
        };
        set_scanner.set_scan(set_key, &mut visitor).await?;
        return Ok(visitor.live);
    }

    let members = sets.set_members(set_key).await?;
    if members.is_empty() {
        return Ok(Vec::new());
    }

    let (live, stale) = classify_members(driver, bulk, concurrency, members, &entry_key).await?;
    cleanup_stale(sets, set_key, &stale).await;
    Ok(live)
}

/// Per-page SetScanner consumer retaining only live, first-seen IDs.
struct LiveVisitor<'a, F> {
    driver: Arc<dyn Driver>,
    bulk: Option<&'a dyn Bulk>,
    sets: &'a Arc<dyn Sets>,
    concurrency: usize,
    set_key: &'a str,
    entry_key: F,
    seen: HashSet<String>,
    live: Vec<String>,
}

#[async_trait::async_trait]
impl<F> SetScanVisitor for LiveVisitor<'_, F>
where
    F: Fn(&str) -> String + Send + Sync,
{
    async fn visit(&mut self, members: Vec<String>) -> Result<()> {
        if members.is_empty() {
            return Ok(());
        }
        let (live, stale) = classify_members(
            self.driver.clone(),
            self.bulk,
            self.concurrency,
            members,
            &self.entry_key,
        )
        .await?;
        cleanup_stale(self.sets, self.set_key, &stale).await;
        for id in live {
            if self.seen.insert(id.clone()) {
                self.live.push(id);
            }
        }
        Ok(())
    }
}

/// Partitions one membership page using ordered Bulk or fallback liveness.
async fn classify_members<F>(
    driver: Arc<dyn Driver>,
    bulk: Option<&dyn Bulk>,
    concurrency: usize,
    members: Vec<String>,
    entry_key: &F,
) -> Result<(Vec<String>, Vec<String>)>
where
    F: Fn(&str) -> String + Send + Sync,
{
    let qualified: Vec<(String, String)> = members
        .into_iter()
        .map(|id| {
            let key = entry_key(&id);
            (id, key)
        })
        .collect();
    let keys: Vec<String> = qualified.iter().map(|(_, key)| key.clone()).collect();
    let found = exists_all(driver, bulk, concurrency, &keys).await?;
    let checked = qualified
        .into_iter()
        .zip(found)
        .map(|((id, _), exists)| (id, exists));

    let mut live = Vec::with_capacity(checked.len());
    let mut stale = Vec::new();
    for (id, exists) in checked {
        if exists {
            live.push(id);
        } else {
            stale.push(id);
        }
    }

    Ok((live, stale))
}

/// Best-effort repair that never invalidates an already-correct live result.
async fn cleanup_stale(sets: &Arc<dyn Sets>, set_key: &str, stale: &[String]) {
    if stale.is_empty() {
        return;
    }
    let references: Vec<&str> = stale.iter().map(String::as_str).collect();
    let _cleanup = sets.set_remove(set_key, &references).await;
}

/// Fetches qualified keys in order, skipping only ordinary expiry races.
///
/// A key can disappear after liveness was checked, so NotFound leaves no output
/// item. Every other Driver failure aborts rather than returning a misleading
/// partial listing.
///
/// **Cost**: Up to one Driver read per key with bounded concurrency.
pub(crate) async fn get_all(
    driver: Arc<dyn Driver>,
    bulk: Option<&dyn Bulk>,
    concurrency: usize,
    keys: Vec<String>,
) -> Result<Vec<Vec<u8>>> {
    if let Some(bulk) = bulk {
        let mut bodies = Vec::with_capacity(keys.len());
        for batch in keys.chunks(BULK_BATCH_SIZE) {
            let values = bulk.get_many(batch).await?;
            validate_count("get_many", batch.len(), values.len())?;
            bodies.extend(values);
        }
        return Ok(bodies.into_iter().flatten().collect());
    }

    let bodies: Vec<Option<Vec<u8>>> = stream::iter(keys.into_iter().map(|key| {
        let driver = driver.clone();
        async move {
            match driver.get(&key).await {
                Ok(body) => Ok(Some(body)),
                Err(CacheError::NotFound) => Ok(None),
                Err(error) => Err(error),
            }
        }
    }))
    .buffered(concurrency.max(1))
    .try_collect()
    .await?;

    Ok(bodies.into_iter().flatten().collect())
}

/// Reports ordered liveness using Bulk batches or bounded per-key fallback.
async fn exists_all(
    driver: Arc<dyn Driver>,
    bulk: Option<&dyn Bulk>,
    concurrency: usize,
    keys: &[String],
) -> Result<Vec<bool>> {
    if let Some(bulk) = bulk {
        let mut found = Vec::with_capacity(keys.len());
        for batch in keys.chunks(BULK_BATCH_SIZE) {
            let values = bulk.exists_many(batch).await?;
            validate_count("exists_many", batch.len(), values.len())?;
            found.extend(values);
        }
        return Ok(found);
    }

    stream::iter(keys.iter().cloned().map(|key| {
        let driver = driver.clone();
        async move { driver.exists(&key).await }
    }))
    .buffered(concurrency.max(1))
    .try_collect()
    .await
}

/// Rejects malformed capability responses before ordered results are combined.
fn validate_count(operation: &str, expected: usize, actual: usize) -> Result<()> {
    if expected == actual {
        return Ok(());
    }
    Err(CacheError::Internal(format!(
        "Bulk::{operation} returned {actual} result(s) for {expected} key(s)"
    )))
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use super::*;
    use crate::core::{MemoryDriver, MemorySets};
    use tokio::sync::Mutex;

    struct ConcurrencyDriver {
        active: AtomicUsize,
        maximum: AtomicUsize,
    }

    struct RecordingBulk {
        get_batches: Mutex<Vec<Vec<String>>>,
        short_get: bool,
    }

    struct PageScanner {
        pages: Vec<Vec<String>>,
        fail_after_first: bool,
    }

    #[async_trait::async_trait]
    impl SetScanner for PageScanner {
        async fn set_scan(&self, _key: &str, visitor: &mut dyn SetScanVisitor) -> Result<()> {
            for (index, page) in self.pages.iter().cloned().enumerate() {
                visitor.visit(page).await?;
                if self.fail_after_first && index == 0 {
                    return Err(CacheError::Internal("scan failed".to_owned()));
                }
            }
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl Bulk for RecordingBulk {
        async fn get_many(&self, keys: &[String]) -> Result<Vec<Option<Vec<u8>>>> {
            self.get_batches.lock().await.push(keys.to_vec());
            if self.short_get {
                return Ok(Vec::new());
            }
            Ok(keys
                .iter()
                .map(|key| Some(key.as_bytes().to_vec()))
                .collect())
        }

        async fn exists_many(&self, keys: &[String]) -> Result<Vec<bool>> {
            Ok(vec![true; keys.len()])
        }
    }

    impl ConcurrencyDriver {
        fn new() -> Self {
            Self {
                active: AtomicUsize::new(0),
                maximum: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl Driver for ConcurrencyDriver {
        fn name(&self) -> &str {
            "concurrency-test"
        }

        async fn get(&self, _key: &str) -> Result<Vec<u8>> {
            Err(CacheError::Unsupported)
        }

        async fn set(&self, _key: &str, _value: &[u8], _ttl: Duration) -> Result<()> {
            Err(CacheError::Unsupported)
        }

        async fn add(&self, _key: &str, _value: &[u8], _ttl: Duration) -> Result<bool> {
            Err(CacheError::Unsupported)
        }

        async fn replace(&self, _key: &str, _value: &[u8], _ttl: Duration) -> Result<bool> {
            Err(CacheError::Unsupported)
        }

        async fn delete(&self, _keys: &[&str]) -> Result<()> {
            Err(CacheError::Unsupported)
        }

        async fn exists(&self, _key: &str) -> Result<bool> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.maximum.fetch_max(active, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(5)).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(true)
        }

        async fn touch(&self, _key: &str, _ttl: Duration) -> Result<()> {
            Err(CacheError::Unsupported)
        }
    }

    #[tokio::test]
    async fn test_live_members_bounds_fallback_exists_concurrency() {
        let driver = Arc::new(ConcurrencyDriver::new());
        let sets = Arc::new(MemorySets::new());
        sets.set_add("members", &["one", "two", "three", "four"])
            .await
            .unwrap();
        let sets_capability: Arc<dyn Sets> = sets;

        let live = live_members(
            driver.clone(),
            None,
            &sets_capability,
            None,
            2,
            "members",
            str::to_owned,
        )
        .await
        .unwrap();

        assert_eq!(live.len(), 4);
        assert_eq!(driver.maximum.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_get_all_bulk_chunks_at_256_and_preserves_order() {
        let bulk = RecordingBulk {
            get_batches: Mutex::new(Vec::new()),
            short_get: false,
        };
        let keys: Vec<String> = (0..257).map(|index| format!("key-{index}")).collect();

        let values = get_all(
            Arc::new(ConcurrencyDriver::new()),
            Some(&bulk),
            1,
            keys.clone(),
        )
        .await
        .unwrap();

        assert_eq!(values.len(), 257);
        assert_eq!(values[0], b"key-0");
        assert_eq!(values[256], b"key-256");
        let batches = bulk.get_batches.lock().await;
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), 256);
        assert_eq!(batches[1].len(), 1);
    }

    #[tokio::test]
    async fn test_get_all_bulk_rejects_wrong_result_count() {
        let bulk = RecordingBulk {
            get_batches: Mutex::new(Vec::new()),
            short_get: true,
        };

        let result = get_all(Arc::new(ConcurrencyDriver::new()), Some(&bulk), 1, vec![
            "key".to_owned(),
        ])
        .await;

        assert!(
            matches!(result, Err(CacheError::Internal(message)) if message.contains("1 key(s)"))
        );
    }

    #[tokio::test]
    async fn test_live_members_set_scanner_deduplicates_and_sweeps_per_page() {
        let driver = Arc::new(MemoryDriver::new());
        driver.set("one", b"one", Duration::ZERO).await.unwrap();
        driver.set("two", b"two", Duration::ZERO).await.unwrap();
        let sets = Arc::new(MemorySets::new());
        sets.set_add("members", &["one", "two", "stale"])
            .await
            .unwrap();
        let sets_capability: Arc<dyn Sets> = sets.clone();
        let scanner = PageScanner {
            pages: vec![vec!["one".to_owned(), "stale".to_owned()], vec![
                "one".to_owned(),
                "two".to_owned(),
            ]],
            fail_after_first: false,
        };

        let live = live_members(
            driver,
            None,
            &sets_capability,
            Some(&scanner),
            2,
            "members",
            str::to_owned,
        )
        .await
        .unwrap();

        assert_eq!(live, ["one", "two"]);
        let remaining = sets.set_members("members").await.unwrap();
        assert!(!remaining.contains(&"stale".to_owned()));
    }

    #[tokio::test]
    async fn test_live_members_set_scanner_failure_returns_error_not_partial_result() {
        let scanner = PageScanner {
            pages: vec![vec!["one".to_owned()], vec!["two".to_owned()]],
            fail_after_first: true,
        };
        let sets: Arc<dyn Sets> = Arc::new(MemorySets::new());

        let result = live_members(
            Arc::new(ConcurrencyDriver::new()),
            None,
            &sets,
            Some(&scanner),
            1,
            "members",
            str::to_owned,
        )
        .await;

        assert!(matches!(result, Err(CacheError::Internal(message)) if message == "scan failed"));
    }
}
