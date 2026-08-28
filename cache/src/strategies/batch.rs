//! Shared bounded fallbacks for strategy operations spanning many entries.
//!
//! These helpers preserve result order and cap concurrent Driver calls. Future
//! Bulk and SetScanner capabilities can replace individual phases without
//! changing the Document or Indexed public behavior.

use std::sync::Arc;

use futures::{StreamExt, TryStreamExt, stream};

use crate::{
    CacheError, Result,
    core::{Driver, Sets},
};

/// Fan-out used when database configuration does not select one.
pub(crate) const DEFAULT_CONCURRENCY: usize = 16;

/// Returns live set members and best-effort removes stale ones.
///
/// Membership order is preserved. Existence failures abort the read, while a
/// stale-member cleanup failure is suppressed because the returned live view is
/// still correct and a later read can retry cleanup.
///
/// **Cost**: One Sets read, up to one Driver existence read per member with
/// bounded concurrency, and at most one Sets cleanup write.
pub(crate) async fn live_members<F>(
    driver: Arc<dyn Driver>,
    sets: &Arc<dyn Sets>,
    concurrency: usize,
    set_key: &str,
    entry_key: F,
) -> Result<Vec<String>>
where
    F: Fn(&str) -> String,
{
    let members = sets.set_members(set_key).await?;
    if members.is_empty() {
        return Ok(Vec::new());
    }

    let qualified: Vec<(String, String)> = members
        .into_iter()
        .map(|id| {
            let key = entry_key(&id);
            (id, key)
        })
        .collect();
    let checked: Vec<(String, bool)> = stream::iter(qualified.into_iter().map(|(id, key)| {
        let driver = driver.clone();
        async move {
            let exists = driver.exists(&key).await?;
            Ok::<_, CacheError>((id, exists))
        }
    }))
    .buffered(concurrency.max(1))
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
        let references: Vec<&str> = stale.iter().map(String::as_str).collect();
        // Cleanup is repair work, not part of producing the correct live view.
        let _cleanup = sets.set_remove(set_key, &references).await;
    }
    Ok(live)
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
    concurrency: usize,
    keys: Vec<String>,
) -> Result<Vec<Vec<u8>>> {
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

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use super::*;
    use crate::core::MemorySets;

    struct ConcurrencyDriver {
        active: AtomicUsize,
        maximum: AtomicUsize,
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
            &sets_capability,
            2,
            "members",
            str::to_owned,
        )
        .await
        .unwrap();

        assert_eq!(live.len(), 4);
        assert_eq!(driver.maximum.load(Ordering::SeqCst), 2);
    }
}
