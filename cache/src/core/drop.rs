//! Shared bounded deletion for a cache database namespace.

use std::collections::HashSet;

use crate::{CacheError, Result};

use super::{Driver, Scanner};

const DROP_BATCH: usize = 256;

/// Deletes every key under one literal keyspace head.
///
/// Scanning is non-atomic, so a concurrent write may survive. Deletions are
/// issued in bounded batches; if a later batch fails, earlier batches remain
/// deleted and the error is returned.
///
/// **Cost**: One cursor walk plus `ceil(keys / 256)` delete round trips.
/// **Side effects**: Permanently deletes all keys returned by the Scanner.
/// **When to use**: Administrative teardown of a named cache database.
pub async fn drop_database(
    driver: &dyn Driver,
    scanner: &dyn Scanner,
    head: &str,
) -> Result<usize> {
    let pattern = prefix_pattern(head);
    let scanned = scanner.scan(&pattern).await?;
    if scanned.iter().any(|key| !key.starts_with(head)) {
        return Err(CacheError::Internal(
            "scanner returned a key outside the requested prefix".to_owned(),
        ));
    }

    // Cursor scans may repeat keys while the keyspace changes. Deleting twice
    // is harmless, but it would overstate the administrative result count.
    let mut seen = HashSet::with_capacity(scanned.len());
    let keys: Vec<String> = scanned
        .into_iter()
        .filter(|key| seen.insert(key.clone()))
        .collect();
    let mut deleted = 0;
    for batch in keys.chunks(DROP_BATCH) {
        let references: Vec<&str> = batch.iter().map(String::as_str).collect();
        if let Err(source) = driver.delete(&references).await {
            if deleted == 0 {
                return Err(source);
            }
            return Err(CacheError::PartialDelete {
                deleted,
                source: Box::new(source),
            });
        }
        deleted += batch.len();
    }
    Ok(deleted)
}

/// Embeds a literal keyspace head into the Scanner's glob-style contract.
fn prefix_pattern(head: &str) -> String {
    let mut pattern = String::with_capacity(head.len() + 1);
    for character in head.chars() {
        if matches!(character, '*' | '?' | '[' | ']' | '\\') {
            pattern.push('\\');
        }
        pattern.push(character);
    }
    pattern.push('*');
    pattern
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tokio::sync::Mutex;

    use super::*;
    use crate::{CacheError, core::MemoryDriver};

    struct FixedScanner {
        keys: Vec<String>,
        patterns: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl Scanner for FixedScanner {
        async fn scan(&self, pattern: &str) -> Result<Vec<String>> {
            self.patterns.lock().await.push(pattern.to_owned());
            Ok(self.keys.clone())
        }
    }

    struct FailingScanner;

    #[async_trait::async_trait]
    impl Scanner for FailingScanner {
        async fn scan(&self, _pattern: &str) -> Result<Vec<String>> {
            Err(CacheError::Internal("scan failed".to_owned()))
        }
    }

    struct RecordingDriver {
        inner: Arc<MemoryDriver>,
        delete_batches: Mutex<Vec<usize>>,
        fail_batch: Option<usize>,
    }

    #[async_trait::async_trait]
    impl Driver for RecordingDriver {
        fn name(&self) -> &str {
            self.inner.name()
        }

        async fn get(&self, key: &str) -> Result<Vec<u8>> {
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
            let batch_number = {
                let mut batches = self.delete_batches.lock().await;
                batches.push(keys.len());
                batches.len()
            };
            if self.fail_batch == Some(batch_number) {
                return Err(CacheError::Internal("delete failed".to_owned()));
            }
            self.inner.delete(keys).await
        }

        async fn exists(&self, key: &str) -> Result<bool> {
            self.inner.exists(key).await
        }

        async fn touch(&self, key: &str, ttl: Duration) -> Result<()> {
            self.inner.touch(key, ttl).await
        }
    }

    #[tokio::test]
    async fn test_drop_database_deletes_scanned_keys_in_bounded_batches() {
        let memory = Arc::new(MemoryDriver::new());
        let driver = RecordingDriver {
            inner: memory.clone(),
            delete_batches: Mutex::new(Vec::new()),
            fail_batch: None,
        };
        let keys: Vec<String> = (0..257)
            .map(|index| format!("app:orders:cache:key-{index}"))
            .collect();
        for key in &keys {
            memory.set(key, b"value", Duration::ZERO).await.unwrap();
        }
        let scanner = FixedScanner {
            keys: keys.clone(),
            patterns: Mutex::new(Vec::new()),
        };

        let deleted = drop_database(&driver, &scanner, "app:orders:cache:")
            .await
            .unwrap();

        assert_eq!(deleted, 257);
        assert_eq!(*scanner.patterns.lock().await, vec![
            "app:orders:cache:*".to_owned()
        ]);
        assert_eq!(*driver.delete_batches.lock().await, [256, 1]);
        for key in keys {
            assert!(!memory.exists(&key).await.unwrap());
        }
    }

    #[tokio::test]
    async fn test_drop_database_empty_scan_performs_no_delete() {
        let driver = RecordingDriver {
            inner: Arc::new(MemoryDriver::new()),
            delete_batches: Mutex::new(Vec::new()),
            fail_batch: None,
        };
        let scanner = FixedScanner {
            keys: Vec::new(),
            patterns: Mutex::new(Vec::new()),
        };

        let deleted = drop_database(&driver, &scanner, "empty:cache:")
            .await
            .unwrap();

        assert_eq!(deleted, 0);
        assert!(driver.delete_batches.lock().await.is_empty());
    }

    #[test]
    fn test_drop_database_pattern_escapes_literal_keyspace_head() {
        assert_eq!(
            prefix_pattern(r"app:*?[x]\:cache:"),
            r"app:\*\?\[x\]\\:cache:*"
        );
    }

    #[tokio::test]
    async fn test_drop_database_scan_failure_prevents_delete() {
        let driver = RecordingDriver {
            inner: Arc::new(MemoryDriver::new()),
            delete_batches: Mutex::new(Vec::new()),
            fail_batch: None,
        };

        let result = drop_database(&driver, &FailingScanner, "app:orders:cache:").await;

        assert!(matches!(result, Err(CacheError::Internal(message)) if message == "scan failed"));
        assert!(driver.delete_batches.lock().await.is_empty());
    }

    #[tokio::test]
    async fn test_drop_database_rejects_scanner_key_outside_prefix_before_delete() {
        let driver = RecordingDriver {
            inner: Arc::new(MemoryDriver::new()),
            delete_batches: Mutex::new(Vec::new()),
            fail_batch: None,
        };
        let scanner = FixedScanner {
            keys: vec!["another:cache:key".to_owned()],
            patterns: Mutex::new(Vec::new()),
        };

        let result = drop_database(&driver, &scanner, "app:orders:cache:").await;

        assert!(matches!(result, Err(CacheError::Internal(_))));
        assert!(driver.delete_batches.lock().await.is_empty());
    }

    #[tokio::test]
    async fn test_drop_database_counts_duplicate_cursor_key_once() {
        let memory = Arc::new(MemoryDriver::new());
        let driver = RecordingDriver {
            inner: memory.clone(),
            delete_batches: Mutex::new(Vec::new()),
            fail_batch: None,
        };
        let key = "app:orders:cache:key".to_owned();
        memory.set(&key, b"value", Duration::ZERO).await.unwrap();
        let scanner = FixedScanner {
            keys: vec![key.clone(), key.clone()],
            patterns: Mutex::new(Vec::new()),
        };

        let deleted = drop_database(&driver, &scanner, "app:orders:cache:")
            .await
            .unwrap();

        assert_eq!(deleted, 1);
        assert_eq!(*driver.delete_batches.lock().await, [1]);
        assert!(!memory.exists(&key).await.unwrap());
    }

    #[tokio::test]
    async fn test_drop_database_delete_failure_keeps_earlier_batch_deleted() {
        let memory = Arc::new(MemoryDriver::new());
        let driver = RecordingDriver {
            inner: memory.clone(),
            delete_batches: Mutex::new(Vec::new()),
            fail_batch: Some(2),
        };
        let keys: Vec<String> = (0..257)
            .map(|index| format!("app:orders:cache:key-{index}"))
            .collect();
        for key in &keys {
            memory.set(key, b"value", Duration::ZERO).await.unwrap();
        }
        let scanner = FixedScanner {
            keys: keys.clone(),
            patterns: Mutex::new(Vec::new()),
        };

        let result = drop_database(&driver, &scanner, "app:orders:cache:").await;

        match result {
            Err(CacheError::PartialDelete { deleted, source }) => {
                assert_eq!(deleted, 256);
                assert!(
                    matches!(*source, CacheError::Internal(message) if message == "delete failed")
                );
            }
            other => panic!("expected partial delete failure, got {other:?}"),
        }
        assert_eq!(*driver.delete_batches.lock().await, [256, 1]);
        for key in &keys[..256] {
            assert!(!memory.exists(key).await.unwrap());
        }
        assert!(memory.exists(&keys[256]).await.unwrap());
    }
}
