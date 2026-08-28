//! Optional server-side set capabilities for enumeration and indexing.
//!
//! A Set is a server-side collection (like Redis SADD).
//! Without it, a backend cannot enumerate and cannot index.

use crate::Result;

/// Server-side unordered collections used to maintain groups of IDs.
///
/// This capability makes a group of IDs addressable, enabling enumeration and indexing.
/// Faking it with a single key holding a serialized list would:
/// - Put every write in contention on one key
/// - Silently drop ids on race conditions
/// Better to say "unsupported" than to silently corrupt data
#[async_trait::async_trait]
pub trait Sets: Send + Sync {
    /// Adds all members idempotently.
    ///
    /// **Cost**: One backend round trip. Creates the set when absent.
    async fn set_add(&self, key: &str, members: &[&str]) -> Result<()>;

    /// Removes all supplied members; absent members and sets are harmless.
    ///
    /// **Cost**: One backend round trip.
    async fn set_remove(&self, key: &str, members: &[&str]) -> Result<()>;

    /// Returns all members in unspecified order.
    ///
    /// **Cost**: One round trip plus memory proportional to set cardinality.
    async fn set_members(&self, key: &str) -> Result<Vec<String>>;
}

/// Asynchronous consumer for one cursor page of set members.
///
/// Drivers await each page before requesting the next, allowing core to perform
/// liveness checks and cleanup without accumulating the complete set.
///
/// **Trade-offs**: Object-safe dynamic dispatch adds a small per-page call cost
/// but permits asynchronous strategy work without exposing backend cursors.
/// **Scalability**: The visitor retains only the state it chooses across pages.
/// **Use when**: Consuming `SetScanner` through a capability trait object.
#[async_trait::async_trait]
pub trait SetScanVisitor: Send {
    /// Processes one non-empty cursor page.
    ///
    /// **Cost**: Visitor-defined and paid once per non-empty page.
    /// **Side effects**: Visitor-defined; an error stops the cursor walk and
    /// propagates to the strategy.
    /// **Use when**: Processing one bounded page before the driver advances its
    /// backend cursor.
    async fn visit(&mut self, members: Vec<String>) -> Result<()>;
}

/// Cursor-based traversal for sets too large to retrieve in one response.
///
/// Without it, reading an entire index means one huge reply from the server.
/// That's fine for thousands but ruinous for millions - Redis stalls on that one call.
/// A cursor breaks it into batches.
///
/// **Trade-offs**: Requires several bounded round trips instead of one large
/// response and may yield duplicate members under cursor semantics.
/// **Scalability**: Bounds driver-side response memory to one page.
/// **Use when**: A backend offers cursor traversal for large membership sets.
#[async_trait::async_trait]
pub trait SetScanner: Send + Sync {
    /// Invokes `visitor` once per non-empty page until the cursor is exhausted.
    ///
    /// Implementations may return duplicates across pages. Scan and visitor
    /// errors propagate; implementations must not report partial success.
    ///
    /// **Cost**: Backend-defined, normally one round trip per cursor page.
    /// **Side effects**: None required of the scanner; the visitor may mutate
    /// related storage while processing a page.
    /// **Use when**: Traversing a set without materializing every member in one
    /// backend response.
    async fn set_scan(&self, key: &str, visitor: &mut dyn SetScanVisitor) -> Result<()>;
}

/// Process-local Sets implementation for deterministic tests.
///
/// Clones share one lock-protected map when wrapped in the same `Arc`. It does
/// not expire set keys and is not intended as a distributed cache backend.
pub struct MemorySets {
    /// Set key to unique owned member strings.
    data: std::sync::Arc<
        tokio::sync::RwLock<std::collections::HashMap<String, std::collections::HashSet<String>>>,
    >,
}

impl MemorySets {
    /// Creates an empty in-memory set store.
    pub fn new() -> Self {
        Self {
            data: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
        }
    }
}

impl Default for MemorySets {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Sets for MemorySets {
    async fn set_add(&self, key: &str, members: &[&str]) -> Result<()> {
        let mut data = self.data.write().await;
        let set = data
            .entry(key.to_string())
            .or_insert_with(std::collections::HashSet::new);
        for member in members {
            set.insert(member.to_string());
        }
        Ok(())
    }

    async fn set_remove(&self, key: &str, members: &[&str]) -> Result<()> {
        let mut data = self.data.write().await;
        if let Some(set) = data.get_mut(key) {
            for member in members {
                set.remove(*member);
            }
            if set.is_empty() {
                data.remove(key);
            }
        }
        Ok(())
    }

    async fn set_members(&self, key: &str) -> Result<Vec<String>> {
        let data = self.data.read().await;
        match data.get(key) {
            Some(set) => Ok(set.iter().cloned().collect()),
            None => Ok(vec![]),
        }
    }
}

#[async_trait::async_trait]
impl SetScanner for MemorySets {
    /// Presents the in-memory set as one capability page.
    async fn set_scan(&self, key: &str, visitor: &mut dyn SetScanVisitor) -> Result<()> {
        let members = self.set_members(key).await?;
        if !members.is_empty() {
            visitor.visit(members).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CacheError;

    struct RecordingVisitor {
        pages: Vec<Vec<String>>,
        fail: bool,
    }

    #[async_trait::async_trait]
    impl SetScanVisitor for RecordingVisitor {
        async fn visit(&mut self, members: Vec<String>) -> Result<()> {
            if self.fail {
                return Err(CacheError::Internal("visitor failed".to_owned()));
            }
            self.pages.push(members);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_memory_sets_set_scan_visits_one_page() {
        let sets = MemorySets::new();
        sets.set_add("set", &["one", "two"]).await.unwrap();
        let mut visitor = RecordingVisitor {
            pages: Vec::new(),
            fail: false,
        };

        sets.set_scan("set", &mut visitor).await.unwrap();

        assert_eq!(visitor.pages.len(), 1);
        visitor.pages[0].sort();
        assert_eq!(visitor.pages[0], ["one", "two"]);
    }

    #[tokio::test]
    async fn test_memory_sets_set_scan_empty_set_skips_visitor() {
        let sets = MemorySets::new();
        let mut visitor = RecordingVisitor {
            pages: Vec::new(),
            fail: true,
        };

        sets.set_scan("missing", &mut visitor).await.unwrap();

        assert!(visitor.pages.is_empty());
    }

    #[tokio::test]
    async fn test_memory_sets_set_scan_visitor_failure_propagates() {
        let sets = MemorySets::new();
        sets.set_add("set", &["one"]).await.unwrap();

        let mut failing = RecordingVisitor {
            pages: Vec::new(),
            fail: true,
        };
        assert!(matches!(
            sets.set_scan("set", &mut failing).await,
            Err(CacheError::Internal(message)) if message == "visitor failed"
        ));
    }
}
