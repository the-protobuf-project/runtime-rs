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

/// Cursor-based traversal for sets too large to retrieve in one response.
///
/// Without it, reading an entire index means one huge reply from the server.
/// That's fine for thousands but ruinous for millions - Redis stalls on that one call.
/// A cursor breaks it into batches.
#[async_trait::async_trait]
pub trait SetScanner: Send + Sync {
    /// Invokes `f` once per batch until the backend cursor is exhausted.
    ///
    /// The callback is synchronous and must not block. Drivers propagate scan
    /// errors and must not silently return a partial successful result.
    async fn set_scan<F>(&self, key: &str, mut f: F) -> Result<()>
    where
        F: FnMut(Vec<String>) + Send;
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
