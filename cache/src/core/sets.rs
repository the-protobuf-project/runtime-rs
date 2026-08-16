//! Sets capability for maintaining indexes
//!
//! A Set is a server-side collection (like Redis SADD).
//! Without it, a backend cannot enumerate and cannot index.

use crate::Result;

/// Sets is implemented by drivers with server-side sets
/// 
/// This is what makes a group of ids addressable, enabling enumeration and indexing.
/// Faking it with a single key holding a serialized list would:
/// - Put every write in contention on one key
/// - Silently drop ids on race conditions
/// Better to say "unsupported" than to silently corrupt data
#[async_trait::async_trait]
pub trait Sets: Send + Sync {
    /// Add members to a set
    async fn set_add(&self, key: &str, members: &[&str]) -> Result<()>;
    
    /// Remove members from a set
    async fn set_remove(&self, key: &str, members: &[&str]) -> Result<()>;
    
    /// Get all members of a set
    async fn set_members(&self, key: &str) -> Result<Vec<String>>;
}

/// SetScanner walks a set with a cursor (for large sets)
///
/// Without it, reading an entire index means one huge reply from the server.
/// That's fine for thousands but ruinous for millions - Redis stalls on that one call.
/// A cursor breaks it into batches.
#[async_trait::async_trait]
pub trait SetScanner: Send + Sync {
    /// Scan a set with a cursor, calling fn for each batch of members
    async fn set_scan<F>(&self, key: &str, mut f: F) -> Result<()>
    where
        F: FnMut(Vec<String>) + Send;
}

/// Memory-based Sets implementation for testing
pub struct MemorySets {
    data: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, std::collections::HashSet<String>>>>,
}

impl MemorySets {
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
        let set = data.entry(key.to_string()).or_insert_with(std::collections::HashSet::new);
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
