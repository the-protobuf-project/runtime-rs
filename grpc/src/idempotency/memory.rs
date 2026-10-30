//! A [`Store`] in memory.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::SystemTime;

use super::{ClockFn, Key, Store, StoreError};

/// A [`Store`] in process memory: names are unique, as a primary key makes them.
///
/// It is for tests and for a single-process service that accepts losing its keys on
/// restart; a replicated service needs the keys where every replica sees them.
pub struct MemoryStore {
    keys: Mutex<HashMap<String, Key>>,
    now: ClockFn,
}

impl MemoryStore {
    /// An empty store stamping claims with [`SystemTime::now`].
    #[must_use]
    pub fn new() -> Self {
        Self::with_clock(std::sync::Arc::new(SystemTime::now))
    }

    /// An empty store stamping claims with `now`.
    #[must_use]
    pub fn with_clock(now: ClockFn) -> Self {
        Self {
            keys: Mutex::new(HashMap::new()),
            now,
        }
    }

    /// How many keys it holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Whether it holds no keys.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Key>> {
        // A panic while holding the lock leaves the map as consistent as any single
        // insert or remove leaves it, so a poisoned lock is still safe to use.
        self.keys
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for MemoryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryStore")
            .field("keys", &self.len())
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl Store for MemoryStore {
    async fn create(&self, mut key: Key) -> Result<(), StoreError> {
        let mut keys = self.lock();
        if keys.contains_key(&key.name) {
            return Err(StoreError::backend("duplicate key"));
        }
        key.create_time = Some((self.now)());
        keys.insert(key.name.clone(), key);
        Ok(())
    }

    async fn get(&self, name: &str) -> Result<Key, StoreError> {
        self.lock().get(name).cloned().ok_or(StoreError::NotFound)
    }

    async fn settle(&self, key: Key) -> Result<(), StoreError> {
        let mut keys = self.lock();
        let current = keys.get_mut(&key.name).ok_or(StoreError::NotFound)?;
        current.state = key.state;
        current.response = key.response;
        current.response_type = key.response_type;
        Ok(())
    }

    async fn delete(&self, name: &str) -> Result<(), StoreError> {
        self.lock().remove(name);
        Ok(())
    }
}
