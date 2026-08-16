//! Keyspace management for strategy key separation
//!
//! Four strategies share one backend storage, so each needs its own key segment.
//! This module builds qualified key names that prevent collisions.
//!
//! Key format:
//! {prefix}:{namespace}:cache:{strategy}:{segment}
//!
//! Example keys:
//! app:orders:cache:doc:entry:order-123          Document entry
//! app:orders:cache:vol:session-abc               Volatile entry
//! app:orders:cache:idx:entry:user-456            Indexed entry
//! app:orders:cache:aside:entry:product-789       Aside entry

#[derive(Clone, Debug)]
pub struct Keyspace {
    base: String,
}

impl Keyspace {
    /// Create a new keyspace with prefix and namespace
    pub fn new(prefix: &str, namespace: &str, _db: usize, _embed_db: bool) -> Self {
        let mut base = String::new();

        if !prefix.is_empty() {
            base.push_str(prefix);
            base.push(':');
        }

        if !namespace.is_empty() {
            base.push_str(namespace);
            base.push(':');
        }

        base.push_str("cache:");

        Self { base }
    }

    // Document strategy keys
    pub fn doc_entry(&self, id: &str) -> String {
        format!("{}doc:entry:{}", self.base, id)
    }

    pub fn doc_index(&self) -> String {
        format!("{}doc:index", self.base)
    }

    // Volatile strategy keys
    pub fn vol_entry(&self, key: &str) -> String {
        format!("{}vol:{}", self.base, key)
    }

    // Indexed strategy keys
    pub fn idx_entry(&self, id: &str) -> String {
        format!("{}idx:entry:{}", self.base, id)
    }

    pub fn idx_index(&self) -> String {
        format!("{}idx:index", self.base)
    }

    pub fn idx_by_field(&self, field: &str, value: &str) -> String {
        format!("{}idx:by:{}:{}", self.base, field, value)
    }

    pub fn idx_fields(&self, id: &str) -> String {
        format!("{}idx:fields:{}", self.base, id)
    }

    // Aside strategy keys
    pub fn aside_entry(&self, id: &str) -> String {
        format!("{}aside:entry:{}", self.base, id)
    }

    pub fn aside_lock(&self, id: &str) -> String {
        format!("{}aside:lock:{}", self.base, id)
    }
}

/// Validate a namespace name - no colons allowed
pub fn check_namespace(name: &str) -> crate::Result<()> {
    if name.is_empty() {
        return Err(crate::CacheError::Internal(
            "database name cannot be empty".to_string(),
        ));
    }
    if name.contains(':') {
        return Err(crate::CacheError::Internal(format!(
            "database name '{}' cannot contain ':' (separates prefix from name)",
            name
        )));
    }
    Ok(())
}

/// UUID generator for unique IDs
pub struct IDGenerator;

impl IDGenerator {
    /// Generate a new unique ID
    pub fn new_id() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::time::{SystemTime, UNIX_EPOCH};

        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let counter = COUNTER.fetch_add(1, Ordering::SeqCst);

        // Format: {timestamp_hex}-{counter_hex}
        format!("{:x}-{:x}", nanos, counter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keyspace_keys() {
        let ks = Keyspace::new("app", "orders", 0, false);

        assert_eq!(
            ks.doc_entry("order-123"),
            "app:orders:cache:doc:entry:order-123"
        );
        assert_eq!(
            ks.vol_entry("session-abc"),
            "app:orders:cache:vol:session-abc"
        );
        assert_eq!(ks.doc_index(), "app:orders:cache:doc:index");
    }

    #[test]
    fn test_keyspace_no_prefix() {
        let ks = Keyspace::new("", "mydb", 0, false);
        assert_eq!(ks.doc_entry("id"), "mydb:cache:doc:entry:id");
    }

    #[test]
    fn test_id_generator() {
        let id1 = IDGenerator::new_id();
        let id2 = IDGenerator::new_id();

        assert!(!id1.is_empty());
        assert!(!id2.is_empty());
        assert_ne!(id1, id2); // Should be unique
    }

    #[test]
    fn test_namespace_validation() {
        assert!(check_namespace("valid_name").is_ok());
        assert!(check_namespace("").is_err()); // Empty
        assert!(check_namespace("bad:name").is_err()); // Contains colon
    }
}
