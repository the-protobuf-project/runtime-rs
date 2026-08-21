//! Central key construction and database-namespace validation.
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

/// Immutable prefix builder shared by every strategy in one selected database.
///
/// Centralizing all key formats prevents strategy collisions and ensures
/// administrative deletion uses exactly the same database boundary as normal
/// operations. Clones are cheap apart from cloning the base string.
#[derive(Clone, Debug)]
pub struct Keyspace {
    /// Fully qualified database head ending in `cache:`.
    base: String,
}

impl Keyspace {
    /// Builds the collision-safe key prefix for one selected database.
    ///
    /// `prefix` separates applications and `namespace` identifies a named
    /// database. When a backend has no native databases, `embed_db` adds the
    /// selected numeric index as `db{index}`. Backends with native databases
    /// must leave it false because the connection already enforces that
    /// boundary and repeating the index would only change the wire keys.
    ///
    /// **Cost**: Local string construction only; no driver round trip.
    /// **Side effects**: None.
    pub fn new(prefix: &str, namespace: &str, db: usize, embed_db: bool) -> Self {
        let mut base = String::new();

        if !prefix.is_empty() {
            base.push_str(prefix);
            base.push(':');
        }

        if !namespace.is_empty() {
            base.push_str(namespace);
            base.push(':');
        }

        if embed_db {
            base.push_str(&format!("db{db}:"));
        }

        base.push_str("cache:");

        Self { base }
    }

    /// Returns the prefix shared by every strategy in this database.
    ///
    /// A future database-drop operation can qualify its backend scan with this
    /// head without reconstructing or partially duplicating keyspace rules.
    ///
    /// **Cost**: O(1), with no allocation or driver round trip.
    /// **Side effects**: None.
    pub fn head(&self) -> &str {
        &self.base
    }

    /// Qualifies a Document value ID.
    pub fn doc_entry(&self, id: &str) -> String {
        format!("{}doc:entry:{}", self.base, id)
    }

    /// Returns the shared Document enumeration-set key.
    pub fn doc_index(&self) -> String {
        format!("{}doc:index", self.base)
    }

    /// Qualifies a caller-provided Volatile key.
    pub fn vol_entry(&self, key: &str) -> String {
        format!("{}vol:{}", self.base, key)
    }

    /// Qualifies an Indexed value ID.
    pub fn idx_entry(&self, id: &str) -> String {
        format!("{}idx:entry:{}", self.base, id)
    }

    /// Returns the shared Indexed enumeration-set key.
    pub fn idx_index(&self) -> String {
        format!("{}idx:index", self.base)
    }

    /// Returns the membership-set key for one secondary field/value pair.
    pub fn idx_by_field(&self, field: &str, value: &str) -> String {
        format!("{}idx:by:{}:{}", self.base, field, value)
    }

    /// Returns the field-metadata set key for one Indexed entry.
    pub fn idx_fields(&self, id: &str) -> String {
        format!("{}idx:fields:{}", self.base, id)
    }

    /// Qualifies an Aside value-frame ID.
    pub fn aside_entry(&self, id: &str) -> String {
        format!("{}aside:entry:{}", self.base, id)
    }

    /// Qualifies the reserved distributed-lock key for an Aside ID.
    ///
    /// Current coordination is process-local; this format is reserved for a
    /// future fenced lock capability and does not itself acquire a lock.
    pub fn aside_lock(&self, id: &str) -> String {
        format!("{}aside:lock:{}", self.base, id)
    }
}

/// Validates one named database segment before key construction.
///
/// Names must be non-empty and cannot contain `:`, because colons delimit
/// independently controlled keyspace segments. This is a local validation with
/// no allocation on success, backend I/O, or side effects.
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

/// Validates a database name against an optional configured allowlist.
///
/// An empty allowlist permits every already-valid namespace. This check is
/// local and has no backend round trip or side effects. Providers use it after
/// [`check_namespace`] and before contacting storage, so a rejected selection
/// cannot open or mutate backend state.
///
/// Returns an error containing the requested and configured names when a
/// non-empty allowlist has no exact match. Administrative database deletion may
/// intentionally omit this check so stale configuration names can be removed.
pub fn check_known(name: &str, known: &[String]) -> crate::Result<()> {
    if known.is_empty() || known.iter().any(|candidate| candidate == name) {
        return Ok(());
    }
    Err(crate::CacheError::Internal(format!(
        "database '{name}' is not one of the configured databases {known:?}"
    )))
}

/// Legacy process-local fallback ID generator.
///
/// IDs combine wall-clock nanoseconds with an atomic counter. This avoids
/// collisions within a normally progressing process but is not a UUID and does
/// not promise global uniqueness across hosts or clock resets. New databases
/// use the shared UUID-based generator configured by `DatabaseSpec` instead.
pub struct IDGenerator;

impl IDGenerator {
    /// Produces a timestamp/counter ID without backend I/O.
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
    fn test_keyspace_new_embedded_database_separates_indexes() {
        let first = Keyspace::new("app", "", 2, true);
        let second = Keyspace::new("app", "", 7, true);

        assert_eq!(first.vol_entry("session"), "app:db2:cache:vol:session");
        assert_eq!(second.vol_entry("session"), "app:db7:cache:vol:session");
        assert_ne!(first.vol_entry("session"), second.vol_entry("session"));
    }

    #[test]
    fn test_keyspace_new_native_database_omits_index() {
        let keys = Keyspace::new("app", "", 7, false);

        assert_eq!(keys.vol_entry("session"), "app:cache:vol:session");
    }

    #[test]
    fn test_keyspace_head_returns_complete_database_prefix() {
        let named = Keyspace::new("app", "orders", 0, false);
        let embedded = Keyspace::new("app", "", 3, true);

        assert_eq!(named.head(), "app:orders:cache:");
        assert_eq!(embedded.head(), "app:db3:cache:");
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

    #[test]
    fn test_keyspace_check_known_enforces_nonempty_allowlist() {
        assert!(check_known("orders", &[]).is_ok());
        assert!(check_known("orders", &["orders".to_owned()]).is_ok());
        assert!(check_known("users", &["orders".to_owned()]).is_err());
    }
}
