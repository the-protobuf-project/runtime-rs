use std::time::Duration;

/// Options for cache operations
#[derive(Clone, Debug, Default)]
pub struct Options {
    /// ID to store entry under (for Document.Create)
    pub id: Option<String>,

    /// How long the entry stays fresh
    pub ttl: Option<Duration>,

    /// How much longer past TTL entry may be served while refreshing
    pub stale: Option<Duration>,

    /// Secondary keys to file this entry under (for Indexed)
    pub indexes: Option<std::collections::HashMap<String, String>>,

    /// Whether this entry has no expiry
    pub permanent: bool,
}
