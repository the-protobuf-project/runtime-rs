//! Option builder pattern (Rust idiomatic)
//!
//! Implements fluent API similar to Go options, but using Rust's builder pattern.

use super::Options;
use std::collections::HashMap;
use std::time::Duration;

impl Options {
    /// Set the custom ID for this entry (Document.Create only)
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Set how long the entry stays fresh
    pub fn with_ttl(mut self, duration: Duration) -> Self {
        self.ttl = Some(duration);
        self
    }

    /// Set the staleness window for Aside read-through cache
    /// Expired entries are served while refreshing in background
    pub fn with_stale(mut self, duration: Duration) -> Self {
        self.stale = Some(duration);
        self
    }

    /// Set a secondary index (Indexed strategy only)
    pub fn with_index(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.indexes
            .get_or_insert_with(HashMap::new)
            .insert(name.into(), value.into());
        self
    }

    /// Mark this entry as permanent (no expiry), explicitly stating intent
    pub fn permanent(mut self) -> Self {
        self.permanent = true;
        self
    }

    /// No expiry - explicit marker that this entry lives forever
    pub fn no_expiry(mut self) -> Self {
        self.permanent = true;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_pattern() {
        let opts = Options::default()
            .with_id("entry-123")
            .with_ttl(Duration::from_secs(60))
            .permanent();

        assert_eq!(opts.id, Some("entry-123".to_string()));
        assert_eq!(opts.ttl, Some(Duration::from_secs(60)));
        assert!(opts.permanent);
    }
}
