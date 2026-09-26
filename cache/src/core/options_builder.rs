//! Fluent construction helpers for [`Options`].
//!
//! Builders consume and return the value, allowing operation settings to be
//! composed inline without mutable configuration state.

use super::Options;
use std::collections::HashMap;
use std::time::Duration;

impl Options {
    /// Selects the ID used by Document or Indexed create.
    ///
    /// Without this setting, the database's configured ID generator is used.
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Sets an explicit freshness lease.
    ///
    /// This takes precedence over `permanent` and database defaults.
    pub fn with_ttl(mut self, duration: Duration) -> Self {
        self.ttl = Some(duration);
        self
    }

    /// Allows Aside to serve an expired value for this additional duration.
    ///
    /// A stale hit returns immediately and starts a background refresh.
    pub fn with_stale(mut self, duration: Duration) -> Self {
        self.stale = Some(duration);
        self
    }

    /// Adds or replaces one secondary field/value membership for Indexed.
    ///
    /// Repeated calls with the same field retain only the last value.
    pub fn with_index(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.indexes
            .get_or_insert_with(HashMap::new)
            .insert(name.into(), value.into());
        self
    }

    /// Explicitly requests a permanent entry when no TTL is supplied.
    pub fn permanent(mut self) -> Self {
        self.permanent = true;
        self
    }

    /// Alias for [`Options::permanent`].
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
