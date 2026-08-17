//! Optional backend capabilities resolved during database construction.

use std::sync::Arc;

use super::Sets;

/// Optional behavior a backend can provide beyond the required Driver trait.
///
/// Rust trait objects cannot be safely type-asserted into unrelated traits the
/// way Go interfaces can. A backend therefore declares its capabilities once
/// when constructing a database. Strategies retain only the capabilities they
/// use and return [`crate::CacheError::Unsupported`] when an operation requires
/// one that is absent.
///
/// This explicit bundle makes capability gaps visible without emulating them in
/// memory. Additional optional traits will be added here as their strategies
/// are implemented.
///
/// **Trade-off**: Backend construction is slightly more explicit than Go's
/// runtime assertions, but operation paths perform no repeated discovery.
#[derive(Clone, Default)]
pub struct Capabilities {
    sets: Option<Arc<dyn Sets>>,
}

impl Capabilities {
    /// Creates a bundle containing only the required Driver behavior.
    ///
    /// **Cost**: O(1), with no allocation or backend round trip.
    /// **Side effects**: None.
    pub fn new() -> Self {
        Self::default()
    }

    /// Declares server-side set support for enumeration and indexing.
    ///
    /// **Cost**: O(1); stores one reference-counted capability handle.
    /// **Side effects**: Replaces any previously declared Sets capability.
    pub fn with_sets(mut self, sets: Arc<dyn Sets>) -> Self {
        self.sets = Some(sets);
        self
    }

    /// Returns the declared Sets capability, if the backend has one.
    ///
    /// **Cost**: O(1); cloning increments an atomic reference count.
    /// **Side effects**: None on the backend.
    pub fn sets(&self) -> Option<Arc<dyn Sets>> {
        self.sets.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::MemorySets;

    #[test]
    fn test_capabilities_new_has_no_optional_capabilities() {
        assert!(Capabilities::new().sets().is_none());
    }

    #[test]
    fn test_capabilities_with_sets_exposes_declared_capability() {
        let capabilities = Capabilities::new().with_sets(Arc::new(MemorySets::new()));

        assert!(capabilities.sets().is_some());
    }
}
