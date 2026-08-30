//! Optional backend capabilities resolved during database construction.

use std::sync::Arc;

use super::{Bulk, Fenced, Leases, Scanner, SetScanner, Sets};

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
    /// Server-side unordered collections for enumeration and secondary indexes.
    sets: Option<Arc<dyn Sets>>,
    /// Remaining-expiry reporting for live entries.
    leases: Option<Arc<dyn Leases>>,
    /// Cursor-based whole-keyspace traversal for administrative deletion.
    scanner: Option<Arc<dyn Scanner>>,
    /// Ordered multi-key reads sharing backend round trips.
    bulk: Option<Arc<dyn Bulk>>,
    /// Cursor-based traversal of large server-side sets.
    set_scanner: Option<Arc<dyn SetScanner>>,
    /// Atomic compare-and-delete for safely releasing leased ownership keys.
    fenced: Option<Arc<dyn Fenced>>,
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

    /// Declares remaining-expiry reporting support.
    ///
    /// **Cost**: O(1); stores one reference-counted capability handle.
    /// **Side effects**: Replaces any previously declared Leases capability.
    pub fn with_leases(mut self, leases: Arc<dyn Leases>) -> Self {
        self.leases = Some(leases);
        self
    }

    /// Returns the declared Leases capability, if the backend can report TTL.
    ///
    /// **Cost**: O(1); cloning increments an atomic reference count.
    /// **Side effects**: None on the backend.
    pub fn leases(&self) -> Option<Arc<dyn Leases>> {
        self.leases.clone()
    }

    /// Declares cursor-based keyspace pattern scanning support.
    ///
    /// **Cost**: O(1); stores one reference-counted capability handle.
    /// **Side effects**: Replaces any previously declared Scanner capability.
    pub fn with_scanner(mut self, scanner: Arc<dyn Scanner>) -> Self {
        self.scanner = Some(scanner);
        self
    }

    /// Returns the declared Scanner capability, if the backend has one.
    ///
    /// **Cost**: O(1); cloning increments an atomic reference count.
    /// **Side effects**: None on the backend.
    pub fn scanner(&self) -> Option<Arc<dyn Scanner>> {
        self.scanner.clone()
    }

    /// Declares ordered multi-key read support.
    ///
    /// **Cost**: O(1); stores one reference-counted capability handle.
    /// **Side effects**: Replaces any previously declared Bulk capability.
    pub fn with_bulk(mut self, bulk: Arc<dyn Bulk>) -> Self {
        self.bulk = Some(bulk);
        self
    }

    /// Returns the declared Bulk capability, if the backend can batch reads.
    ///
    /// **Cost**: O(1); cloning increments an atomic reference count.
    /// **Side effects**: None on the backend.
    pub fn bulk(&self) -> Option<Arc<dyn Bulk>> {
        self.bulk.clone()
    }

    /// Declares cursor-based server-side set traversal.
    ///
    /// **Cost**: O(1); stores one reference-counted capability handle.
    /// **Side effects**: Replaces any previously declared SetScanner capability.
    pub fn with_set_scanner(mut self, set_scanner: Arc<dyn SetScanner>) -> Self {
        self.set_scanner = Some(set_scanner);
        self
    }

    /// Returns the declared SetScanner capability, if available.
    ///
    /// **Cost**: O(1); cloning increments an atomic reference count.
    /// **Side effects**: None on the backend.
    pub fn set_scanner(&self) -> Option<Arc<dyn SetScanner>> {
        self.set_scanner.clone()
    }

    /// Declares atomic compare-and-delete support.
    ///
    /// **Cost**: O(1); stores one reference-counted capability handle.
    /// **Side effects**: Replaces any previously declared Fenced capability.
    pub fn with_fenced(mut self, fenced: Arc<dyn Fenced>) -> Self {
        self.fenced = Some(fenced);
        self
    }

    /// Returns the declared Fenced capability, if available.
    ///
    /// **Cost**: O(1); cloning increments an atomic reference count.
    /// **Side effects**: None on the backend.
    pub fn fenced(&self) -> Option<Arc<dyn Fenced>> {
        self.fenced.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{MemoryDriver, MemorySets};

    struct EmptyScanner;

    #[async_trait::async_trait]
    impl Scanner for EmptyScanner {
        async fn scan(&self, _pattern: &str) -> crate::Result<Vec<String>> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn test_capabilities_new_has_no_optional_capabilities() {
        assert!(Capabilities::new().sets().is_none());
        assert!(Capabilities::new().leases().is_none());
        assert!(Capabilities::new().scanner().is_none());
        assert!(Capabilities::new().bulk().is_none());
        assert!(Capabilities::new().set_scanner().is_none());
        assert!(Capabilities::new().fenced().is_none());
    }

    #[test]
    fn test_capabilities_with_sets_exposes_declared_capability() {
        let capabilities = Capabilities::new().with_sets(Arc::new(MemorySets::new()));

        assert!(capabilities.sets().is_some());
    }

    #[test]
    fn test_capabilities_with_leases_exposes_declared_capability() {
        let capabilities = Capabilities::new().with_leases(Arc::new(MemoryDriver::new()));

        assert!(capabilities.leases().is_some());
    }

    #[test]
    fn test_capabilities_with_scanner_exposes_declared_capability() {
        let capabilities = Capabilities::new().with_scanner(Arc::new(EmptyScanner));

        assert!(capabilities.scanner().is_some());
    }

    #[test]
    fn test_capabilities_with_bulk_exposes_declared_capability() {
        let capabilities = Capabilities::new().with_bulk(Arc::new(MemoryDriver::new()));

        assert!(capabilities.bulk().is_some());
    }

    #[test]
    fn test_capabilities_with_set_scanner_exposes_declared_capability() {
        let capabilities = Capabilities::new().with_set_scanner(Arc::new(MemorySets::new()));

        assert!(capabilities.set_scanner().is_some());
    }

    #[test]
    fn test_capabilities_with_fenced_exposes_declared_capability() {
        let capabilities = Capabilities::new().with_fenced(Arc::new(MemoryDriver::new()));

        assert!(capabilities.fenced().is_some());
    }
}
