use crate::Result;

use super::Document;

/// Enumerable Document storage with secondary field/value indexes.
///
/// Indexed is for values commonly retrieved by something other than their ID:
/// a user by e-mail, orders by tenant, or cached computations by version. Each
/// secondary index is maintained on writes so callers do not need a second,
/// independently maintained cache.
///
/// # Trade-offs
///
/// - Each indexed field adds set writes and cleanup work.
/// - Low-cardinality values create shared hot keys containing many IDs.
/// - Expired entries can leave memberships behind until lookups sweep them.
/// - A backend without server-side Sets cannot perform secondary operations and
///   returns [`crate::CacheError::Unsupported`].
///
/// Prefer [`Document`] when enumeration by ID is sufficient, or Volatile when
/// neither enumeration nor secondary lookup is needed.
#[async_trait::async_trait]
pub trait Indexed: Document {
    /// Returns every live value filed under `field=value`.
    ///
    /// No match is an empty collection rather than `NotFound`, because this is
    /// a set query rather than a request for one expected entry. Implementations
    /// verify entry liveness and sweep expired memberships while reading.
    ///
    /// **Cost**: At least one set read plus value reads for every live matching
    /// ID; future bulk capabilities may reduce round trips.
    ///
    /// **Side effects**: May remove stale members whose entries have expired.
    async fn by_index(&self, field: &str, value: &str) -> Result<Vec<Vec<u8>>>;

    /// Returns the IDs of live entries filed under `field=value`.
    ///
    /// Use this instead of [`Indexed::by_index`] when callers need only IDs and
    /// should not pay to transfer and decode every stored value.
    ///
    /// **Cost**: One set read plus bounded liveness checks unless a backend
    /// capability supplies a batched path.
    ///
    /// **Side effects**: May remove stale members whose entries have expired.
    async fn ids_by_index(&self, field: &str, value: &str) -> Result<Vec<String>>;

    /// Deletes every live entry filed under `field=value` and returns the count.
    ///
    /// This is group invalidation for entries belonging to one tenant, user, or
    /// computation version. No match succeeds with zero.
    ///
    /// **Cost**: Proportional to the matching entries and all secondary
    /// memberships attached to them.
    ///
    /// **Side effects**: Removes values, their enumeration members, their field
    /// records, and all secondary memberships that name them.
    async fn delete_by_index(&self, field: &str, value: &str) -> Result<usize>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_indexed_contract_is_object_safe_for_database_field() {
        fn accepts_indexed_trait_object(_: Option<&dyn Indexed>) {}

        accepts_indexed_trait_object(None);
    }
}
