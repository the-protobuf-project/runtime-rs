//! Optional pattern-based keyspace scanning.

use crate::Result;

/// Walks backend keys matching a glob-style pattern.
///
/// Scanner is optional because some backends cannot enumerate keys safely.
/// Its pattern contract follows Go core and Redis glob syntax. Callers placing
/// literal text into a pattern must escape metacharacters first. Returned keys
/// must be unique, even when the backend cursor may emit duplicates.
///
/// **Scalability**: Implementations should use a non-blocking cursor rather
/// than a whole-keyspace command such as Redis `KEYS`.
#[async_trait::async_trait]
pub trait Scanner: Send + Sync {
    /// Returns every unique live key matching `pattern`.
    ///
    /// **Cost**: Backend-specific cursor walk, generally proportional to the
    /// whole keyspace. **Side effects**: None. **When to use**: Administrative
    /// namespace teardown, not request-path lookup.
    async fn scan(&self, pattern: &str) -> Result<Vec<String>>;
}
