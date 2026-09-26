//! Optional pattern-based keyspace scanning.

use crate::Result;

/// Escapes literal text before it is embedded in a glob-style scan pattern.
///
/// Callers append any intentional wildcard syntax after this function returns.
/// Keeping the fixed namespace literal prevents configuration text such as
/// `app:*` from widening a scan into adjacent keyspaces.
///
/// **Cost**: O(n) local string construction. **Side effects**: None.
pub(crate) fn escape_glob_literal(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        if matches!(character, '*' | '?' | '[' | ']' | '\\') {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped
}

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
