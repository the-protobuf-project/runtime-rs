//! This crate's error type.

/// Errors returned by helpers in this crate.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Returned by an optimistic-concurrency update helper (an "UpdateIfMatch"-style
    /// function in generated code, not implemented in this crate) when no row matched
    /// the precondition — i.e. the mutation reported zero affected rows because another
    /// writer changed the row first. Callers re-read the row and retry. Test for it
    /// with `matches!(err, network_graphql::Error::Conflict)`.
    #[error("graphql: update precondition failed (no rows matched; concurrent modification)")]
    Conflict,
}
