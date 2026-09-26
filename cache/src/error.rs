//! Cache error types

use thiserror::Error;

/// Stable failures exposed by cache strategies, drivers, and Providers.
///
/// Variants distinguish ordinary cache state, unsupported backend behavior,
/// policy rejection, overload, partial destructive work, and contextual
/// internal failures. Callers should normally match semantic variants and use
/// formatted messages only for diagnostics.
///
/// **Trade-offs**: One closed error type keeps object-safe APIs uniform but
/// converts backend-specific errors into [`CacheError::Internal`] context.
#[derive(Clone, Debug, Error)]
pub enum CacheError {
    /// The requested live entry does not exist or expired before it was read.
    #[error("cache: not found")]
    NotFound,

    /// The selected backend lacks a capability required by the operation.
    #[error("cache: unsupported by this backend")]
    Unsupported,

    /// A write resolved to permanence while the cache requires an expiry.
    #[error("cache: no expiry, and this cache requires one")]
    NoTTL,

    /// A bounded concurrency budget refused a new distinct unit of work.
    #[error("cache: too many concurrent loads")]
    Overloaded,

    /// A database drop removed earlier batches before a later delete failed.
    #[error("cache: database drop failed after deleting {deleted} keys: {source}")]
    PartialDelete {
        /// Number of unique keys successfully deleted before the failure.
        deleted: usize,
        /// Original Driver failure from the first unsuccessful batch.
        #[source]
        source: Box<CacheError>,
    },

    /// A backend, codec, lifecycle, or invariant failure with safe context.
    #[error("cache: {0}")]
    Internal(String),
}

/// Cache result using the crate's stable [`CacheError`] contract.
pub type Result<T> = std::result::Result<T, CacheError>;
