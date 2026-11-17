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

    /// A [`Loader`](crate::core::Loader)'s own failure, other than `NotFound`, carried out
    /// of [`Aside`](crate::core::Aside) unchanged so the caller can downcast it back to its
    /// own error type. Shared because one load's result reaches every caller it collapsed.
    /// Go's Aside returns a loader error as it is; a closed enum needs this to do the same.
    #[error("cache: loader: {0}")]
    Loader(#[source] std::sync::Arc<dyn std::error::Error + Send + Sync>),
}

/// Cache result using the crate's stable [`CacheError`] contract.
pub type Result<T> = std::result::Result<T, CacheError>;
