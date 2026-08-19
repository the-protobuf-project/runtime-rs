//! Cache error types

use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum CacheError {
    #[error("cache: not found")]
    NotFound,

    #[error("cache: unsupported by this backend")]
    Unsupported,

    #[error("cache: no expiry, and this cache requires one")]
    NoTTL,

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

    #[error("cache: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, CacheError>;
