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

    #[error("cache: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, CacheError>;
