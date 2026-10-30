//! Stable error classifications shared by database backends.

/// Result returned by database operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors callers can match without parsing backend messages.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Administrative endpoint rejected a request. No response secrets are echoed.
    #[error("InfluxDB management returned HTTP {status}")]
    HttpStatus {
        /// HTTP response status.
        status: u16,
    },
    /// The background ingestion queue is no longer accepting observations.
    #[error("batch writer is closed")]
    Closed,
    /// A background batch failed; previously admitted points may be uncommitted.
    #[error("background ingestion failed: {0}")]
    Ingestion(#[source] std::sync::Arc<Error>),
    /// Descriptor, record, or query violates the portable contract.
    #[error("invalid database input: {0}")]
    Invalid(String),
    /// A requested record does not exist.
    #[error("record not found")]
    NotFound,
    /// An insert-only write conflicts with an existing record.
    #[error("record already exists")]
    AlreadyExists,
    /// The selected backend has no implementation of this capability.
    #[error("{backend} does not support {capability}")]
    Unsupported {
        /// Backend implementation name.
        backend: &'static str,
        /// Missing capability name.
        capability: &'static str,
    },
    /// The operation exceeded its complete wall-clock budget.
    /// A write may have reached the server before this error.
    #[error("database operation timed out; write outcome may be unknown")]
    Timeout,
    /// Caller data could not be serialized or decoded.
    #[error("record encoding: {0}")]
    Encoding(#[from] serde_json::Error),
    /// Backend-specific failure; the source remains available for diagnostics.
    #[error("{backend}: {source}")]
    Backend {
        /// Backend implementation name.
        backend: &'static str,
        /// Original SDK failure, including partial-write details.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}
