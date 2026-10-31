//! Arrow and filesystem error translation kept at the backend boundary.

use crate::store::Error;

/// Backend name reported in errors, diagnostics and telemetry.
pub(super) const BACKEND: &str = "arrow";

pub(super) fn backend(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::Backend {
        backend: BACKEND,
        source: Box::new(error),
    }
}

pub(super) fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}

/// A panicked or cancelled blocking file task.
pub(super) fn joined(error: tokio::task::JoinError) -> Error {
    backend(error)
}
