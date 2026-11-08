//! Stream error types

use thiserror::Error;

/// Stable failures exposed by stream providers, codecs, and wrappers.
///
/// Variants distinguish a missing stream, a subject the stream never declared,
/// a settled capability gap, payload encoding, a partially published batch,
/// and contextual backend failures. Callers match semantic variants for control
/// flow and use formatted messages only for diagnostics.
///
/// **Trade-offs**: One closed error type keeps the object-safe contracts
/// uniform, at the cost of folding backend-specific errors into
/// [`StreamError::Internal`] context.
#[derive(Clone, Debug, Error)]
pub enum StreamError {
    /// No stream exists under the named id.
    #[error("streams: not found: {0}")]
    NotFound(String),

    /// Publishing or subscribing named a subject the stream does not declare.
    ///
    /// Subjects are fixed at creation so a typo fails loudly at the call that
    /// made it, instead of silently producing a topic nobody reads.
    #[error(
        "streams: subject not declared by this stream: {subject:?} (stream {stream} declares {declared:?})"
    )]
    UnknownSubject {
        /// Stream the call was bound to.
        stream: String,
        /// Subject the caller named.
        subject: String,
        /// Subjects the stream was created with.
        declared: Vec<String>,
    },

    /// The provider cannot do what was asked, and retrying cannot change that.
    ///
    /// It is a settled answer rather than a transient failure, so
    /// [`with_retry`](crate::with_retry) gives up on it immediately.
    #[error("streams: unsupported by this provider: {0}")]
    Unsupported(String),

    /// A value could not be encoded, or a payload could not be decoded.
    #[error("streams: {0}")]
    Codec(String),

    /// A received payload was not a frame this contract wrote.
    #[error("streams: message on {subject:?} {reason}")]
    Frame {
        /// Subject the payload arrived on.
        subject: String,
        /// What was wrong with it.
        reason: String,
    },

    /// Some entries of a batch failed to publish.
    ///
    /// `ids` always has one entry per value, in order, including the failed
    /// ones: every id is assigned before anything is sent. A failed entry may
    /// still have landed, because that is what at-least-once means.
    #[error("streams: {} of {total} entries failed to publish on {subject:?}: {}", failures.len(), summarize(failures))]
    PartialBatch {
        /// Subject the batch was published on.
        subject: String,
        /// Number of values in the batch.
        total: usize,
        /// The id assigned to every entry, published or not.
        ids: Vec<String>,
        /// Index and failure of each entry that did not publish.
        failures: Vec<(usize, StreamError)>,
    },

    /// A backend, lifecycle, or invariant failure with safe context.
    #[error("streams: {0}")]
    Internal(String),
}

impl StreamError {
    /// Whether retrying the same call could succeed.
    ///
    /// An undeclared subject and an unsupported request are settled answers:
    /// the stream will not grow the subject and the provider will not grow the
    /// capability between attempts.
    pub fn is_settled(&self) -> bool {
        matches!(
            self,
            Self::UnknownSubject { .. } | Self::Unsupported(_) | Self::Codec(_)
        )
    }

    /// Wraps any backend error as [`StreamError::Internal`] with context.
    pub fn internal(context: &str, error: impl std::fmt::Display) -> Self {
        Self::Internal(format!("{context}: {error}"))
    }
}

/// Renders batch failures as `entry N: reason` pairs.
fn summarize(failures: &[(usize, StreamError)]) -> String {
    failures
        .iter()
        .map(|(index, error)| format!("entry {index}: {error}"))
        .collect::<Vec<_>>()
        .join("; ")
}

/// Stream result using the crate's stable [`StreamError`] contract.
pub type Result<T> = std::result::Result<T, StreamError>;
