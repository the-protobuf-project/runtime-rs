//! Bounded operation classifications shared by runtime packages.

/// Outcome labels contain no caller-provided text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Completed successfully.
    Ok,
    /// Requested record is absent.
    NotFound,
    /// A uniqueness/precondition conflict.
    Conflict,
    /// Capability is unavailable.
    Unsupported,
    /// Invalid input.
    Invalid,
    /// Operation exceeded its deadline.
    Timeout,
    /// Future or stream was dropped before completion.
    Cancelled,
    /// Operation unwound through a panic.
    Panic,
    /// A closed component refused work.
    Closed,
    /// Backend/transport/decoding failure.
    Error,
}

impl Outcome {
    /// Stable label used consistently by logs, metrics and spans.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::Unsupported => "unsupported",
            Self::Invalid => "invalid",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::Panic => "panic",
            Self::Closed => "closed",
            Self::Error => "error",
        }
    }

    /// Expected absence/conflicts/cancellation do not mark a span as failed.
    pub fn is_error(self) -> bool {
        matches!(
            self,
            Self::Invalid
                | Self::Timeout
                | Self::Panic
                | Self::Closed
                | Self::Error
                | Self::Unsupported
        )
    }
}
