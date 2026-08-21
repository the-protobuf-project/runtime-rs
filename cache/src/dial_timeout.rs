//! Backend-neutral connection-attempt timeout policy.
//!
//! Driver clients have different native timeout APIs, but callers should not
//! have to relearn what a zero duration means for each backend. This module
//! defines one intent-level contract; every driver maps it to its own client.

use std::time::Duration;

/// Selects how a backend client bounds each dial or reconnection attempt.
///
/// This type is shared by Redis and future Memcached or other driver
/// configurations. It distinguishes retaining a driver's native default,
/// explicitly removing the bound, and selecting a positive duration.
///
/// **Driver requirement**: [`DialTimeout::After`] containing [`Duration::ZERO`]
/// is invalid and must be rejected before network I/O. A caller must use
/// [`DialTimeout::Default`] or [`DialTimeout::Disabled`] to state the intended
/// zero-like policy.
///
/// **Cost and side effects**: This value is configuration only. Constructing or
/// copying it performs no I/O; the consuming driver applies it while connecting.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DialTimeout {
    /// Retains the backend client's native default dial timeout.
    #[default]
    Default,
    /// Explicitly disables the dial-attempt timeout.
    Disabled,
    /// Uses the supplied positive duration for each dial attempt.
    After(Duration),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dial_timeout_default_preserves_driver_policy() {
        assert_eq!(DialTimeout::default(), DialTimeout::Default);
    }

    #[test]
    fn test_dial_timeout_variants_preserve_caller_intent() {
        assert_eq!(DialTimeout::Disabled, DialTimeout::Disabled);
        assert_eq!(
            DialTimeout::After(Duration::from_secs(3)),
            DialTimeout::After(Duration::from_secs(3))
        );
    }
}
