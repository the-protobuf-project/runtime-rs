//! Backend-neutral storage-operation timeout policy.
//!
//! Connection setup and command execution fail independently. [`DialTimeout`]
//! expresses the first concern; this module gives drivers the same explicit
//! three-state intent for reads, writes, and backend batches.

use std::time::Duration;

/// Selects how a backend bounds one storage operation or batch exchange.
///
/// Drivers map [`OperationTimeout::Default`] to their documented backend
/// policy, [`OperationTimeout::Disabled`] to no client-side bound, and
/// [`OperationTimeout::After`] to the supplied positive duration. A zero
/// `After` is invalid and must be rejected before network I/O.
///
/// **Cost and side effects**: This is configuration only. Copying it performs
/// no I/O and does not start a timer until a driver executes an operation.
///
/// [`DialTimeout`]: crate::DialTimeout
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OperationTimeout {
    /// Uses the driver's documented default operation timeout.
    #[default]
    Default,
    /// Explicitly disables the client-side operation timeout.
    Disabled,
    /// Bounds each operation by the supplied positive duration.
    After(Duration),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_timeout_default_preserves_driver_policy() {
        assert_eq!(OperationTimeout::default(), OperationTimeout::Default);
    }

    #[test]
    fn test_operation_timeout_variants_preserve_caller_intent() {
        assert_eq!(OperationTimeout::Disabled, OperationTimeout::Disabled);
        assert_eq!(
            OperationTimeout::After(Duration::from_secs(3)),
            OperationTimeout::After(Duration::from_secs(3))
        );
    }
}
