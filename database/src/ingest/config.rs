//! Bounds and scheduling policy for background ingestion.
use crate::{Error, Result};
use std::time::Duration;

/// Queue capacity bounds admitted records; record bytes are bounded separately.
/// The queue is in memory, not a durable spool. Explicit close drains it.
#[derive(Clone, Debug)]
pub struct IngestConfig {
    /// Maximum points per server write, in 1..=10,000.
    pub batch_size: usize,
    /// Maximum queued commands/points; producers await capacity when full.
    pub queue_capacity: usize,
    /// Flush partially filled batches at this interval, even without new writes.
    pub flush_interval: Duration,
    /// Complete budget for each backend batch operation.
    pub write_timeout: Duration,
    /// Maximum serialized JSON bytes per admitted point (before SDK encoding).
    pub max_record_bytes: usize,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            batch_size: 500,
            queue_capacity: 1024,
            flush_interval: Duration::from_secs(1),
            write_timeout: Duration::from_secs(30),
            max_record_bytes: 65_536,
        }
    }
}

impl IngestConfig {
    pub(super) fn validate(&self) -> Result<()> {
        if !(1..=10_000).contains(&self.batch_size)
            || self.queue_capacity == 0
            || self.max_record_bytes == 0
            || self.flush_interval.is_zero()
            || self.write_timeout.is_zero()
            || self.flush_interval > Duration::from_secs(86_400)
            || self.write_timeout > Duration::from_secs(86_400)
        {
            return Err(Error::Invalid(
                "ingestion needs positive capacities, batch <=10000, and deadlines <=24h".into(),
            ));
        }
        Ok(())
    }
}
