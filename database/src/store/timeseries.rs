//! Time partitioning, windows and bucketed reductions, as runtime-go.
//!
//! Writes are ordinary [`Driver::create`](super::Driver::create) and
//! [`Batcher::create_many`](super::Batcher::create_many) calls; this trait adds
//! only what is specific to time. Times are signed epoch nanoseconds, the same
//! representation [`Kind::Timestamp`](super::Kind::Timestamp) columns hold.

use super::{ListResult, Resource, Result};
use std::collections::BTreeMap;
use std::time::Duration;

/// Partitions a resource by time, reads a window of it and reduces it.
#[async_trait::async_trait]
pub trait TimeSeries: Send + Sync {
    /// Prepares the resource for time-partitioned storage.
    async fn ensure_hypertable(&self, res: &Resource, opts: &HypertableOptions) -> Result<()>;
    /// One page of the half-open window [start, end), ordered by time.
    async fn range(&self, res: &Resource, opts: &RangeOptions) -> Result<ListResult>;
    /// Fixed-width buckets over the window, reduced on the backend.
    async fn aggregate(&self, res: &Resource, opts: &AggregateOptions) -> Result<Vec<Bucket>>;
}

/// How [`TimeSeries::ensure_hypertable`] partitions a resource.
#[derive(Clone, Debug, Default)]
pub struct HypertableOptions {
    /// Partitioning column; empty means the resource's timestamp column.
    pub time_column: String,
    /// Width of each time partition, when the backend lets callers choose.
    pub chunk_interval: Option<Duration>,
    /// Optional space-partitioning column.
    pub partition_column: String,
    /// Number of space partitions; 0 for none.
    pub partitions: i32,
    /// Drop data older than this, when the backend can.
    pub retention: Option<Duration>,
}

/// A window read by [`TimeSeries::range`].
#[derive(Clone, Debug, Default)]
pub struct RangeOptions {
    /// Window column; empty means the resource's timestamp column.
    pub time_column: String,
    /// Inclusive lower bound, epoch nanoseconds.
    pub start: i64,
    /// Exclusive upper bound, epoch nanoseconds.
    pub end: i64,
    /// Newest first when true.
    pub descending: bool,
    /// AIP-160 conjunction applied inside the window.
    pub filter: String,
    /// Records per page; 0 or less means the default of 50.
    pub page_size: i32,
    /// Opaque token from the previous page.
    pub page_token: String,
}

/// A reduction read by [`TimeSeries::aggregate`].
#[derive(Clone, Debug, Default)]
pub struct AggregateOptions {
    /// Bucketing column; empty means the resource's timestamp column.
    pub time_column: String,
    /// Inclusive lower bound, epoch nanoseconds.
    pub start: i64,
    /// Exclusive upper bound, epoch nanoseconds.
    pub end: i64,
    /// Bucket width, aligned to the Unix epoch. Whole seconds.
    pub every: Duration,
    /// One or more reductions.
    pub reduce: Vec<Reduction>,
    /// Tag columns each bucket is also grouped by.
    pub group_by: Vec<String>,
    /// AIP-160 conjunction applied before reducing.
    pub filter: String,
    /// Maximum buckets; 0 means the default of 1000.
    pub limit: usize,
}

/// Reduction function.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reducer {
    /// Number of values.
    Count,
    /// Sum.
    Sum,
    /// Arithmetic mean.
    Avg,
    /// Minimum.
    Min,
    /// Maximum.
    Max,
}

/// One reduced column and the name its result is reported under.
#[derive(Clone, Debug)]
pub struct Reduction {
    /// Function applied.
    pub func: Reducer,
    /// Numeric field reduced.
    pub column: String,
    /// Output name, runtime-go's `As`.
    pub alias: String,
}

impl Reduction {
    /// Reduces `column` with `func`, reported as `alias`.
    pub fn new(func: Reducer, column: impl Into<String>, alias: impl Into<String>) -> Self {
        Self {
            func,
            column: column.into(),
            alias: alias.into(),
        }
    }
}

/// One bucket: its start, its group's tag values and each reduction's value.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Bucket {
    /// Bucket start, epoch nanoseconds.
    pub start: i64,
    /// Group-by tag values, empty without group_by.
    pub group: BTreeMap<String, String>,
    /// Reduction results by alias.
    pub values: BTreeMap<String, f64>,
}

/// Default bucket limit when a caller names none, as runtime-go's timescale.
pub const DEFAULT_BUCKET_LIMIT: usize = 1000;
