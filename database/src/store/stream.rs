//! Pull-based time-window streams: a Rust extension beyond runtime-go.
//!
//! [`TimeSeries::range`](super::TimeSeries::range) pages a window; a stream
//! reads all of one without a row cap, pulling server batches as the caller
//! polls, for exports and replays too large for pages.
use super::{Error, Record, Resource, Result};
use futures::Stream;
use std::pin::Pin;

/// Records arrive only as the caller polls. Dropping cancels the read.
/// Backend documentation specifies the deadline and server batch buffering.
pub type RecordStream = Pin<Box<dyn Stream<Item = Result<Record>> + Send>>;

/// Streaming reads of a time window.
#[async_trait::async_trait]
pub trait SeriesReader: Send + Sync {
    /// Opens a pull-based window, with an optional total row limit.
    async fn stream(&self, resource: &Resource, scan: &Scan) -> Result<RecordStream>;
}

/// A time-bounded streaming query, with no row cap by default.
#[derive(Clone, Debug)]
pub struct Scan {
    /// Inclusive lower epoch-nanosecond bound.
    pub start: i64,
    /// Exclusive upper epoch-nanosecond bound.
    pub end: i64,
    /// Return newest timestamps first; tag tie-breakers remain ascending.
    pub descending: bool,
    /// AIP-160 conjunction applied inside the window.
    pub filter: String,
    /// Optional total row bound. None scans the entire selected time window.
    pub limit: Option<usize>,
}

impl Scan {
    /// Creates an oldest-first scan without a total row cap.
    pub fn new(start: i64, end: i64) -> Self {
        Self {
            start,
            end,
            descending: false,
            filter: String::new(),
            limit: None,
        }
    }
    /// Validates time bounds and an optional positive SQL-compatible row cap.
    pub fn validate(&self) -> Result<()> {
        if self.start >= self.end || self.limit.is_some_and(|n| n == 0 || n > i64::MAX as usize) {
            return Err(Error::Invalid(
                "scan needs start < end and an optional positive row limit".into(),
            ));
        }
        Ok(())
    }
}
