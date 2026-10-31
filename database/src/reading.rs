//! Pull-based reads and keyset pages, independent of backend transports.
use crate::{Error, Predicate, Range, Record, Resource, Result};
use futures::Stream;
use std::pin::Pin;

/// Records arrive only as the caller polls. Dropping cancels the read.
/// Backend documentation specifies the deadline and server batch buffering.
pub type RecordStream = Pin<Box<dyn Stream<Item = Result<Record>> + Send>>;

/// One page. Tokens are opaque continuation positions, not authorization or
/// snapshot handles. Inserts/updates during paging can change later results.
#[derive(Debug)]
pub struct Page {
    /// Ordered records, at most the requested page size.
    pub items: Vec<Record>,
    /// None when the server has no next page at query time.
    pub next_page_token: Option<String>,
}

/// Streaming and paged reads available independently of record CRUD.
#[async_trait::async_trait]
pub trait SeriesReader: Send + Sync {
    /// Opens a pull-based time window, with an optional total row limit. A dropped stream cancels transport.
    async fn stream(&self, resource: &Resource, range: &Scan) -> Result<RecordStream>;
    /// Reads a page using the same bounds, filters and order on every call.
    /// The descriptor must declare the table's complete tag set.
    async fn page(&self, resource: &Resource, range: &Range, token: Option<&str>) -> Result<Page>;
}

/// A time-bounded streaming query. Unlike a page, it has no row cap by default;
/// records are pulled in server batches, under the backend's stream deadline.
#[derive(Clone, Debug)]
pub struct Scan {
    /// Inclusive lower epoch-nanosecond bound.
    pub start: i64,
    /// Exclusive upper epoch-nanosecond bound.
    pub end: i64,
    /// Return newest timestamps first; tag tie-breakers remain ascending.
    pub descending: bool,
    /// Additional ANDed comparisons.
    pub predicates: Vec<Predicate>,
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
            predicates: Vec::new(),
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
