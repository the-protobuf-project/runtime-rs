//! Windows and bucketed reductions through the TimeSeries contract.

use super::{ArrowDriver, aggregate, filter};
use crate::store::window::{Aggregate, Range};
use crate::store::{
    AggregateOptions, Bucket, HypertableOptions, ListResult, RangeOptions, Resource, Result,
    TimeSeries,
};

#[async_trait::async_trait]
impl TimeSeries for ArrowDriver {
    /// Segments already partition a table by time; retention and space
    /// partitioning are refused rather than ignored.
    async fn ensure_hypertable(&self, res: &Resource, opts: &HypertableOptions) -> Result<()> {
        if opts.retention.is_some() || opts.partitions > 0 || opts.chunk_interval.is_some() {
            return self.refuse("set retention or partitioning; rotate segments instead");
        }
        Range::of(
            res,
            &RangeOptions {
                time_column: opts.time_column.clone(),
                start: 0,
                end: 1,
                ..RangeOptions::default()
            },
        )
        .map(|_| ())
    }

    async fn range(&self, res: &Resource, opts: &RangeOptions) -> Result<ListResult> {
        let range = Range::of(res, opts)?;
        filter::validate(res, &range)?;
        self.page(res, &range, &opts.page_token).await
    }

    async fn aggregate(&self, res: &Resource, opts: &AggregateOptions) -> Result<Vec<Bucket>> {
        let query = Aggregate::of(res, opts)?;
        filter::validate(res, &query.range)?;
        aggregate::validate(res, &query)?;
        let reducer = aggregate::Reducer::new(res, &query)?;
        let (resource, range) = (res.clone(), query.range.clone());
        let rows = self
            .window(res, &query.range, reducer, move |reducer, rows| {
                for row in rows.iter().filter(|r| filter::keep(&resource, &range, r)) {
                    reducer.add(row);
                }
            })
            .await?
            .finish();
        query.buckets(res, rows)
    }
}
