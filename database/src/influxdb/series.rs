//! Windows and bucketed reductions through the TimeSeries contract.

use super::{InfluxDriver, aggregate};
use crate::store::window::{Aggregate, Range};
use crate::store::{
    AggregateOptions, Bucket, HypertableOptions, ListResult, RangeOptions, Resource, Result,
    TimeSeries,
};

#[async_trait::async_trait]
impl TimeSeries for InfluxDriver {
    /// Every InfluxDB table is already partitioned by time. Retention is a
    /// database property fixed at creation, so asking for one here is refused.
    async fn ensure_hypertable(&self, res: &Resource, opts: &HypertableOptions) -> Result<()> {
        self.validate_resource(res)?;
        if opts.retention.is_some() || opts.partitions > 0 || opts.chunk_interval.is_some() {
            return self.refuse("set retention or partitioning per table; use InfluxAdmin");
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

    /// Keyset pages ordered by time then every tag; `total` is -1 because
    /// counting a window costs a second scan of it.
    async fn range(&self, res: &Resource, opts: &RangeOptions) -> Result<ListResult> {
        self.validate_resource(res)?;
        let range = Range::of(res, opts)?;
        let (items, next_page_token) = self.page(res, &range, &opts.page_token).await?;
        Ok(ListResult {
            items,
            next_page_token,
            total: -1,
        })
    }

    async fn aggregate(&self, res: &Resource, opts: &AggregateOptions) -> Result<Vec<Bucket>> {
        self.validate_resource(res)?;
        let query = Aggregate::of(res, opts)?;
        let rows = self.query(aggregate::aggregate(res, &query)?).await?;
        query.buckets(res, rows)
    }
}
