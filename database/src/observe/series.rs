//! Time-series and schema observations share one policy.
use super::{Observed, run};
use crate::store::{
    AggregateOptions, Bucket, HypertableOptions, ListResult, Migrator, RangeOptions, Resource,
    Result, TimeSeries,
};

macro_rules! observe {
    ($self:ident, $name:literal, $call:expr, $read:expr) => {
        run(&$self.observer, $self.backend, $name, $call, 0, $read).await
    };
}

#[async_trait::async_trait]
impl TimeSeries for Observed<dyn TimeSeries> {
    async fn ensure_hypertable(&self, res: &Resource, opts: &HypertableOptions) -> Result<()> {
        observe!(
            self,
            "ensure_hypertable",
            self.next.ensure_hypertable(res, opts),
            |_| 0
        )
    }
    async fn range(&self, res: &Resource, opts: &RangeOptions) -> Result<ListResult> {
        observe!(self, "range", self.next.range(res, opts), |r| r.items.len())
    }
    async fn aggregate(&self, res: &Resource, opts: &AggregateOptions) -> Result<Vec<Bucket>> {
        observe!(self, "aggregate", self.next.aggregate(res, opts), Vec::len)
    }
}

#[async_trait::async_trait]
impl Migrator for Observed<dyn Migrator> {
    async fn ensure_schema(&self, res: &Resource) -> Result<()> {
        observe!(self, "ensure_schema", self.next.ensure_schema(res), |_| 0)
    }
    async fn drop_schema(&self, res: &Resource) -> Result<()> {
        observe!(self, "drop_schema", self.next.drop_schema(res), |_| 0)
    }
    async fn has_schema(&self, res: &Resource) -> Result<bool> {
        observe!(self, "has_schema", self.next.has_schema(res), |_| 0)
    }
}
