//! Series and lifecycle capability wrappers share one observation policy.
use super::{Observed, run};
use crate::{
    Aggregate, Buckets, DeleteRange, Range, Record, Resource, Result, RowDeletion,
    SeriesMaintenance, TimeSeries,
};

#[async_trait::async_trait]
impl TimeSeries for Observed<dyn TimeSeries> {
    async fn write(&self, res: &Resource, records: &[Record]) -> Result<()> {
        run(
            &self.observer,
            self.backend,
            "write",
            self.next.write(res, records),
            records.len(),
            |_| 0,
        )
        .await
    }
    async fn range(&self, res: &Resource, range: &Range) -> Result<Vec<Record>> {
        run(
            &self.observer,
            self.backend,
            "range",
            self.next.range(res, range),
            0,
            Vec::len,
        )
        .await
    }
    async fn aggregate(&self, res: &Resource, query: &Aggregate) -> Result<Buckets> {
        run(
            &self.observer,
            self.backend,
            "aggregate",
            self.next.aggregate(res, query),
            0,
            Vec::len,
        )
        .await
    }
}

#[async_trait::async_trait]
impl SeriesMaintenance for Observed<dyn SeriesMaintenance> {
    async fn drop_table(&self, res: &Resource) -> Result<()> {
        run(
            &self.observer,
            self.backend,
            "drop_table",
            self.next.drop_table(res),
            0,
            |_| 0,
        )
        .await
    }
}

#[async_trait::async_trait]
impl RowDeletion for Observed<dyn RowDeletion> {
    async fn delete_range(&self, res: &Resource, range: &DeleteRange) -> Result<()> {
        run(
            &self.observer,
            self.backend,
            "delete_range",
            self.next.delete_range(res, range),
            0,
            |_| 0,
        )
        .await
    }
}
