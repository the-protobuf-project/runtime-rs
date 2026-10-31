//! Record-store observation remains available for future SQL/document drivers.
use super::{Observed, run};
use crate::{Record, RecordStore, Resource, Result};

#[async_trait::async_trait]
impl RecordStore for Observed<dyn RecordStore> {
    async fn create(&self, res: &Resource, record: &Record) -> Result<Record> {
        run(
            &self.observer,
            self.backend,
            "create",
            self.next.create(res, record),
            1,
            |_| 0,
        )
        .await
    }
    async fn get(&self, res: &Resource, key: &str) -> Result<Record> {
        run(
            &self.observer,
            self.backend,
            "get",
            self.next.get(res, key),
            0,
            |_| 1,
        )
        .await
    }
    async fn update(&self, res: &Resource, record: &Record) -> Result<Record> {
        run(
            &self.observer,
            self.backend,
            "update",
            self.next.update(res, record),
            1,
            |_| 0,
        )
        .await
    }
    async fn delete(&self, res: &Resource, key: &str) -> Result<()> {
        run(
            &self.observer,
            self.backend,
            "delete",
            self.next.delete(res, key),
            1,
            |_| 0,
        )
        .await
    }
}
