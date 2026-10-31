//! CRUD and bulk observations, one operation guard per awaited call.
use super::{Observed, run};
use crate::store::{
    Batcher, Driver, ListOptions, ListResult, Record, Resource, Result, WriteResult,
};

macro_rules! observe {
    ($self:ident, $name:literal, $call:expr, $written:expr, $read:expr) => {
        run(
            &$self.observer,
            $self.backend,
            $name,
            $call,
            $written,
            $read,
        )
        .await
    };
}

#[async_trait::async_trait]
impl Driver for Observed<dyn Driver> {
    async fn create(&self, res: &Resource, record: &Record) -> Result<WriteResult> {
        observe!(self, "create", self.next.create(res, record), 1, |_| 0)
    }
    async fn get(&self, res: &Resource, key: &str) -> Result<Record> {
        observe!(self, "get", self.next.get(res, key), 0, |_| 1)
    }
    async fn update(&self, res: &Resource, record: &Record) -> Result<WriteResult> {
        observe!(self, "update", self.next.update(res, record), 1, |_| 0)
    }
    async fn delete(&self, res: &Resource, key: &str) -> Result<()> {
        observe!(self, "delete", self.next.delete(res, key), 1, |_| 0)
    }
    async fn list(&self, res: &Resource, opts: &ListOptions) -> Result<ListResult> {
        observe!(self, "list", self.next.list(res, opts), 0, |r| r
            .items
            .len())
    }
    async fn count(&self, res: &Resource, opts: &ListOptions) -> Result<i64> {
        observe!(self, "count", self.next.count(res, opts), 0, |_| 0)
    }
    async fn exists(&self, res: &Resource, key: &str) -> Result<bool> {
        observe!(self, "exists", self.next.exists(res, key), 0, |_| 0)
    }
}

#[async_trait::async_trait]
impl Batcher for Observed<dyn Batcher> {
    async fn create_many(&self, res: &Resource, records: &[Record]) -> Result<Vec<WriteResult>> {
        let written = records.len();
        observe!(
            self,
            "create_many",
            self.next.create_many(res, records),
            written,
            |_| 0
        )
    }
    async fn get_many(&self, res: &Resource, keys: &[String]) -> Result<Vec<Record>> {
        observe!(self, "get_many", self.next.get_many(res, keys), 0, Vec::len)
    }
}
