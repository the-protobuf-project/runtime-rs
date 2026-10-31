//! Stands in for every capability a backend lacks, naming backend and operation.

use super::{
    AggregateOptions, Batcher, Bucket, Driver, Error, HypertableOptions, ListOptions, ListResult,
    Migrator, RangeOptions, Record, RecordStream, Resource, Result, Scan, SeriesReader, TimeSeries,
    Transactional, TxWork, WriteResult,
};

pub(super) struct Refuses(pub(super) &'static str);

impl Refuses {
    fn no<T>(&self, capability: &'static str) -> Result<T> {
        Err(Error::Unimplemented {
            backend: self.0,
            capability,
        })
    }
}

#[async_trait::async_trait]
impl Driver for Refuses {
    async fn create(&self, _: &Resource, _: &Record) -> Result<WriteResult> {
        self.no("create records")
    }
    async fn get(&self, _: &Resource, _: &str) -> Result<Record> {
        self.no("get records by key")
    }
    async fn update(&self, _: &Resource, _: &Record) -> Result<WriteResult> {
        self.no("update records")
    }
    async fn delete(&self, _: &Resource, _: &str) -> Result<()> {
        self.no("delete records by key")
    }
    async fn list(&self, _: &Resource, _: &ListOptions) -> Result<ListResult> {
        self.no("list records")
    }
    async fn count(&self, _: &Resource, _: &ListOptions) -> Result<i64> {
        self.no("count records")
    }
    async fn exists(&self, _: &Resource, _: &str) -> Result<bool> {
        self.no("check records by key")
    }
}

#[async_trait::async_trait]
impl Transactional for Refuses {
    async fn run(&self, _: TxWork) -> Result<()> {
        self.no("run a transaction")
    }
}

#[async_trait::async_trait]
impl Migrator for Refuses {
    async fn ensure_schema(&self, _: &Resource) -> Result<()> {
        self.no("create schema")
    }
    async fn drop_schema(&self, _: &Resource) -> Result<()> {
        self.no("drop schema")
    }
    async fn has_schema(&self, _: &Resource) -> Result<bool> {
        self.no("inspect schema")
    }
}

#[async_trait::async_trait]
impl Batcher for Refuses {
    async fn create_many(&self, _: &Resource, _: &[Record]) -> Result<Vec<WriteResult>> {
        self.no("create records in bulk")
    }
    async fn get_many(&self, _: &Resource, _: &[String]) -> Result<Vec<Record>> {
        self.no("get records in bulk")
    }
}

#[async_trait::async_trait]
impl TimeSeries for Refuses {
    async fn ensure_hypertable(&self, _: &Resource, _: &HypertableOptions) -> Result<()> {
        self.no("partition by time")
    }
    async fn range(&self, _: &Resource, _: &RangeOptions) -> Result<ListResult> {
        self.no("read a time window")
    }
    async fn aggregate(&self, _: &Resource, _: &AggregateOptions) -> Result<Vec<Bucket>> {
        self.no("aggregate a time window")
    }
}

#[async_trait::async_trait]
impl SeriesReader for Refuses {
    async fn stream(&self, _: &Resource, _: &Scan) -> Result<RecordStream> {
        self.no("stream a time window")
    }
}
