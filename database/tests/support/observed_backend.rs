//! Complete fake capability bundle; wrappers must preserve every one.
use super::common::record;
use database::store::{
    AggregateOptions, Batcher, Bucket, Driver, Error, HypertableOptions, ListOptions, ListResult,
    Migrator, RangeOptions, Record, RecordStream, Resource, Result, Scan, SeriesReader, TimeSeries,
    WriteResult,
};
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct Backend {
    pub calls: AtomicUsize,
    pub mode: u8,
}
impl Backend {
    pub fn new(mode: u8) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            mode,
        }
    }
    fn step(&self) -> Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.mode == 3 {
            return Err(Error::Invalid("secret-token SQL payload".into()));
        }
        Ok(())
    }
    fn page(&self) -> Result<ListResult> {
        self.step()?;
        Ok(ListResult {
            items: vec![record()],
            next_page_token: "secret-cursor".into(),
            total: 1,
        })
    }
}
#[async_trait::async_trait]
impl Driver for Backend {
    async fn create(&self, _: &Resource, value: &Record) -> Result<WriteResult> {
        self.step()?;
        Ok(WriteResult::done(value.clone()))
    }
    async fn get(&self, _: &Resource, _: &str) -> Result<Record> {
        self.step()?;
        Ok(record())
    }
    async fn update(&self, _: &Resource, value: &Record) -> Result<WriteResult> {
        self.step()?;
        Ok(WriteResult::done(value.clone()))
    }
    async fn delete(&self, _: &Resource, _: &str) -> Result<()> {
        self.step()
    }
    async fn list(&self, _: &Resource, _: &ListOptions) -> Result<ListResult> {
        self.page()
    }
    async fn count(&self, _: &Resource, _: &ListOptions) -> Result<i64> {
        self.step().map(|()| 1)
    }
    async fn exists(&self, _: &Resource, _: &str) -> Result<bool> {
        self.step().map(|()| true)
    }
}
#[async_trait::async_trait]
impl Batcher for Backend {
    async fn create_many(&self, _: &Resource, values: &[Record]) -> Result<Vec<WriteResult>> {
        self.step()?;
        Ok(values.iter().cloned().map(WriteResult::done).collect())
    }
    async fn get_many(&self, _: &Resource, _: &[String]) -> Result<Vec<Record>> {
        self.step().map(|()| vec![record()])
    }
}
#[async_trait::async_trait]
impl TimeSeries for Backend {
    async fn ensure_hypertable(&self, _: &Resource, _: &HypertableOptions) -> Result<()> {
        self.step()
    }
    async fn range(&self, _: &Resource, _: &RangeOptions) -> Result<ListResult> {
        self.page()
    }
    async fn aggregate(&self, _: &Resource, _: &AggregateOptions) -> Result<Vec<Bucket>> {
        self.step().map(|()| vec![Bucket::default()])
    }
}
#[async_trait::async_trait]
impl Migrator for Backend {
    async fn ensure_schema(&self, _: &Resource) -> Result<()> {
        self.step()
    }
    async fn drop_schema(&self, _: &Resource) -> Result<()> {
        self.step()
    }
    async fn has_schema(&self, _: &Resource) -> Result<bool> {
        self.step().map(|()| true)
    }
}
#[async_trait::async_trait]
impl SeriesReader for Backend {
    async fn stream(&self, _: &Resource, _: &Scan) -> Result<RecordStream> {
        self.step()?;
        let head = futures::stream::iter(vec![Ok(record())]);
        use futures::StreamExt;
        if self.mode == 1 {
            return Ok(Box::pin(head.chain(futures::stream::pending())));
        }
        if self.mode == 2 {
            return Ok(Box::pin(
                head.chain(futures::stream::once(async { Err(Error::Timeout) })),
            ));
        }
        Ok(Box::pin(head))
    }
}
