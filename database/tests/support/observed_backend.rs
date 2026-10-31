//! Complete fake capability bundle; wrappers must preserve every one.
use super::common::record;
use database::{
    Aggregate, Buckets, DeleteRange, Error, Page, Range, Record, RecordStore, RecordStream,
    Resource, Result, RowDeletion, Scan, SeriesMaintenance, SeriesReader, TimeSeries,
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
}
#[async_trait::async_trait]
impl TimeSeries for Backend {
    async fn write(&self, _: &Resource, _: &[Record]) -> Result<()> {
        self.step()
    }
    async fn range(&self, _: &Resource, _: &Range) -> Result<Vec<Record>> {
        self.step()?;
        Ok(vec![record()])
    }
    async fn aggregate(&self, _: &Resource, _: &Aggregate) -> Result<Buckets> {
        self.step()?;
        Ok(vec![record()])
    }
}
#[async_trait::async_trait]
impl RecordStore for Backend {
    async fn create(&self, _: &Resource, value: &Record) -> Result<Record> {
        self.step()?;
        Ok(value.clone())
    }
    async fn get(&self, _: &Resource, _: &str) -> Result<Record> {
        self.step()?;
        Ok(record())
    }
    async fn update(&self, _: &Resource, value: &Record) -> Result<Record> {
        self.step()?;
        Ok(value.clone())
    }
    async fn delete(&self, _: &Resource, _: &str) -> Result<()> {
        self.step()
    }
}
#[async_trait::async_trait]
impl SeriesMaintenance for Backend {
    async fn drop_table(&self, _: &Resource) -> Result<()> {
        self.step()
    }
}
#[async_trait::async_trait]
impl RowDeletion for Backend {
    async fn delete_range(&self, _: &Resource, _: &DeleteRange) -> Result<()> {
        self.step()
    }
}
#[async_trait::async_trait]
impl SeriesReader for Backend {
    async fn page(&self, _: &Resource, _: &Range, _: Option<&str>) -> Result<Page> {
        self.step()?;
        Ok(Page {
            items: vec![record()],
            next_page_token: Some("secret-cursor".into()),
        })
    }
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
