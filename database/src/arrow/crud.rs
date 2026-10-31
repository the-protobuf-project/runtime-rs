//! Captured rows through the CRUD, bulk, schema and release contracts. Rows
//! have no key, so keyed reads and writes are refused by name.

use super::ArrowDriver;
use super::errors::backend;
use crate::store::window::Range;
use crate::store::{
    Batcher, Driver, Error, ListOptions, ListResult, Migrator, Record, Release, Resource, Result,
    WriteResult, filter::predicates, page_size,
};

/// Every row ever captured, ordered as `opts.order_by` asks.
fn all_time(res: &Resource, opts: &ListOptions) -> Result<Range> {
    let time = res.time_column()?;
    let descending = match opts.order_by.split_whitespace().collect::<Vec<_>>()[..] {
        [] => false,
        [column] | [column, "asc"] if column == time => false,
        [column, "desc"] if column == time => true,
        _ => {
            return Err(Error::Invalid(format!(
                "arrow captures order only by {time}"
            )));
        }
    };
    let range = Range {
        start: i64::MIN,
        end: i64::MAX,
        limit: page_size(opts.page_size)?,
        descending,
        predicates: predicates(res, &opts.filter)?,
    };
    range.validate()?;
    Ok(range)
}

#[async_trait::async_trait]
impl Driver for ArrowDriver {
    async fn create(&self, res: &Resource, record: &Record) -> Result<WriteResult> {
        self.write(res, std::slice::from_ref(record)).await?;
        Ok(WriteResult::done(record.clone()))
    }
    async fn get(&self, _: &Resource, _: &str) -> Result<Record> {
        self.refuse("get rows by key; rows have no key")
    }
    async fn update(&self, _: &Resource, _: &Record) -> Result<WriteResult> {
        self.refuse("update rows; captures are append-only")
    }
    async fn delete(&self, _: &Resource, _: &str) -> Result<()> {
        self.refuse("delete rows; captures are append-only")
    }
    async fn list(&self, res: &Resource, opts: &ListOptions) -> Result<ListResult> {
        let mut page = self
            .page(res, &all_time(res, opts)?, &opts.page_token)
            .await?;
        if opts.omit_total {
            page.total = -1;
        }
        Ok(page)
    }
    async fn count(&self, res: &Resource, opts: &ListOptions) -> Result<i64> {
        let mut range = all_time(res, opts)?;
        range.limit = 1;
        Ok(self.page(res, &range, "").await?.total)
    }
    async fn exists(&self, _: &Resource, _: &str) -> Result<bool> {
        self.refuse("check rows by key; rows have no key")
    }
}

#[async_trait::async_trait]
impl Batcher for ArrowDriver {
    async fn create_many(&self, res: &Resource, records: &[Record]) -> Result<Vec<WriteResult>> {
        self.write(res, records).await?;
        Ok(records.iter().cloned().map(WriteResult::done).collect())
    }
    async fn get_many(&self, _: &Resource, _: &[String]) -> Result<Vec<Record>> {
        self.refuse("get rows by key; rows have no key")
    }
}

/// A table is a directory of segments.
#[async_trait::async_trait]
impl Migrator for ArrowDriver {
    async fn ensure_schema(&self, res: &Resource) -> Result<()> {
        res.time_column()?;
        tokio::fs::create_dir_all(self.table_dir(res.table()))
            .await
            .map_err(backend)
    }
    async fn drop_schema(&self, res: &Resource) -> Result<()> {
        self.close_table(res.table()).await?;
        match tokio::fs::remove_dir_all(self.table_dir(res.table())).await {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(backend(e)),
            _ => Ok(()),
        }
    }
    async fn has_schema(&self, res: &Resource) -> Result<bool> {
        let dir = self.table_dir(res.table());
        Ok(tokio::fs::metadata(&dir).await.is_ok_and(|m| m.is_dir()))
    }
}

#[async_trait::async_trait]
impl Release for ArrowDriver {
    /// Finishes every open segment with its end marker and bounds.
    async fn release(&self) -> Result<()> {
        self.close().await
    }
}
