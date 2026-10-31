//! Every write to two databases at once; reads from the first.
//!
//! [`mirror`] keeps a primary store (InfluxDB) and a copy (an Arrow capture)
//! in step: create, update, delete, create_many and schema changes run on
//! both concurrently, and a write succeeds only when both do. A failure
//! reports the primary's error first. A retried batch can repeat rows in the
//! copy, while InfluxDB merges a rewritten point, so prefer retrying whole
//! batches over dropping them. Reads, time windows, streams and transactions
//! use the primary only.

use crate::store::{
    Batcher, Capabilities, DB, Driver, ListOptions, ListResult, Migrator, Record, Release,
    Resource, Result, WriteResult,
};
use std::sync::Arc;

/// Mirrors writes from `primary` to `copy`. Closing the result closes both.
pub fn mirror(primary: DB, copy: DB) -> DB {
    let both = Arc::new(Mirror {
        primary: primary.clone(),
        copy,
    });
    let mut out = DB::build(
        primary.backend,
        primary.name.clone(),
        Capabilities {
            driver: Some(both.clone()),
            batch: Some(both.clone()),
            schema: Some(both.clone()),
            release: Some(both),
            tx: Some(primary.tx.clone()),
            series: Some(primary.series.clone()),
            streams: Some(primary.streams.clone()),
        },
    );
    out.telemetry = primary.telemetry.clone();
    out
}

struct Mirror {
    primary: DB,
    copy: DB,
}

/// The primary's result, failing when either side failed.
fn both<T, U>(primary: Result<T>, copy: Result<U>) -> Result<T> {
    let value = primary?;
    copy?;
    Ok(value)
}

#[async_trait::async_trait]
impl Driver for Mirror {
    async fn create(&self, res: &Resource, record: &Record) -> Result<WriteResult> {
        let (a, b) = tokio::join!(
            self.primary.create(res, record),
            self.copy.create(res, record)
        );
        both(a, b)
    }
    async fn get(&self, res: &Resource, key: &str) -> Result<Record> {
        self.primary.get(res, key).await
    }
    async fn update(&self, res: &Resource, record: &Record) -> Result<WriteResult> {
        let (a, b) = tokio::join!(
            self.primary.update(res, record),
            self.copy.update(res, record)
        );
        both(a, b)
    }
    async fn delete(&self, res: &Resource, key: &str) -> Result<()> {
        let (a, b) = tokio::join!(self.primary.delete(res, key), self.copy.delete(res, key));
        both(a, b)
    }
    async fn list(&self, res: &Resource, opts: &ListOptions) -> Result<ListResult> {
        self.primary.list(res, opts).await
    }
    async fn count(&self, res: &Resource, opts: &ListOptions) -> Result<i64> {
        self.primary.count(res, opts).await
    }
    async fn exists(&self, res: &Resource, key: &str) -> Result<bool> {
        self.primary.exists(res, key).await
    }
}

#[async_trait::async_trait]
impl Batcher for Mirror {
    async fn create_many(&self, res: &Resource, records: &[Record]) -> Result<Vec<WriteResult>> {
        let (a, b) = tokio::join!(
            self.primary.batch.create_many(res, records),
            self.copy.batch.create_many(res, records)
        );
        both(a, b)
    }
    async fn get_many(&self, res: &Resource, keys: &[String]) -> Result<Vec<Record>> {
        self.primary.batch.get_many(res, keys).await
    }
}

#[async_trait::async_trait]
impl Migrator for Mirror {
    async fn ensure_schema(&self, res: &Resource) -> Result<()> {
        let (a, b) = tokio::join!(
            self.primary.schema.ensure_schema(res),
            self.copy.schema.ensure_schema(res)
        );
        both(a, b)
    }
    async fn drop_schema(&self, res: &Resource) -> Result<()> {
        let (a, b) = tokio::join!(
            self.primary.schema.drop_schema(res),
            self.copy.schema.drop_schema(res)
        );
        both(a, b)
    }
    async fn has_schema(&self, res: &Resource) -> Result<bool> {
        self.primary.schema.has_schema(res).await
    }
}

#[async_trait::async_trait]
impl Release for Mirror {
    async fn release(&self) -> Result<()> {
        let (a, b) = tokio::join!(self.primary.close(), self.copy.close());
        both(a, b)
    }
}
