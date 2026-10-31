//! Create through a background batch writer, reported as a pending write.
//!
//! Generated code calls `create` once per message. At tens of thousands of
//! points a second that cannot be one request each, so [`buffered`] admits
//! each record into a bounded per-table queue and returns at once with
//! `pending: true`, runtime-go's signal for an accepted but not yet durable
//! write. Batches reach the wrapped DB's `create_many`. Producers wait when a
//! queue is full, so a slow backend slows them rather than growing memory.
//!
//! [`DB::close`] flushes every queue and reports the first failed batch. A
//! batch failure also stops that table's queue, and later creates return it.
//! Everything except `create` passes straight through, including
//! `create_many`, which is already a batch.

use crate::store::{
    Capabilities, DB, Driver, Error, ListOptions, ListResult, Record, Release, Resource, Result,
    WriteResult,
};
use crate::{BatchWriter, IngestConfig};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Wraps `db` so `create` is queued. Instrument `db` first, not the result.
pub fn buffered(db: DB, config: IngestConfig) -> Result<DB> {
    config.validate()?;
    let buffer = Arc::new(Buffer {
        inner: db.clone(),
        config,
        writers: Mutex::default(),
        closed: AtomicBool::new(false),
    });
    let mut out = DB::build(
        db.backend,
        db.name.clone(),
        Capabilities {
            driver: Some(buffer.clone()),
            tx: Some(db.tx.clone()),
            schema: Some(db.schema.clone()),
            batch: Some(db.batch.clone()),
            series: Some(db.series.clone()),
            streams: Some(db.streams.clone()),
            release: Some(buffer),
        },
    );
    out.telemetry = db.telemetry.clone();
    Ok(out)
}

fn same_columns(a: &Resource, b: &Resource) -> bool {
    a.columns().len() == b.columns().len()
        && a.columns()
            .iter()
            .zip(b.columns())
            .all(|(x, y)| x.name == y.name && x.kind == y.kind && x.role == y.role)
}

struct Buffer {
    inner: DB,
    config: IngestConfig,
    writers: Mutex<HashMap<String, Arc<BatchWriter>>>,
    closed: AtomicBool,
}

impl Buffer {
    fn writer(&self, res: &Resource) -> Result<Arc<BatchWriter>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let mut writers = self.writers.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(writer) = writers.get(res.table()) {
            return Ok(writer.clone());
        }
        let writer = Arc::new(self.inner.batch_writer(res.clone(), self.config.clone())?);
        writers.insert(res.table().to_owned(), writer.clone());
        Ok(writer)
    }
}

#[async_trait::async_trait]
impl Driver for Buffer {
    async fn create(&self, res: &Resource, record: &Record) -> Result<WriteResult> {
        let writer = self.writer(res)?;
        if !same_columns(writer.resource(), res) {
            return Err(Error::Invalid(format!(
                "table {} is already buffered with other columns",
                res.table()
            )));
        }
        writer.write(record.clone()).await?;
        Ok(WriteResult {
            record: record.clone(),
            pending: true,
            metadata: BTreeMap::from([("state".into(), "queued".into())]),
        })
    }
    async fn get(&self, res: &Resource, key: &str) -> Result<Record> {
        self.inner.get(res, key).await
    }
    async fn update(&self, res: &Resource, record: &Record) -> Result<WriteResult> {
        self.inner.update(res, record).await
    }
    async fn delete(&self, res: &Resource, key: &str) -> Result<()> {
        self.inner.delete(res, key).await
    }
    async fn list(&self, res: &Resource, opts: &ListOptions) -> Result<ListResult> {
        self.inner.list(res, opts).await
    }
    async fn count(&self, res: &Resource, opts: &ListOptions) -> Result<i64> {
        self.inner.count(res, opts).await
    }
    async fn exists(&self, res: &Resource, key: &str) -> Result<bool> {
        self.inner.exists(res, key).await
    }
}

#[async_trait::async_trait]
impl Release for Buffer {
    /// Stops new creates, flushes every queue, then closes the wrapped DB.
    async fn release(&self) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        let writers: Vec<_> = {
            let mut map = self.writers.lock().unwrap_or_else(|e| e.into_inner());
            map.drain().map(|(_, w)| w).collect()
        };
        let mut first = None;
        for writer in writers {
            if let Err(e) = writer.flush().await {
                first.get_or_insert(e);
            }
        }
        let closed = self.inner.close().await;
        match first {
            Some(e) => Err(e),
            None => closed,
        }
    }
}
