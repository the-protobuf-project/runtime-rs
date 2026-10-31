//! A directory of Arrow IPC segments, one directory per table.

use super::codec::encode;
use super::errors::{invalid, joined};
use super::filter;
use super::load::{scan, segments};
use super::rotation::Rotation;
use super::schema::matches;
use super::segment::Segment;
use crate::store::window::Range;
use crate::store::{Error, ListResult, Record, Resource, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// Appends each write as one batch to the table's open segment.
///
/// File I/O runs on Tokio's blocking pool. Writes to one selection are
/// serialized; reads do not wait for writers and see every flushed batch.
/// A failed write, or a segment reaching its rotation limit, closes that
/// segment and the next write starts a new file.
#[derive(Clone)]
pub struct ArrowDriver {
    inner: Arc<Inner>,
}

struct Inner {
    dir: PathBuf,
    sync: bool,
    rotation: Rotation,
    open: Mutex<HashMap<String, Segment>>,
}

impl ArrowDriver {
    pub(super) fn new(dir: PathBuf, sync: bool, rotation: Rotation) -> Self {
        Self {
            inner: Arc::new(Inner {
                dir,
                sync,
                rotation,
                open: Mutex::default(),
            }),
        }
    }

    /// Finishes every open segment so its file ends with an end-of-stream
    /// marker. Later writes start new segments. Dropping the last handle does
    /// the same, so calling this is optional.
    pub async fn close(&self) -> Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || lock(&inner).clear())
            .await
            .map_err(joined)
    }
}

fn lock(inner: &Inner) -> std::sync::MutexGuard<'_, HashMap<String, Segment>> {
    // Segment state stays consistent across a panic: a failed append marks it.
    inner.open.lock().unwrap_or_else(|e| e.into_inner())
}

impl ArrowDriver {
    /// Validates every record, then appends them as one batch.
    pub(super) async fn write(&self, resource: &Resource, records: &[Record]) -> Result<()> {
        resource.time_column()?;
        for record in records {
            resource.validate(record)?;
        }
        if records.is_empty() {
            return Ok(());
        }
        let batch = encode(resource, records)?;
        let (inner, resource) = (self.inner.clone(), resource.clone());
        tokio::task::spawn_blocking(move || {
            let mut open = lock(&inner);
            let table = resource.table().to_owned();
            if let Some(segment) = open.get(&table) {
                if !matches(&segment.resource, &resource) {
                    return Err(invalid(format!(
                        "table {table} is already being written with other columns"
                    )));
                }
            } else {
                let segment = Segment::create(&inner.dir.join(&table), &resource, inner.sync)?;
                open.insert(table.clone(), segment);
            }
            let segment = open.get_mut(&table).expect("segment inserted above");
            let appended = segment.append(&batch);
            if appended.is_err() || segment.full(&inner.rotation) {
                open.remove(&table);
            }
            appended
        })
        .await
        .map_err(joined)?
    }

    /// One page of the window after the offset in `token`, with every match
    /// counted, as runtime-go's offset tokens.
    pub(super) async fn page(
        &self,
        res: &Resource,
        range: &Range,
        token: &str,
    ) -> Result<ListResult> {
        let offset: usize = match token {
            "" => 0,
            t => t
                .parse()
                .map_err(|_| invalid("page_token is not from this backend"))?,
        };
        let top = filter::Top::new(res, range, offset)?;
        let (items, total) = self
            .window(res, range, top, |top, rows| top.push(rows))
            .await?
            .finish();
        let next = offset + items.len();
        Ok(ListResult {
            next_page_token: if (next as i64) < total {
                next.to_string()
            } else {
                String::new()
            },
            items,
            total,
        })
    }

    /// Closes the table's open segment, if any, before its files change.
    pub(super) async fn close_table(&self, table: &str) -> Result<()> {
        let (inner, table) = (self.inner.clone(), table.to_owned());
        tokio::task::spawn_blocking(move || {
            lock(&inner).remove(&table);
        })
        .await
        .map_err(joined)
    }

    pub(super) fn table_dir(&self, table: &str) -> PathBuf {
        self.inner.dir.join(table)
    }

    pub(super) fn refuse<T>(&self, capability: &'static str) -> Result<T> {
        Err(Error::Unimplemented {
            backend: super::errors::BACKEND,
            capability,
        })
    }
    /// Streams the table's rows inside the range's time window, batch by
    /// batch, through `step` on the blocking pool, and returns the state.
    pub(super) async fn window<S: Send + 'static>(
        &self,
        resource: &Resource,
        range: &Range,
        mut state: S,
        step: impl Fn(&mut S, Vec<Record>) + Send + 'static,
    ) -> Result<S> {
        let dir = self.inner.dir.join(resource.table());
        let (resource, bounds) = (resource.clone(), (range.start, range.end));
        tokio::task::spawn_blocking(move || {
            for path in segments(&dir)? {
                scan(&path, &resource, bounds, &mut |rows| step(&mut state, rows))?;
            }
            Ok(state)
        })
        .await
        .map_err(joined)?
    }
}
