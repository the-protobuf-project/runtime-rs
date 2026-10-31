//! Append-only Arrow IPC stream segments.
//!
//! A segment is one `.arrows` file in the IPC streaming format: a schema header
//! followed by record batches. Every write is flushed to the operating system
//! before it returns, so a crashed process loses at most the batch it was
//! writing. Files are created with `create_new` and never reopened, so an
//! existing capture is never overwritten or appended to with another schema.
//! A segment appears under its final name only after its header is written.
//!
//! A cleanly closed segment also gets a `.bounds` sidecar holding its time
//! range, which lets range queries skip the file without opening it.

use super::bounds::Bounds;
use super::errors::backend;
use super::rotation::Rotation;
use super::schema::to_schema;
use crate::store::{Resource, Result};
use arrow_array::{Array, RecordBatch, TimestampNanosecondArray};
use arrow_ipc::writer::StreamWriter;
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// File extension for Arrow IPC streams, as DuckDB, pyarrow and polars expect.
pub const EXTENSION: &str = "arrows";

static SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// The open segment one writer appends to.
pub(super) struct Segment {
    writer: StreamWriter<BufWriter<File>>,
    path: PathBuf,
    pub(super) resource: Resource,
    sync: bool,
    opened: Instant,
    bounds: Option<Bounds>,
    /// Set once a write fails part-way; the file then ends at the last good
    /// batch and no end-of-stream marker is written after the partial bytes.
    failed: bool,
}

impl Segment {
    /// Starts a new file in `dir`, named by creation time so a directory
    /// listing sorts segments chronologically.
    pub(super) fn create(dir: &Path, resource: &Resource, sync: bool) -> Result<Self> {
        std::fs::create_dir_all(dir).map_err(backend)?;
        let started = chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default();
        let name = format!(
            "{started:020}-{}-{}.{EXTENSION}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        );
        let path = dir.join(&name);
        // Readers list `.arrows` files without the writer's lock, so the file
        // gets its final name only once its schema header is on disk.
        let staging = dir.join(format!("{name}.part"));
        let file = File::options()
            .write(true)
            .create_new(true)
            .open(&staging)
            .map_err(backend)?;
        let mut writer =
            StreamWriter::try_new_buffered(file, &to_schema(resource)).map_err(backend)?;
        writer.flush().map_err(backend)?;
        if path.exists() {
            return Err(backend(std::io::Error::from(
                std::io::ErrorKind::AlreadyExists,
            )));
        }
        std::fs::rename(&staging, &path).map_err(backend)?;
        Ok(Self {
            writer,
            path,
            resource: resource.clone(),
            sync,
            opened: Instant::now(),
            bounds: None,
            failed: false,
        })
    }

    /// Appends and flushes one batch; with `sync`, also waits for the disk.
    pub(super) fn append(&mut self, batch: &RecordBatch) -> Result<()> {
        let written = self.writer.write(batch).and_then(|()| self.writer.flush());
        let synced = written.map_err(backend).and_then(|()| match self.sync {
            true => self.writer.get_ref().get_ref().sync_data().map_err(backend),
            false => Ok(()),
        });
        self.failed = synced.is_err();
        if synced.is_ok() {
            self.extend_bounds(batch);
        }
        synced
    }

    /// True once this segment has reached either rotation limit.
    pub(super) fn full(&self, rotation: &Rotation) -> bool {
        let too_old = rotation
            .max_age
            .is_some_and(|age| self.opened.elapsed() >= age);
        let too_big = rotation.max_bytes.is_some_and(|max| {
            self.writer
                .get_ref()
                .get_ref()
                .metadata()
                .is_ok_and(|m| m.len() >= max)
        });
        too_old || too_big
    }

    fn extend_bounds(&mut self, batch: &RecordBatch) {
        let Some(times) = self
            .resource
            .time_column()
            .ok()
            .and_then(|name| batch.column_by_name(name))
            .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
        else {
            return;
        };
        let values = times.values();
        let (Some(&min), Some(&max)) = (values.iter().min(), values.iter().max()) else {
            return;
        };
        let rows = times.len() as u64;
        self.bounds = Some(match self.bounds {
            Some(b) => Bounds {
                min: b.min.min(min),
                max: b.max.max(max),
                rows: b.rows + rows,
            },
            None => Bounds { min, max, rows },
        });
    }
}

impl Drop for Segment {
    /// Writes the end-of-stream marker and the bounds sidecar. A failed or
    /// crashed segment has neither; readers scan such files in full.
    fn drop(&mut self) {
        if self.failed || self.writer.finish().is_err() {
            return;
        }
        if let Some(bounds) = self.bounds {
            let _ = bounds.save(&self.path);
        }
    }
}
