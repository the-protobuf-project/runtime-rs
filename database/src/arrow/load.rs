//! Crash-tolerant segment loading, with time pruning for range queries.

use super::bounds::Bounds;
use super::codec::{decode, decode_rows};
use super::errors::{backend, invalid};
use super::schema::{from_schema, matches};
use super::segment::EXTENSION;
use crate::store::{Record, Resource, Result};
use arrow_array::{Array, RecordBatch, TimestampNanosecondArray};
use arrow_ipc::reader::StreamReader;
use arrow_schema::ArrowError;
use std::fs::File;
use std::io::{BufReader, ErrorKind};
use std::path::{Path, PathBuf};

/// One segment file decoded with the descriptor stored inside it.
#[derive(Debug)]
pub struct Loaded {
    /// Descriptor rebuilt from the file's schema metadata.
    pub resource: Resource,
    /// Every complete row, in write order.
    pub records: Vec<Record>,
    /// False when the file ends part-way through a batch, as after a crash.
    /// The rows before that batch are still returned.
    pub complete: bool,
}

/// Reads a segment written by this driver, tolerating a truncated final batch.
pub fn load(path: &Path) -> Result<Loaded> {
    let (resource, reader) = open(path)?;
    let mut records = Vec::new();
    let complete = each_batch(reader, |batch| {
        records.extend(decode(&resource, &batch)?);
        Ok(())
    })?;
    Ok(Loaded {
        resource,
        records,
        complete,
    })
}

/// Segment files in one table directory, oldest first. A missing directory
/// means nothing has been written yet.
pub fn segments(dir: &Path) -> Result<Vec<PathBuf>> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(backend(e)),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let path = entry.map_err(backend)?.path();
        if path.extension().is_some_and(|ext| ext == EXTENSION) {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

/// Feeds `visit` the rows of one segment with times in [start, end), one
/// batch at a time. The file is skipped when its sidecar says it cannot
/// overlap; only rows inside the window become records.
pub(super) fn scan(
    path: &Path,
    resource: &Resource,
    (start, end): (i64, i64),
    visit: &mut impl FnMut(Vec<Record>),
) -> Result<()> {
    if Bounds::read(path).is_some_and(|b| b.outside(start, end)) {
        return Ok(());
    }
    let (stored, reader) = open(path)?;
    if !matches(&stored, resource) {
        return Err(invalid(format!(
            "{} was written with other columns",
            path.display()
        )));
    }
    let time = resource.time_column()?;
    each_batch(reader, |batch| {
        let times = batch
            .column_by_name(time)
            .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>())
            .ok_or_else(|| invalid("arrow batch has no timestamp column"))?;
        let rows: Vec<usize> = (0..times.len())
            .filter(|&i| !times.is_null(i) && (start..end).contains(&times.value(i)))
            .collect();
        if !rows.is_empty() {
            visit(decode_rows(resource, &batch, &rows)?);
        }
        Ok(())
    })?;
    Ok(())
}

fn open(path: &Path) -> Result<(Resource, StreamReader<BufReader<File>>)> {
    let file = File::open(path).map_err(backend)?;
    let reader = StreamReader::try_new(BufReader::new(file), None).map_err(backend)?;
    Ok((from_schema(&reader.schema())?, reader))
}

/// Feeds every complete batch to `visit`; returns false for a truncated tail.
fn each_batch(
    reader: StreamReader<BufReader<File>>,
    mut visit: impl FnMut(RecordBatch) -> Result<()>,
) -> Result<bool> {
    for batch in reader {
        match batch {
            Ok(batch) => visit(batch)?,
            Err(ArrowError::IoError(_, e)) if e.kind() == ErrorKind::UnexpectedEof => {
                return Ok(false);
            }
            Err(e) => return Err(backend(e)),
        }
    }
    Ok(true)
}
