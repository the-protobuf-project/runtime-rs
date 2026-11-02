//! A small in-memory index of each segment, so a page can seek instead of rescan.
//!
//! An IPC stream has no footer to seek by. Reading one batch means reading past every batch
//! before it, which is what made paging a table quadratic. The index records, per block of
//! about [`BLOCK_ROWS`] rows, the byte range and time range of the batches inside. It costs
//! one read of each segment's time column, once, and about 48 bytes per block: 5.3M rows is
//! some 650 blocks. A segment still being written is extended from where the index ended.
//!
//! Nothing here is authoritative. The index is rebuilt from the file if it is lost, and it
//! never leaves the process.

use super::block::Block;
use super::errors::{backend, invalid};
use super::resume::{batches, header, resume};
use crate::store::{Resource, Result};
use arrow_array::{Array, RecordBatch, TimestampNanosecondArray};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Rows a block gathers before the next batch starts another.
const BLOCK_ROWS: u64 = 8192;

/// What is known about one segment.
pub(super) struct SegmentIndex {
    /// The descriptor stored in the file.
    pub resource: Resource,
    /// The stream's schema message, which every resumed read must start with.
    header: Vec<u8>,
    pub blocks: Vec<Block>,
    /// The end of the last complete batch.
    indexed: u64,
    /// The file length when last examined, so an unchanged file costs one `stat`.
    seen: u64,
}

/// Every segment's index, for one driver.
#[derive(Default)]
pub(super) struct Indexes {
    segments: Mutex<HashMap<PathBuf, Arc<SegmentIndex>>>,
    /// Batches read back from disk by queries, for the tests holding reads to linear.
    pub reads: AtomicU64,
}

impl Indexes {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<PathBuf, Arc<SegmentIndex>>> {
        // Entries are replaced whole, so a panic cannot leave one half-written.
        self.segments.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The index of `path`, current with the file.
    pub fn get(&self, path: &Path) -> Result<Arc<SegmentIndex>> {
        let len = std::fs::metadata(path).map_err(backend)?.len();
        let known = self.lock().get(path).cloned();
        if let Some(index) = &known
            && index.seen == len
        {
            return Ok(Arc::clone(index));
        }
        let index = Arc::new(self.extended(path, known.as_deref(), len)?);
        self.lock().insert(path.to_owned(), Arc::clone(&index));
        Ok(index)
    }

    /// Forgets every segment under `dir`, whose files are being removed.
    pub fn forget(&self, dir: &Path) {
        self.lock().retain(|path, _| !path.starts_with(dir));
    }

    fn extended(
        &self,
        path: &Path,
        known: Option<&SegmentIndex>,
        len: u64,
    ) -> Result<SegmentIndex> {
        let mut index = match known {
            // Segments only grow; a shorter file is not the one that was indexed.
            Some(known) if known.indexed <= len => SegmentIndex {
                resource: known.resource.clone(),
                header: known.header.clone(),
                blocks: known.blocks.clone(),
                indexed: known.indexed,
                seen: len,
            },
            _ => {
                let (resource, header) = header(path)?;
                SegmentIndex {
                    resource,
                    indexed: header.len() as u64,
                    header,
                    blocks: Vec::new(),
                    seen: len,
                }
            }
        };
        let time = index.resource.time_column()?.to_owned();
        let projection = index
            .resource
            .columns()
            .iter()
            .position(|column| column.name == time)
            .ok_or_else(|| invalid("arrow file has no timestamp column"))?;

        let next_row = index.blocks.last().map_or(0, |b| b.first_row + b.rows);
        // A short last block goes on filling, so a table read while it grows does not end
        // up with one block per page.
        let mut block = match index.blocks.last() {
            Some(last) if last.rows < BLOCK_ROWS => index.blocks.pop(),
            _ => None,
        }
        .unwrap_or_else(|| Block::new(index.indexed, next_row));

        let mut blocks = Vec::new();
        let reader = resume(path, &index.header, index.indexed, Some(vec![projection]))?;
        batches(reader, |batch, end| {
            self.reads.fetch_add(1, Ordering::Relaxed);
            let times = batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| invalid("arrow batch has no timestamp column"))?;
            block.rows += times.len() as u64;
            block.end = end;
            for time in times.iter().flatten() {
                block.valid += 1;
                block.min = block.min.min(time);
                block.max = block.max.max(time);
            }
            if block.rows >= BLOCK_ROWS {
                blocks.push(block);
                block = Block::new(end, block.first_row + block.rows);
            }
            Ok(true)
        })?;
        if block.rows > 0 {
            blocks.push(block);
        }
        index.indexed = blocks.last().map_or(index.indexed, |b| b.end);
        index.blocks.extend(blocks);
        Ok(index)
    }

    /// Feeds `visit` every batch of `block` and the row ordinal it starts at.
    pub fn read(
        &self,
        path: &Path,
        index: &SegmentIndex,
        block: &Block,
        mut visit: impl FnMut(&RecordBatch, u64) -> Result<()>,
    ) -> Result<()> {
        let mut row = block.first_row;
        let reader = resume(path, &index.header, block.offset, None)?;
        batches(reader, |batch, end| {
            self.reads.fetch_add(1, Ordering::Relaxed);
            visit(&batch, row)?;
            row += batch.num_rows() as u64;
            Ok(end < block.end)
        })
    }
}
