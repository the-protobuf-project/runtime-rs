//! One page's search of a table: which blocks to read, and which of their rows to keep.

use super::block::Block;
use super::codec::decode_rows;
use super::filter::keep;
use super::index::{Indexes, SegmentIndex};
use super::key::{Entry, Key, order, owned, parts};
use super::resume::times;
use crate::store::window::Range;
use crate::store::{Record, Resource, Result};
use arrow_array::{Array, RecordBatch};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::path::Path;

/// One page's search.
pub(super) struct Search<'a> {
    resource: &'a Resource,
    range: &'a Range,
    time: &'a str,
    after: Option<Key>,
    /// One past the page, which shows whether another page follows.
    capacity: usize,
    best: BinaryHeap<Entry>,
    /// The total so far, or `None` when this page is not counting.
    matched: Option<i64>,
}

impl<'a> Search<'a> {
    /// A search of `range` after `after`, counting every match when `counting`.
    pub(super) fn new(
        resource: &'a Resource,
        range: &'a Range,
        after: Option<Key>,
        counting: bool,
    ) -> Result<Self> {
        Ok(Self {
            resource,
            range,
            time: resource.time_column()?,
            after,
            capacity: range.limit + 1,
            best: BinaryHeap::new(),
            matched: counting.then_some(0),
        })
    }

    /// The best rows found, and the count if this search kept one.
    pub(super) fn finish(self) -> (BinaryHeap<Entry>, Option<i64>) {
        (self.best, self.matched)
    }

    fn after_cursor(&self, key: (i64, &str, u64)) -> bool {
        self.after.as_ref().is_none_or(|after| {
            order(key, parts(after), self.range.descending) == Ordering::Greater
        })
    }

    /// True when a row at `key` would earn a place in the page.
    fn wanted(&self, key: (i64, &str, u64)) -> bool {
        self.after_cursor(key)
            && (self.best.len() < self.capacity
                || self.best.peek().is_none_or(|worst| {
                    order(key, parts(&worst.key), self.range.descending) == Ordering::Less
                }))
    }

    /// True when any row of `block` could earn a place: its latest possible key is past the
    /// cursor, and its earliest possible key beats the page's worst.
    fn needs(&self, block: &Block, segment: &str) -> bool {
        let descending = self.range.descending;
        let (first, last) = match descending {
            true => (block.max, block.min),
            false => (block.min, block.max),
        };
        self.after_cursor((last, segment, block.last_row()))
            && (self.best.len() < self.capacity
                || self.best.peek().is_none_or(|worst| {
                    order(
                        (first, segment, block.first_row),
                        parts(&worst.key),
                        descending,
                    ) == Ordering::Less
                }))
    }

    fn push(&mut self, key: Key, record: Record) {
        self.best.push(Entry {
            key,
            descending: self.range.descending,
            record,
        });
        if self.best.len() > self.capacity {
            self.best.pop();
        }
    }

    pub(super) fn block(
        &mut self,
        indexes: &Indexes,
        path: &Path,
        index: &SegmentIndex,
        block: &Block,
        segment: &str,
    ) -> Result<()> {
        let (start, end) = (self.range.start, self.range.end);
        if block.outside(start, end) {
            return Ok(());
        }
        let filtered = !self.range.predicates.is_empty();
        let mut counting = self.matched.is_some();
        // A whole block inside the window is counted without being read, unless each of
        // its rows has a predicate to answer.
        if counting && !filtered && block.inside(start, end) {
            self.matched = self.matched.map(|n| n + block.valid as i64);
            counting = false;
        }
        if counting || self.needs(block, segment) {
            indexes.read(path, index, block, |batch, first| {
                self.batch(batch, first, segment, counting)
            })?;
        }
        Ok(())
    }

    fn batch(
        &mut self,
        batch: &RecordBatch,
        first: u64,
        segment: &str,
        counting: bool,
    ) -> Result<()> {
        let (start, end) = (self.range.start, self.range.end);
        let times = times(batch, self.time)?;
        let mut inside: Vec<usize> = (0..times.len())
            .filter(|&i| !times.is_null(i) && (start..end).contains(&times.value(i)))
            .collect();
        let key = |i: usize| (times.value(i), segment, first + i as u64);

        if counting && !self.range.predicates.is_empty() {
            let records = decode_rows(self.resource, batch, &inside)?;
            let mut matched = 0;
            for (&i, record) in inside.iter().zip(records) {
                if keep(self.resource, self.range, &record) {
                    matched += 1;
                    if self.wanted(key(i)) {
                        self.push(owned(key(i)), record);
                    }
                }
            }
            self.matched = self.matched.map(|n| n + matched);
            return Ok(());
        }
        if counting {
            self.matched = self.matched.map(|n| n + inside.len() as i64);
        }

        // Best first, so a large batch decodes only as far as the page needs.
        inside.retain(|&i| self.after_cursor(key(i)));
        let descending = self.range.descending;
        inside.sort_unstable_by(|&a, &b| order(key(a), key(b), descending));
        for rows in inside.chunks(self.capacity.max(256)) {
            if rows.first().is_some_and(|&i| !self.wanted(key(i))) {
                break;
            }
            let records = decode_rows(self.resource, batch, rows)?;
            for (&i, record) in rows.iter().zip(records) {
                if keep(self.resource, self.range, &record) && self.wanted(key(i)) {
                    self.push(owned(key(i)), record);
                }
            }
        }
        Ok(())
    }
}
