//! In-memory evaluation of half-open ranges and typed predicates.

use crate::store::validate_value;
use crate::store::window::{Comparison, Predicate, Range};
use crate::store::{Kind, Record, Resource, Result, Value};
use std::cmp::Ordering;

/// Checks the query against the descriptor before any file is read.
pub(super) fn validate(resource: &Resource, range: &Range) -> Result<()> {
    range.validate()?;
    resource.time_column()?;
    for p in &range.predicates {
        validate_value(resource.column(&p.column)?.kind, &p.value)?;
    }
    Ok(())
}

/// The page of matching rows after `offset`, in time order, kept while
/// batches stream in, plus a count of every match. Memory stays near twice
/// offset + limit however many rows are scanned. The sort is stable, so equal
/// timestamps keep their write order.
pub(super) struct Top {
    resource: Resource,
    range: Range,
    time: String,
    offset: usize,
    kept: Vec<Record>,
    matched: i64,
}

impl Top {
    pub(super) fn new(resource: &Resource, range: &Range, offset: usize) -> Result<Self> {
        Ok(Self {
            resource: resource.clone(),
            range: range.clone(),
            time: resource.time_column()?.to_owned(),
            offset,
            kept: Vec::new(),
            matched: 0,
        })
    }

    pub(super) fn push(&mut self, rows: Vec<Record>) {
        let (resource, range) = (&self.resource, &self.range);
        let before = self.kept.len();
        self.kept
            .extend(rows.into_iter().filter(|row| keep(resource, range, row)));
        self.matched += (self.kept.len() - before) as i64;
        if self.kept.len() > 2 * self.keep_count() {
            self.compact();
        }
    }

    /// The page and the total number of matching rows.
    pub(super) fn finish(mut self) -> (Vec<Record>, i64) {
        self.compact();
        let page = self.kept.split_off(self.offset.min(self.kept.len()));
        (page, self.matched)
    }

    fn keep_count(&self) -> usize {
        self.offset.saturating_add(self.range.limit)
    }

    fn compact(&mut self) {
        let time = &self.time;
        let at = |row: &Record| row.get(time).and_then(Value::as_i64).unwrap_or(i64::MIN);
        match self.range.descending {
            true => self.kept.sort_by_key(|row| std::cmp::Reverse(at(row))),
            false => self.kept.sort_by_key(at),
        }
        self.kept.truncate(self.keep_count());
    }
}

/// True for a row inside the window that satisfies every predicate.
pub(super) fn keep(resource: &Resource, range: &Range, row: &Record) -> bool {
    let inside = resource
        .time_column()
        .ok()
        .and_then(|time| row.get(time))
        .and_then(Value::as_i64)
        .is_some_and(|t| t >= range.start && t < range.end);
    inside && range.predicates.iter().all(|p| holds(resource, row, p))
}

fn holds(resource: &Resource, row: &Record, predicate: &Predicate) -> bool {
    let (Ok(col), Some(value)) = (
        resource.column(&predicate.column),
        row.get(&predicate.column),
    ) else {
        return false;
    };
    let Some(order) = compare(col.kind, value, &predicate.value) else {
        return false;
    };
    match predicate.comparison {
        Comparison::Eq => order == Ordering::Equal,
        Comparison::Ne => order != Ordering::Equal,
        Comparison::Lt => order == Ordering::Less,
        Comparison::Le => order != Ordering::Greater,
        Comparison::Gt => order == Ordering::Greater,
        Comparison::Ge => order != Ordering::Less,
    }
}

/// Compares within the column's own domain; nothing is coerced across kinds.
fn compare(kind: Kind, left: &Value, right: &Value) -> Option<Ordering> {
    match kind {
        Kind::String => Some(left.as_str()?.cmp(right.as_str()?)),
        Kind::Int | Kind::Enum | Kind::Timestamp => Some(left.as_i64()?.cmp(&right.as_i64()?)),
        Kind::Uint => Some(left.as_u64()?.cmp(&right.as_u64()?)),
        Kind::Float => left.as_f64()?.partial_cmp(&right.as_f64()?),
        Kind::Bool => Some(left.as_bool()?.cmp(&right.as_bool()?)),
    }
}
