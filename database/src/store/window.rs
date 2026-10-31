//! Validated windows built from the public options, shared by every backend.
//!
//! Backends never parse filters or resolve time columns themselves: they take
//! these, so a filter or bound means the same thing everywhere.

use super::driver::page_size;
use super::filter::predicates;
#[cfg(test)]
pub(crate) use super::reduce::Reduce;
pub(crate) use super::reduce::{Aggregate, Reduction};
use super::{Error, Kind, RangeOptions, Resource, Result, Role, Value};

/// Comparison applied to one declared column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Comparison {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// A typed comparison; all predicates of a window are ANDed.
#[derive(Clone, Debug)]
pub(crate) struct Predicate {
    pub column: String,
    pub comparison: Comparison,
    pub value: Value,
}

/// A bounded half-open [start, end) window in epoch nanoseconds.
#[derive(Clone, Debug)]
pub(crate) struct Range {
    pub start: i64,
    pub end: i64,
    /// Rows per page, in 1..=10,000.
    pub limit: usize,
    pub descending: bool,
    pub predicates: Vec<Predicate>,
}

impl Range {
    /// A 100-row, oldest-first window, for unit tests.
    #[cfg(test)]
    pub fn new(start: i64, end: i64) -> Self {
        Self {
            start,
            end,
            limit: 100,
            descending: false,
            predicates: Vec::new(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.start >= self.end || !(1..=10_000).contains(&self.limit) {
            return Err(Error::Invalid(
                "range needs start < end and a page size of at most 10000".into(),
            ));
        }
        Ok(())
    }

    /// The window of a [`RangeOptions`] read against `res`.
    pub fn of(res: &Resource, opts: &RangeOptions) -> Result<Self> {
        time_column(res, &opts.time_column)?;
        let range = Self {
            start: opts.start,
            end: opts.end,
            limit: page_size(opts.page_size)?,
            descending: opts.descending,
            predicates: predicates(res, &opts.filter)?,
        };
        range.validate()?;
        Ok(range)
    }
}

/// Checks a caller-named time column. Backends here have one timestamp column
/// per resource, so naming any other is refused rather than ignored.
pub(super) fn time_column(res: &Resource, named: &str) -> Result<()> {
    let time = res.time_column()?;
    if named.is_empty() || named == time {
        return Ok(());
    }
    match res.column(named)? {
        col if col.role == Role::Timestamp && col.kind == Kind::Timestamp => Ok(()),
        _ => Err(Error::Invalid(format!(
            "{named} is not the timestamp column {time}"
        ))),
    }
}
