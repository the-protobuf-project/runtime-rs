//! Structured queries: identifiers are descriptors and values are parameters.

use crate::{Error, Record, Result, Value};

/// Comparison applied to one declared column.
#[derive(Clone, Copy, Debug)]
pub enum Comparison {
    /// Equal.
    Eq,
    /// Not equal.
    Ne,
    /// Less than.
    Lt,
    /// Less than or equal.
    Le,
    /// Greater than.
    Gt,
    /// Greater than or equal.
    Ge,
}

/// A typed comparison; all supplied predicates are ANDed.
#[derive(Clone, Debug)]
pub struct Predicate {
    /// Descriptor column name.
    pub column: String,
    /// Comparison operator.
    pub comparison: Comparison,
    /// Scalar value, validated against the descriptor.
    pub value: Value,
}

/// A bounded half-open [start, end) time window in epoch nanoseconds.
#[derive(Clone, Debug)]
pub struct Range {
    /// Inclusive lower time bound.
    pub start: i64,
    /// Exclusive upper time bound.
    pub end: i64,
    /// Maximum rows per range query or page, in 1..=10,000.
    pub limit: usize,
    /// Return newest points first when true, oldest first otherwise.
    pub descending: bool,
    /// Additional comparisons combined with AND.
    pub predicates: Vec<Predicate>,
}

impl Range {
    /// Creates a window returning at most 100 points, oldest first.
    pub fn new(start: i64, end: i64) -> Self {
        Self {
            start,
            end,
            limit: 100,
            descending: false,
            predicates: Vec::new(),
        }
    }

    /// Rejects invalid bounds and limits before backend I/O.
    pub fn validate(&self) -> Result<()> {
        if self.start >= self.end || !(1..=10_000).contains(&self.limit) {
            return Err(Error::Invalid(
                "range needs start < end and limit in 1..=10000".into(),
            ));
        }
        Ok(())
    }
}

/// Numeric reduction performed on the server.
#[derive(Clone, Copy, Debug)]
pub enum Reduction {
    /// Number of non-null observations.
    Count,
    /// Arithmetic mean.
    Mean,
    /// Sum.
    Sum,
    /// Minimum.
    Min,
    /// Maximum.
    Max,
}

/// One reduced field with an explicit output name.
#[derive(Clone, Debug)]
pub struct Reduce {
    /// Declared numeric field to reduce.
    pub column: String,
    /// Server-side function.
    pub function: Reduction,
    /// Unique output field name.
    pub alias: String,
}

/// Bucketed server-side reduction over a bounded time window.
#[derive(Clone, Debug)]
pub struct Aggregate {
    /// Time bounds, predicates and output-row limit.
    pub range: Range,
    /// Fixed bucket width in whole seconds, aligned to the Unix epoch.
    pub every_seconds: u32,
    /// One or more reductions.
    pub reduce: Vec<Reduce>,
    /// Declared tag columns retained in each output group.
    pub group_by: Vec<String>,
}

/// Aggregate rows contain the resource's timestamp column, group tags, and
/// reduction aliases. They are not full records and bypass write validation.
pub type Buckets = Vec<Record>;
