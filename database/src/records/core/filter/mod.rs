//! AIP-160 filters (<https://google.aip.dev/160>) parsed into an [`Expr`] whose columns are
//! resolved and whose values are typed against a [`Resource`], so every provider translates
//! one tree, and agrees on what it matches.
//!
//! Supported: `AND`, `OR`, `NOT` and `-`, parentheses, juxtaposition as `AND`, the
//! comparators `= != < <= > >=`, the has operator as presence (`field:*`), quoted strings with
//! escapes, numbers, booleans, `null` (`field = null`, `field != null`), and timestamps as
//! quoted RFC 3339 text. `OR` binds tighter than `AND`, as AIP-160 specifies:
//! `a AND b OR c` is `a AND (b OR c)`. Keywords are case-insensitive.
//!
//! Refused by name: wildcards (`title = "Dune*"`; escape a literal star as `\*`), function
//! calls, traversal into messages (`author.name`), bare values (global search), the has
//! operator with anything but `*`, ordering comparisons on bytes, and durations, which no
//! column kind stores.
//!
//! Nulls follow SQL: a comparison with a null column matches nothing, `!=` and `NOT`
//! included. [`Expr::negated`] pushes a `NOT` down to the comparisons, which is how providers
//! without three-valued logic keep that rule.
//!
//! ```
//! use database::records::core::filter::{self, Expr, Op};
//! # let md = prost_reflect::DescriptorPool::global()
//! #     .get_message_by_name("google.protobuf.Timestamp").unwrap();
//! # use database::records::{Column, Kind, Resource, Value};
//! let res = Resource::new(md, "t", "seconds", vec![Column::new("seconds", Kind::Int)]);
//! let expr = filter::parse(&res, "NOT seconds > 5").unwrap().unwrap();
//! let Expr::Compare(c) = expr.without_not() else { unreachable!() };
//! assert_eq!((c.op, c.value), (Op::Le, Value::Int(5)));
//! ```

mod eval;
mod lex;
mod resolve;
mod restriction;
pub(crate) mod syntax;

pub use eval::compare;

use crate::records::{Kind, Resource, Value};
use crate::{Error, Result};

/// A filter, resolved: columns are the backend names [`Resource`] declares, values are typed
/// by their column's [`Kind`].
#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    /// Every one holds.
    And(Vec<Expr>),
    /// Any one holds.
    Or(Vec<Expr>),
    /// The inner expression does not hold; see [`Expr::negated`] for how nulls fare.
    Not(Box<Expr>),
    /// `column op value`. Never true when the column is null.
    Compare(Comparison),
    /// `column:*` or `column != null`: the column holds a value.
    Has(String),
    /// `column = null`: the column holds no value.
    Null(String),
}

/// One `column op value` comparison.
#[derive(Clone, Debug, PartialEq)]
pub struct Comparison {
    /// The column's backend name.
    pub column: String,
    /// The column's kind, which typed `value`.
    pub kind: Kind,
    /// The comparator.
    pub op: Op,
    /// The value, typed by `kind`; never [`Value::Null`].
    pub value: Value,
}

/// A comparator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    /// `=`
    Eq,
    /// `!=`
    Ne,
    /// `<`
    Lt,
    /// `<=`
    Le,
    /// `>`
    Gt,
    /// `>=`
    Ge,
}

impl Op {
    /// The comparator that holds exactly when this one does not, for a non-null column.
    pub fn negated(self) -> Op {
        match self {
            Op::Eq => Op::Ne,
            Op::Ne => Op::Eq,
            Op::Lt => Op::Ge,
            Op::Le => Op::Gt,
            Op::Gt => Op::Le,
            Op::Ge => Op::Lt,
        }
    }

    /// Whether `have op want` holds, given how `have` orders against `want`.
    pub fn holds(self, o: std::cmp::Ordering) -> bool {
        use std::cmp::Ordering::{Equal, Greater, Less};
        match self {
            Op::Eq => o == Equal,
            Op::Ne => o != Equal,
            Op::Lt => o == Less,
            Op::Le => o != Greater,
            Op::Gt => o == Greater,
            Op::Ge => o != Less,
        }
    }

    /// The comparator as a filter spells it.
    pub fn as_str(self) -> &'static str {
        match self {
            Op::Eq => "=",
            Op::Ne => "!=",
            Op::Lt => "<",
            Op::Le => "<=",
            Op::Gt => ">",
            Op::Ge => ">=",
        }
    }
}

impl Expr {
    /// `NOT self` with the negation pushed down to the leaves, so the result holds no
    /// [`Expr::Not`]: De Morgan through `AND` and `OR`, a comparison's comparator flipped,
    /// [`Expr::Has`] and [`Expr::Null`] swapped. A flipped comparison still fails on a null
    /// column, so `NOT a = 1` matches what SQL's `NOT (a = 1)` does, nulls excluded, on a
    /// provider whose own negation would let nulls through (MongoDB's `$nor`, a `!` in Rust).
    pub fn negated(&self) -> Expr {
        match self {
            Expr::And(all) => Expr::Or(all.iter().map(Expr::negated).collect()),
            Expr::Or(any) => Expr::And(any.iter().map(Expr::negated).collect()),
            Expr::Not(inner) => inner.without_not(),
            Expr::Compare(c) => Expr::Compare(Comparison {
                op: c.op.negated(),
                ..c.clone()
            }),
            Expr::Has(c) => Expr::Null(c.clone()),
            Expr::Null(c) => Expr::Has(c.clone()),
        }
    }

    /// The same filter with every [`Expr::Not`] pushed down by [`Expr::negated`].
    pub fn without_not(&self) -> Expr {
        match self {
            Expr::And(all) => Expr::And(all.iter().map(Expr::without_not).collect()),
            Expr::Or(any) => Expr::Or(any.iter().map(Expr::without_not).collect()),
            Expr::Not(inner) => inner.negated(),
            leaf => leaf.clone(),
        }
    }
}

/// The filter `text` as an [`Expr`] over `res`, or `None` when it is empty. Every refusal is
/// [`Error::Invalid`], naming the filter and the 1-based position of what it refuses.
pub fn parse(res: &Resource, text: &str) -> Result<Option<Expr>> {
    syntax::parse(text)?
        .map(|node| resolve::resolve(res, text, node))
        .transpose()
}

/// The error for what `src` holds at `at`.
pub(crate) fn refuse(src: &str, at: usize, what: impl std::fmt::Display) -> Error {
    Error::Invalid(format!("filter {src:?}: {what} (at position {at})"))
}
