//! AIP-160 filters (<https://google.aip.dev/160>) as protoc-gen-store's generated Go
//! `filterx` reads them, parsed into an [`Expr`] whose columns are resolved and whose values
//! are typed against a [`Resource`], so a Rust service accepts the filters a Go one does
//! and every provider here matches the same records.
//!
//! - **Logic:** `AND` (any case) or whitespace, `OR` and `NOT` (capitals only), `-` before a
//!   term, and parentheses, nested at most [`MAX_DEPTH`] deep. `OR` binds tighter than
//!   `AND`: `a AND b OR c` is `a AND (b OR c)`.
//! - **Values:** a bareword (`SUMMER25`) or a double-quoted string without escapes.
//! - **By column kind,** as Go's gorm engine: a string takes `=`, `!=` and `:`
//!   (case-insensitive contains); a number every comparator but `:`; a timestamp `=`,
//!   `<`, `<=`, `>` and `>=`, as RFC 3339 or a bare date; a boolean, an enum (by number or
//!   value name, prefix optional) and bytes `=` and `!=`. Anything else is refused.
//! - **Free text:** a value with no operator is a case-insensitive contains over
//!   [`Resource::search`], any column matching; a resource without search columns refuses it.
//! - **Nulls:** a comparison or contains on a null column is false, `!=` included. `NOT`
//!   is true wherever its operand is not, so `NOT a = 1` matches a null `a`, as Go's
//!   `NOT coalesce(..., false)` does.
//!
//! ```
//! use database::records::core::filter::{self, Expr, Op};
//! # let md = prost_reflect::DescriptorPool::global()
//! #     .get_message_by_name("google.protobuf.Timestamp").unwrap();
//! # use database::records::{Column, Kind, Resource, Value};
//! let res = Resource::new(md, "t", "seconds", vec![Column::new("seconds", Kind::Int)]);
//! let Some(Expr::Not(inner)) = filter::parse(&res, "-seconds > 5").unwrap() else { panic!() };
//! let Expr::Compare(c) = *inner else { panic!() };
//! assert_eq!((c.op, c.value), (Op::Gt, Value::Int(5)));
//! ```

mod eval;
mod lex;
mod resolve;
mod simple;
pub(crate) mod syntax;

pub use eval::compare;
pub use syntax::MAX_DEPTH;

use crate::records::{Kind, ListOptions, Resource, Value};
use crate::{Error, Result};

/// A filter, resolved: columns are the backend names [`Resource`] declares, values are typed
/// by their column's [`Kind`].
#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    /// Every one holds.
    And(Vec<Expr>),
    /// Any one holds.
    Or(Vec<Expr>),
    /// The inner expression does not hold, including where it compares a null column.
    Not(Box<Expr>),
    /// `column op value`. False on a null column.
    Compare(Comparison),
    /// The string column holds `value`, ignoring case: `column : value`, or one column of a
    /// free-text term. False on a null column. `%`, `_` and `*` are literal.
    Contains(Contains),
    /// The column, by backend name, is null or absent. Never read from filter text, whose
    /// grammar, as Go's, has no null test: a caller restricts with it in code
    /// ([`ListOptions::restrict`](crate::records::ListOptions::restrict)), as Go's
    /// repository adds `delete_time IS NULL` outside `filterx.Parse`.
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

/// A case-insensitive substring test on a string column.
#[derive(Clone, Debug, PartialEq)]
pub struct Contains {
    /// The column's backend name.
    pub column: String,
    /// The text to find, as written.
    pub value: String,
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

/// The filter `text` as an [`Expr`] over `res`, or `None` when it holds no term (an empty
/// filter, or only `AND`s). Every refusal is [`Error::Invalid`], naming the filter and the
/// 1-based position of what it refuses, in Go's words.
pub fn parse(res: &Resource, text: &str) -> Result<Option<Expr>> {
    syntax::parse(text)?
        .map(|node| resolve::resolve(res, text, node))
        .transpose()
}

/// What a listing selects: `opts.filter` parsed over `res`, and every one of
/// `opts.restrict`, all ANDed; `None` when neither says anything.
pub fn listing(res: &Resource, opts: &ListOptions) -> Result<Option<Expr>> {
    let mut all: Vec<Expr> = parse(res, &opts.filter)?.into_iter().collect();
    all.extend(opts.restrict.iter().cloned());
    Ok(match all.len() {
        0 => None,
        1 => all.pop(),
        _ => Some(Expr::And(all)),
    })
}

/// The error for what `src` holds at `at`.
pub(crate) fn refuse(src: &str, at: usize, what: impl std::fmt::Display) -> Error {
    Error::Invalid(format!("filter {src:?}: {what} (at position {at})"))
}
