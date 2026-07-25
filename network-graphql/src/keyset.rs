//! Keyset (cursor) pagination.
//!
//! Offset/limit paging is unstable under concurrent inserts: a row added before the
//! cursor shifts every later row by one, so the next page repeats or skips rows. Keyset
//! paging instead orders by a column and asks for rows strictly after the last one seen,
//! which is stable because the cursor is a value in the data, not a position.

use serde::Serialize;

use crate::columns::{OrderBy, OrderTerm};
use crate::predicate::{pred, Predicate};

/// Restricts a list to rows that sort strictly after `last` for the order term: `col
/// _gt last` when `term` is ascending, `col _lt last` when descending. `last` is the
/// order column's value from the last row of the previous page (the cursor). The order
/// column should be unique (e.g. an id or a strictly-increasing timestamp); a
/// non-unique column can skip or repeat rows at page boundaries.
pub fn after(term: &OrderTerm, last: impl Serialize) -> Predicate {
    let op = if term.dir == OrderBy::Desc {
        "_lt"
    } else {
        "_gt"
    };
    pred(&term.col, op, last)
}
