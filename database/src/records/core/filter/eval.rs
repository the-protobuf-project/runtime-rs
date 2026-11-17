//! An [`Expr`] evaluated over a row in memory: what a provider without a query language uses,
//! and the reference every translating provider agrees with.

use std::cmp::Ordering;

use super::Expr;
use crate::records::{Row, Value};

impl Expr {
    /// Whether `row` matches. A comparison with a null or absent column is false, and a
    /// [`Expr::Not`] is evaluated as [`Expr::negated`], so nulls fare as in SQL.
    pub fn matches(&self, row: &Row) -> bool {
        match self {
            Expr::And(all) => all.iter().all(|e| e.matches(row)),
            Expr::Or(any) => any.iter().any(|e| e.matches(row)),
            Expr::Not(inner) => inner.negated().matches(row),
            Expr::Compare(c) => row
                .get(&c.column)
                .and_then(|have| compare(have, &c.value))
                .is_some_and(|o| c.op.holds(o)),
            Expr::Has(c) => present(row.get(c)),
            Expr::Null(c) => !present(row.get(c)),
        }
    }
}

fn present(v: Option<&Value>) -> bool {
    v.is_some_and(|v| !matches!(v, Value::Null))
}

/// How two values of one column order; `None` when either is null or they are of different
/// kinds, which no comparison matches, as SQL's comparisons with `NULL` match nothing.
pub fn compare(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        (Value::Int(x), Value::Int(y)) => Some(x.cmp(y)),
        (Value::Uint(x), Value::Uint(y)) => Some(x.cmp(y)),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y),
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        (Value::Bytes(x), Value::Bytes(y)) => Some(x.cmp(y)),
        (Value::Timestamp(x), Value::Timestamp(y)) => Some(x.cmp(y)),
        _ => None,
    }
}
