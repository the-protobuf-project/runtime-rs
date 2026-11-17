//! An [`Expr`] evaluated over a row in memory: what a provider without a query language uses,
//! and the reference every translating provider agrees with.

use std::cmp::Ordering;

use super::Expr;
use crate::records::{Row, Value};

impl Expr {
    /// Whether `row` matches. A comparison or contains on a null or absent column is false,
    /// and [`Expr::Not`] is true wherever its operand is false.
    pub fn matches(&self, row: &Row) -> bool {
        match self {
            Expr::And(all) => all.iter().all(|e| e.matches(row)),
            Expr::Or(any) => any.iter().any(|e| e.matches(row)),
            Expr::Not(inner) => !inner.matches(row),
            Expr::Compare(c) => row
                .get(&c.column)
                .and_then(|have| compare(have, &c.value))
                .is_some_and(|o| c.op.holds(o)),
            Expr::Null(column) => row.get(column).is_none_or(|v| *v == Value::Null),
            Expr::Contains(c) => match row.get(&c.column) {
                Some(Value::String(s)) => s.to_lowercase().contains(&c.value.to_lowercase()),
                _ => false,
            },
        }
    }
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
