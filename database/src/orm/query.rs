//! A listing's `filter` and `order_by` as SQL. Both arrive from a request, so column names
//! are resolved through the descriptor and quoted by sea-query, and values are bound, never
//! interpolated. Go's `buildWhere`, `sqlOperator` and `orderClause` in `orm/filter.go`.

use sea_query::{Alias, BinOper, Condition, Expr, ExprTrait, Order};

use super::Dialect;
use super::values::{bind, coerce};
use crate::records::{Resource, core};
use crate::{Error, Result};

/// An AIP-160 expression as a bound predicate: [`core::parse_filter`]'s conjunction, each
/// column looked up in `res` and each value typed by its kind. A column the resource does not
/// declare is refused by name rather than ignored, which would return every row.
pub(crate) fn filter(d: Dialect, res: &Resource, expr: &str) -> Result<Condition> {
    let mut cond = Condition::all();
    for c in core::parse_filter(expr)? {
        let col = res.lookup_column(&c.column).ok_or_else(|| {
            Error::Invalid(format!(
                "filter names {:?}, which resource {:?} has no column for",
                c.column, res.name
            ))
        })?;
        let v = coerce(col.kind, &c.value)
            .map_err(|e| Error::Invalid(format!("filter on {:?}: {e}", c.column)))?;
        cond = cond
            .add(Expr::col(Alias::new(&col.name)).binary(operator(&c.op), bind(d, col.kind, &v)?));
    }
    Ok(cond)
}

/// A comparison as SQL; `!=` renders as `<>`. Go's `sqlOperator`.
fn operator(op: &str) -> BinOper {
    match op {
        "!=" => BinOper::NotEqual,
        ">" => BinOper::GreaterThan,
        ">=" => BinOper::GreaterThanOrEqual,
        "<" => BinOper::SmallerThan,
        "<=" => BinOper::SmallerThanOrEqual,
        _ => BinOper::Equal,
    }
}

/// An AIP-132 order expression, `field [desc], other`, as columns and directions, ending in
/// the key so an offset page is stable. Any field `res` does not declare orders by the key
/// alone instead of failing, as Go's `orderClause` does: ordering is presentation, and the
/// right records in an unexpected order beat an error. A direction other than `desc` is
/// ascending.
pub(crate) fn order(res: &Resource, expr: &str) -> Vec<(String, Order)> {
    let key = (res.pk_column.clone(), Order::Asc);
    let mut out = Vec::new();
    for item in expr.split(',').filter(|s| !s.trim().is_empty()) {
        let mut words = item.split_whitespace();
        let Some(col) = words.next().and_then(|f| res.lookup_column(f)) else {
            return vec![key];
        };
        let dir = match words.next() {
            Some(w) if w.eq_ignore_ascii_case("desc") => Order::Desc,
            _ => Order::Asc,
        };
        out.push((col.name.clone(), dir));
    }
    if !out.iter().any(|(c, _)| *c == res.pk_column) {
        out.push(key);
    }
    out
}
