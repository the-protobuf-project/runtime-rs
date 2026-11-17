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

/// [`core::order_by`] as sea-query orders.
pub(crate) fn order(res: &Resource, expr: &str) -> Vec<(String, Order)> {
    core::order_by(res, expr)
        .into_iter()
        .map(|(c, desc)| (c, if desc { Order::Desc } else { Order::Asc }))
        .collect()
}
