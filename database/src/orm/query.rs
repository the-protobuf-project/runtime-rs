//! A listing's `filter` and `order_by` as SQL. Both arrive from a request, so column names
//! are resolved through the descriptor and quoted by sea-query, and values are bound, never
//! interpolated. Go's `buildWhere`, `sqlOperator` and `orderClause` in `orm/filter.go`.

use sea_query::{Alias, BinOper, Condition, Expr as Sql, ExprTrait, Order};

use super::Dialect;
use super::values::bind;
use crate::Result;
use crate::records::core::filter::{self, Expr, Op};
use crate::records::{Resource, core};

/// An AIP-160 expression as a bound predicate: [`filter::parse`]'s tree, columns resolved
/// through `res` (an undeclared one is refused by name rather than ignored, which would
/// return every row), values bound as their column's kind. `AND`, `OR` and `NOT` are
/// sea-query's `all`, `any` and `not`; SQL's own three-valued logic is the null rule every
/// other provider follows.
pub(crate) fn filter(d: Dialect, res: &Resource, expr: &str) -> Result<Condition> {
    match filter::parse(res, expr)? {
        None => Ok(Condition::all()),
        Some(e) => condition(d, &e),
    }
}

fn condition(d: Dialect, e: &Expr) -> Result<Condition> {
    let fold = |mut cond: Condition, all: &[Expr]| -> Result<Condition> {
        for e in all {
            cond = cond.add(condition(d, e)?);
        }
        Ok(cond)
    };
    Ok(match e {
        Expr::And(all) => fold(Condition::all(), all)?,
        Expr::Or(any) => fold(Condition::any(), any)?,
        Expr::Not(inner) => condition(d, inner)?.not(),
        Expr::Compare(c) => Condition::all().add(
            Sql::col(Alias::new(&c.column)).binary(operator(c.op), bind(d, c.kind, &c.value)?),
        ),
        Expr::Has(c) => Condition::all().add(Sql::col(Alias::new(c)).is_not_null()),
        Expr::Null(c) => Condition::all().add(Sql::col(Alias::new(c)).is_null()),
    })
}

/// A comparison as SQL; `!=` renders as `<>`. Go's `sqlOperator`.
fn operator(op: Op) -> BinOper {
    match op {
        Op::Eq => BinOper::Equal,
        Op::Ne => BinOper::NotEqual,
        Op::Gt => BinOper::GreaterThan,
        Op::Ge => BinOper::GreaterThanOrEqual,
        Op::Lt => BinOper::SmallerThan,
        Op::Le => BinOper::SmallerThanOrEqual,
    }
}

/// [`core::order_by`] as sea-query orders.
pub(crate) fn order(res: &Resource, expr: &str) -> Vec<(String, Order)> {
    core::order_by(res, expr)
        .into_iter()
        .map(|(c, desc)| (c, if desc { Order::Desc } else { Order::Asc }))
        .collect()
}
