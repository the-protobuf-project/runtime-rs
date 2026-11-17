//! A listing's `filter` and `order_by` as SQL. Both arrive from a request, so column names
//! are resolved through the descriptor and quoted by sea-query, and values are bound, never
//! interpolated. Go's `buildWhere`, `sqlOperator` and `orderClause` in `orm/filter.go`.

use sea_query::{Alias, BinOper, Condition, Expr as Sql, ExprTrait, Func, LikeExpr, Order};

use super::Dialect;
use super::values::{self, bind};
use crate::Result;
use crate::records::core::filter::{self, Expr, Op};
use crate::records::{ListOptions, Resource, core};

/// An AIP-160 expression as a bound predicate, as Go's gorm engine writes it: columns
/// resolved through `res` by [`filter::parse`] (an undeclared one is refused by name rather
/// than ignored, which would return every row), values bound as their column's kind.
///
/// - `AND` and `OR` are sea-query's `all` and `any`.
/// - `NOT x` is `NOT COALESCE(x, FALSE)`, so it holds where `x` compares a null column.
/// - A restriction's [`Expr::Null`] is `column IS NULL`.
/// - Contains is `LOWER(column) LIKE '%value%' ESCAPE '!'`, the value lowercased and its
///   `%`, `_` and `!` escaped: Go's `ILIKE`, on every dialect.
pub(crate) fn filter(d: Dialect, res: &Resource, opts: &ListOptions) -> Result<Condition> {
    match filter::listing(res, opts)? {
        None => Ok(Condition::all()),
        Some(e) => condition(d, res, &e),
    }
}

fn condition(d: Dialect, res: &Resource, e: &Expr) -> Result<Condition> {
    let fold = |mut cond: Condition, all: &[Expr]| -> Result<Condition> {
        for e in all {
            cond = cond.add(condition(d, res, e)?);
        }
        Ok(cond)
    };
    let one = |e: Sql| Condition::all().add(e);
    Ok(match e {
        Expr::And(all) => fold(Condition::all(), all)?,
        Expr::Or(any) => fold(Condition::any(), any)?,
        Expr::Not(inner) => {
            let held = Sql::from(condition(d, res, inner)?);
            one(Func::coalesce([held, Sql::val(false)]).not())
        }
        Expr::Compare(c) => {
            let v = values::expr(d, res.lookup_column(&c.column), bind(d, c.kind, &c.value)?);
            one(Sql::col(Alias::new(&c.column)).binary(operator(c.op), v))
        }
        Expr::Null(column) => one(Sql::col(Alias::new(column)).is_null()),
        Expr::Contains(c) => {
            let pattern = format!("%{}%", escape(&c.value.to_lowercase()));
            let lower = Sql::from(Func::lower(Sql::col(Alias::new(&c.column))));
            one(lower.like(LikeExpr::new(pattern).escape('!')))
        }
    })
}

/// `v` with LIKE's wildcards and the escape character escaped, so they match literally.
fn escape(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        if matches!(c, '!' | '%' | '_') {
            out.push('!');
        }
        out.push(c);
    }
    out
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
