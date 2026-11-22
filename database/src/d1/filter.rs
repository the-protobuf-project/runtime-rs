//! A listing's filter and order as bound SQL. Both arrive from a request, so columns are
//! resolved through the descriptor and values are bound, never interpolated. Go's
//! `d1/filter.go`.

use super::sql::{bind, quote};
use crate::Result;
use crate::records::core::filter::{self, Expr, Op};
use crate::records::{ListOptions, Resource};

/// A listing's filter and restrictions as a bound predicate, or `None` when there are none.
///
/// - `AND` and `OR` group; `NOT x` is `NOT COALESCE(x, 0)`, so it holds where `x` compares
///   a null column, as the SQL driver writes it.
/// - A restriction's [`Expr::Null`] is `column IS NULL`.
/// - Contains is `LOWER(column) LIKE '%value%' ESCAPE '!'`, the value lowercased and its
///   wildcards escaped.
pub(crate) fn filter(res: &Resource, opts: &ListOptions) -> Result<Option<(String, Vec<String>)>> {
    let Some(e) = filter::listing(res, opts)? else {
        return Ok(None);
    };
    let mut args = Vec::new();
    let sql = predicate(&e, &mut args);
    Ok(Some((sql, args)))
}

fn predicate(e: &Expr, args: &mut Vec<String>) -> String {
    let group = |all: &[Expr], joiner: &str, empty: &str, args: &mut Vec<String>| {
        if all.is_empty() {
            return empty.to_owned();
        }
        let parts: Vec<String> = all.iter().map(|e| predicate(e, args)).collect();
        format!("({})", parts.join(joiner))
    };
    match e {
        Expr::And(all) => group(all, " AND ", "1 = 1", args),
        Expr::Or(any) => group(any, " OR ", "1 = 0", args),
        Expr::Not(inner) => format!("NOT COALESCE({}, 0)", predicate(inner, args)),
        Expr::Compare(c) => {
            // A comparison's value is never null; bound as every write binds it.
            args.push(bind(&c.value).unwrap_or_default());
            format!("{} {} ?", quote(&c.column), operator(c.op))
        }
        Expr::Null(column) => format!("{} IS NULL", quote(column)),
        Expr::Contains(c) => {
            args.push(format!("%{}%", escape(&c.value.to_lowercase())));
            format!("LOWER({}) LIKE ? ESCAPE '!'", quote(&c.column))
        }
    }
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

fn operator(op: Op) -> &'static str {
    match op {
        Op::Eq => "=",
        Op::Ne => "<>",
        Op::Gt => ">",
        Op::Ge => ">=",
        Op::Lt => "<",
        Op::Le => "<=",
    }
}

/// An AIP-132 order expression as SQL, every column resolved through the descriptor.
pub(crate) fn order(res: &Resource, expr: &str) -> String {
    crate::records::core::order_by(res, expr)
        .into_iter()
        .map(|(col, desc)| format!("{} {}", quote(&col), if desc { "DESC" } else { "ASC" }))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_contains_escapes_like_wildcards() {
        assert_eq!(escape("50%_off!"), "50!%!_off!!");
    }
}
