//! A parsed filter's fields resolved to the resource's columns and its values typed by
//! their kind, with what no provider can evaluate refused by name.

use super::syntax::{Node, Restriction};
use super::{Comparison, Expr, Op, refuse};
use crate::Result;
use crate::records::core::coerce;
use crate::records::{Kind, Resource};

/// `node` over `res`; `src` is the filter's text, for errors.
pub(super) fn resolve(res: &Resource, src: &str, node: Node) -> Result<Expr> {
    let all = |nodes: Vec<Node>| -> Result<Vec<Expr>> {
        nodes.into_iter().map(|n| resolve(res, src, n)).collect()
    };
    Ok(match node {
        Node::And(nodes) => Expr::And(all(nodes)?),
        Node::Or(nodes) => Expr::Or(all(nodes)?),
        Node::Not(inner) => Expr::Not(Box::new(resolve(res, src, *inner)?)),
        Node::Restriction(r) => restriction(res, src, r)?,
    })
}

fn restriction(res: &Resource, src: &str, r: Restriction) -> Result<Expr> {
    let col = res.lookup_column(&r.field).ok_or_else(|| {
        let what = format!("{:?} is not a column of resource {:?}", r.field, res.name);
        refuse(src, r.at, what)
    })?;
    let (column, arg) = (col.name.clone(), &r.arg);
    let unquoted = |word: &str| !arg.quoted && arg.text == word;
    if r.op == ":" {
        if unquoted("*") {
            return Ok(Expr::Has(column));
        }
        let what = format!(
            "the has operator takes `*` here (`{}:*`, the column holds a value); a value after \
             it tests membership of a repeated field or map, which columns are not",
            r.field
        );
        return Err(refuse(src, arg.at, what));
    }
    if unquoted("null") {
        return match r.op {
            "=" => Ok(Expr::Null(column)),
            "!=" => Ok(Expr::Has(column)),
            op => Err(refuse(
                src,
                arg.at,
                format!("null compares with = and != only, not `{op}`"),
            )),
        };
    }
    let op = match r.op {
        "=" => Op::Eq,
        "!=" => Op::Ne,
        "<" => Op::Lt,
        "<=" => Op::Le,
        ">" => Op::Gt,
        _ => Op::Ge,
    };
    if arg.wild && matches!(op, Op::Eq | Op::Ne) {
        let what = "a `*` wildcard is not supported, so no provider matches it differently; \
                    escape a literal star as `\\*` inside quotes";
        return Err(refuse(src, arg.at, what));
    }
    if col.kind == Kind::Bytes && !matches!(op, Op::Eq | Op::Ne) {
        let what = format!("bytes column {:?} compares with = and != only", col.name);
        return Err(refuse(src, r.at, what));
    }
    let value = coerce(col.kind, &arg.text).map_err(|e| {
        let hint = if is_duration(&arg.text) {
            "; durations are not supported, as no column kind stores a Duration"
        } else {
            ""
        };
        refuse(
            src,
            arg.at,
            format!("column {:?} is {}: {e}{hint}", col.name, col.kind),
        )
    })?;
    Ok(Expr::Compare(Comparison {
        column,
        kind: col.kind,
        op,
        value,
    }))
}

/// Whether `text` spells an AIP-160 duration: a number of seconds suffixed `s`.
fn is_duration(text: &str) -> bool {
    text.strip_suffix('s')
        .is_some_and(|n| !n.is_empty() && n.parse::<f64>().is_ok())
}
