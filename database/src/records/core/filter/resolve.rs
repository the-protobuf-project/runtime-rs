//! A parsed filter resolved against a resource as Go's gorm engine (`sqlCondition`) resolves
//! it: each field to its column, each value typed by the column's kind, and only the
//! operators that kind takes.

use chrono::{NaiveDate, Utc};
use prost_reflect::Kind as Proto;

use super::syntax::Node;
use super::{Comparison, Contains, Expr, Op, refuse};
use crate::Result;
use crate::records::core::coerce;
use crate::records::{Column, Kind, Resource, Value};

/// `node` over `res`; `src` is the filter's text, for errors.
pub(super) fn resolve(res: &Resource, src: &str, node: Node) -> Result<Expr> {
    let all = |nodes: Vec<Node>| -> Result<Vec<Expr>> {
        nodes.into_iter().map(|n| resolve(res, src, n)).collect()
    };
    match node {
        Node::And(nodes) => Ok(Expr::And(all(nodes)?)),
        Node::Or(nodes) => Ok(Expr::Or(all(nodes)?)),
        Node::Not(inner) => Ok(Expr::Not(Box::new(resolve(res, src, *inner)?))),
        Node::Search { text, at } => search(res, src, text, at),
        Node::Restriction {
            field,
            at,
            op,
            value,
            value_at,
        } => {
            let Some(col) = res.lookup_column(&field) else {
                return Err(refuse(src, at, format!("cannot filter by {field:?}")));
            };
            let unsupported = || refuse(src, at, format!("unsupported operator for {field:?}"));
            let typed = |kind| match (kind, value.as_str()) {
                (Kind::Timestamp, v) => timestamp(v),
                (Kind::Enum, v) => enum_number(res, col, v),
                (kind, v) => coerce(kind, v),
            };
            // Go parses an ordered or boolean value before it checks the operator.
            let early = match col.kind {
                Kind::String | Kind::Unknown | Kind::Bytes | Kind::Enum => None,
                kind => Some(typed(kind).map_err(|e| refuse(src, value_at, e))?),
            };
            let op = match (col.kind, op) {
                (Kind::String | Kind::Unknown, ":") => {
                    let column = col.name.clone();
                    return Ok(Expr::Contains(Contains { column, value }));
                }
                (_, ":") => return Err(unsupported()),
                (_, "=") => Op::Eq,
                (Kind::Timestamp, "!=") => return Err(unsupported()),
                (_, "!=") => Op::Ne,
                (Kind::Int | Kind::Uint | Kind::Float | Kind::Timestamp, op) => match op {
                    "<" => Op::Lt,
                    "<=" => Op::Le,
                    ">" => Op::Gt,
                    _ => Op::Ge,
                },
                _ => return Err(unsupported()),
            };
            let value = match early {
                Some(v) => v,
                None => typed(col.kind).map_err(|e| refuse(src, value_at, e))?,
            };
            let (column, kind) = (col.name.clone(), col.kind);
            Ok(Expr::Compare(Comparison {
                column,
                kind,
                op,
                value,
            }))
        }
    }
}

/// A free-text term: a case-insensitive contains over the search columns, any matching.
fn search(res: &Resource, src: &str, text: String, at: usize) -> Result<Expr> {
    if res.search.is_empty() {
        return Err(refuse(src, at, "free-text search is not supported here"));
    }
    let mut any = Vec::new();
    for name in &res.search {
        let col = res.lookup_column(name).ok_or_else(|| {
            let what = format!("search column {name:?} is not a column of {:?}", res.name);
            refuse(src, at, what)
        })?;
        let (column, value) = (col.name.clone(), text.clone());
        any.push(Expr::Contains(Contains { column, value }));
    }
    Ok(if any.len() == 1 {
        any.remove(0)
    } else {
        Expr::Or(any)
    })
}

/// RFC 3339, or a bare date as its midnight-UTC instant. Go's `ParseTimestamp`.
fn timestamp(v: &str) -> std::result::Result<Value, String> {
    if let Ok(Value::Timestamp(t)) = coerce(Kind::Timestamp, v) {
        return Ok(Value::Timestamp(t));
    }
    NaiveDate::parse_from_str(v, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|t| Value::Timestamp(t.and_utc().with_timezone(&Utc)))
        .ok_or_else(|| format!("{v:?} is not a timestamp (want RFC 3339)"))
}

/// An enum's number, from the number or the value name as Go's `NormalizeEnum` reads it:
/// any case, with or without the enum's prefix (`PUBLISHED`, `status_published`).
fn enum_number(res: &Resource, col: &Column, v: &str) -> std::result::Result<Value, String> {
    if let Ok(n) = v.parse::<i64>() {
        return Ok(Value::Int(n));
    }
    let field = res.message.get_field_by_name(&col.field);
    let Some(Proto::Enum(e)) = field.map(|f| f.kind()) else {
        return Err(format!("{v:?} is not an integer"));
    };
    let prefix = format!("{}_", screaming_snake(e.name()));
    let want = v.to_uppercase();
    let want = want.strip_prefix(&prefix).unwrap_or(&want);
    e.values()
        .find(|x| x.name().strip_prefix(&prefix).unwrap_or(x.name()) == want)
        .map(|x| Value::Int(i64::from(x.number())))
        .ok_or_else(|| format!("{v:?} is not a value of enum {}", e.full_name()))
}

/// `UnitType` as `UNIT_TYPE`. Go's `naming.ScreamingSnake`.
fn screaming_snake(name: &str) -> String {
    let mut out = String::new();
    let mut prev_lower = false;
    for c in name.chars() {
        if c.is_ascii_uppercase() && prev_lower {
            out.push('_');
        }
        prev_lower = c.is_ascii_lowercase() || c.is_ascii_digit();
        out.push(c.to_ascii_uppercase());
    }
    out
}
