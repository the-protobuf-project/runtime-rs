//! AIP-160 filters and AIP-132 orders as Mongo queries and sorts. Go's `mongodb/filter.go`
//! and the `parseFilter`/`sortOf` halves of `codec.go` and `driver.go`.

use std::collections::BTreeSet;

use bson::{Bson, DateTime, Document, doc};

use super::error::invalid;
use crate::Result;
use crate::records::{Kind, Resource, core};

/// The Mongo query for `expr`: the shared [`core::parse_filter`] subset, each column mapped to
/// its field (`_id` for the key) and each value typed by the column's kind, so a number
/// compares as a number. An unknown column is refused by name. Go's `parseFilter`.
pub(super) fn parse_filter(res: &Resource, expr: &str) -> Result<Document> {
    let mut clauses = Vec::new();
    for c in core::parse_filter(expr).map_err(|e| invalid(e.to_string()))? {
        let col = res.lookup_column(&c.column).ok_or_else(|| {
            invalid(format!(
                "filter names {:?}, which resource {:?} has no column for",
                c.column, res.name
            ))
        })?;
        let field = if col.primary_key { "_id" } else { &col.name };
        let typed = coerce(col.kind, &c.value)
            .map_err(|e| invalid(format!("filter on {:?}: {e}", c.column)))?;
        let cond = match c.op.as_str() {
            "=" => typed,
            op => Bson::Document(doc! { mongo_operator(op): typed }),
        };
        clauses.push((field.to_owned(), cond));
    }
    let fields: BTreeSet<_> = clauses.iter().map(|(f, _)| f).collect();
    if fields.len() == clauses.len() {
        return Ok(clauses.into_iter().collect());
    }
    // Go's map keeps one clause per field, so `age >= 2 AND age < 9` would lose a bound;
    // a field named twice is a conjunction of both instead.
    let all: Vec<Bson> = clauses
        .into_iter()
        .map(|(f, c)| Bson::Document(doc! { f: c }))
        .collect();
    Ok(doc! { "$and": all })
}

/// A comparison's Mongo operator. Go's `mongoOperator`.
fn mongo_operator(op: &str) -> &'static str {
    match op {
        "!=" => "$ne",
        ">" => "$gt",
        ">=" => "$gte",
        "<" => "$lt",
        "<=" => "$lte",
        _ => "$eq",
    }
}

/// A filter's text as the BSON its column stores. Go's `coerce`.
pub(super) fn coerce(kind: Kind, raw: &str) -> std::result::Result<Bson, String> {
    Ok(match kind {
        Kind::Int | Kind::Enum => Bson::Int64(
            raw.parse()
                .map_err(|_| format!("{raw:?} is not an integer"))?,
        ),
        Kind::Uint => {
            let n: u64 = raw
                .parse()
                .map_err(|_| format!("{raw:?} is not an unsigned integer"))?;
            Bson::Int64(i64::try_from(n).map_err(|_| {
                format!("{raw:?} is too large to compare; values past 2^63 are Decimal128")
            })?)
        }
        Kind::Float => Bson::Double(
            raw.parse()
                .map_err(|_| format!("{raw:?} is not a number"))?,
        ),
        Kind::Bool => Bson::Boolean(parse_bool(raw).ok_or(format!("{raw:?} is not a boolean"))?),
        Kind::Timestamp => {
            let t = chrono::DateTime::parse_from_rfc3339(raw)
                .map_err(|_| format!("{raw:?} is not an RFC 3339 timestamp"))?;
            Bson::DateTime(DateTime::from_millis(t.timestamp_millis()))
        }
        Kind::String | Kind::Bytes | Kind::Unknown => Bson::String(raw.to_owned()),
    })
}

/// What Go's `strconv.ParseBool` accepts.
fn parse_bool(raw: &str) -> Option<bool> {
    match raw {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
    }
}

/// The Mongo sort for an AIP-132 order: its first column, `desc` or ascending, the key as
/// `_id`. An unknown column sorts by `_id` rather than failing: order is presentation, and
/// the right records in an unexpected order beat an error. A column other than the key is
/// followed by `_id`, so equal values page in a stable order. Go's `sortOf`.
pub(super) fn sort_of(res: &Resource, order_by: &str) -> Document {
    let mut fields = order_by.split_whitespace();
    let Some(column) = fields.next() else {
        return doc! { "_id": 1 };
    };
    let direction = match fields.next() {
        Some(d) if d.eq_ignore_ascii_case("desc") => -1,
        _ => 1,
    };
    if column.eq_ignore_ascii_case(&res.pk_column) {
        return doc! { "_id": direction };
    }
    if res.lookup_column(column).is_none() {
        return doc! { "_id": 1 };
    }
    doc! { column: direction, "_id": 1 }
}
