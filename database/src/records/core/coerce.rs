//! A filter's text typed by the kind of the column it compares. Go's `coerce` in
//! `orm/filter.go`, shared by every backend that evaluates filters.

use chrono::{DateTime, Utc};

use crate::records::{Kind, Value};

/// A filter's textual value as the type its column stores, so a comparison against a number
/// is a number rather than text the engine casts on every row. Go's `coerce`: integers in
/// base 10, booleans as Go's `strconv.ParseBool` reads them, times in RFC 3339.
pub fn coerce(kind: Kind, raw: &str) -> std::result::Result<Value, String> {
    let bad = |what: &str| format!("{raw:?} is not {what}");
    Ok(match kind {
        Kind::Int | Kind::Enum => Value::Int(raw.parse().map_err(|_| bad("an integer"))?),
        Kind::Uint => match raw.strip_prefix('+') {
            Some(_) => return Err(bad("an unsigned integer")),
            None => Value::Uint(raw.parse().map_err(|_| bad("an unsigned integer"))?),
        },
        Kind::Float => Value::Float(raw.parse().map_err(|_| bad("a number"))?),
        Kind::Bool => Value::Bool(match raw {
            "1" | "t" | "T" | "true" | "TRUE" | "True" => true,
            "0" | "f" | "F" | "false" | "FALSE" | "False" => false,
            _ => return Err(bad("a boolean")),
        }),
        Kind::Timestamp => Value::Timestamp(
            DateTime::parse_from_rfc3339(raw)
                .map_err(|_| bad("an RFC 3339 timestamp"))?
                .with_timezone(&Utc),
        ),
        Kind::Bytes => Value::Bytes(raw.as_bytes().to_vec()),
        Kind::String | Kind::Unknown => Value::String(raw.to_owned()),
    })
}
