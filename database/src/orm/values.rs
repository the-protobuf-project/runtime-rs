//! Values on their way into SQL: a filter's text typed by its column's kind, and a column
//! value bound as the parameter type its dialect accepts. Go's `coerce` in `orm/filter.go`;
//! Go binds `any` and lets the database driver choose, which sqlx's typed parameters do not.

use chrono::{DateTime, Utc};
use sea_query::Value as Sql;

use super::Dialect;
use crate::records::{Kind, Resource, Value};
use crate::{Error, Result};

/// A filter's textual value as the type its column stores, so a comparison against a number
/// is a number rather than text the engine casts on every row. Go's `coerce`: integers in
/// base 10, booleans as Go's `strconv.ParseBool` reads them, times in RFC 3339.
pub(crate) fn coerce(kind: Kind, raw: &str) -> std::result::Result<Value, String> {
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

/// `v` as a parameter of column kind `kind`. A null is typed by the kind, since Postgres
/// refuses a text-typed `NULL` for an integer column. An unsigned value is native only on
/// MySQL; elsewhere it is a signed 64-bit integer, and one past 2^63 is refused rather than
/// wrapped.
pub(crate) fn bind(d: Dialect, kind: Kind, v: &Value) -> Result<Sql> {
    Ok(match v {
        Value::Null => null(d, kind),
        Value::String(s) => Sql::String(Some(s.clone())),
        Value::Int(n) => Sql::BigInt(Some(*n)),
        Value::Uint(n) if d == Dialect::MySql => Sql::BigUnsigned(Some(*n)),
        Value::Uint(n) => Sql::BigInt(Some(i64::try_from(*n).map_err(|_| {
            Error::Invalid(format!(
                "{n} does not fit the signed 64-bit column {} stores unsigned integers in",
                d.name()
            ))
        })?)),
        Value::Float(f) => Sql::Double(Some(*f)),
        Value::Bool(b) => Sql::Bool(Some(*b)),
        Value::Bytes(b) => Sql::Bytes(Some(b.clone())),
        Value::Timestamp(t) => Sql::ChronoDateTimeUtc(Some(*t)),
    })
}

fn null(d: Dialect, kind: Kind) -> Sql {
    match kind {
        Kind::Int | Kind::Enum => Sql::BigInt(None),
        Kind::Uint if d == Dialect::MySql => Sql::BigUnsigned(None),
        Kind::Uint => Sql::BigInt(None),
        Kind::Float => Sql::Double(None),
        Kind::Bool => Sql::Bool(None),
        Kind::Bytes => Sql::Bytes(None),
        Kind::Timestamp => Sql::ChronoDateTimeUtc(None),
        Kind::String | Kind::Unknown => Sql::String(None),
    }
}

/// A record key as a parameter of the primary-key column's kind. Go passes the key as a
/// string and lets the database cast it; sqlx's typed parameters need it typed here.
pub(crate) fn key(d: Dialect, res: &Resource, key: &str) -> Result<(String, Sql)> {
    let pk = res
        .pk()
        .ok_or_else(|| Error::Invalid(format!("resource {:?} has no primary key", res.name)))?;
    let v = coerce(pk.kind, key)
        .map_err(|e| Error::Invalid(format!("key of resource {:?}: {e}", res.name)))?;
    Ok((pk.name.clone(), bind(d, pk.kind, &v)?))
}
