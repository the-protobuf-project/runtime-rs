//! Values on their way into SQL: a filter's text typed by its column's kind, and a column
//! value bound as the parameter type its dialect accepts. Go's `coerce` in `orm/filter.go`;
//! Go binds `any` and lets the database driver choose, which sqlx's typed parameters do not.

use sea_query::{Expr, Value as Sql};

use super::Dialect;
use crate::records::{Column, Kind, Resource, Value};
use crate::{Error, Result};

pub(crate) use crate::records::core::coerce;

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

/// `v`, bound for column `col`: on Postgres, an enum stored by label is cast to its enum
/// type (the column's `sql_type`, verbatim), since a text parameter does not convert to an
/// enum implicitly. Everywhere else, the bound value as it is.
pub(crate) fn expr(d: Dialect, col: Option<&Column>, v: Sql) -> Expr {
    match col {
        Some(c) if d == Dialect::Postgres && !c.labels.is_empty() && !c.sql_type.is_empty() => {
            Expr::cust_with_values(format!("CAST($1 AS {})", c.sql_type), [v])
        }
        _ => Expr::val(v),
    }
}
