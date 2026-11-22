//! Values, predicates and types on their way into D1, and rows on their way out. Every value
//! crosses as text, so this is where a kind decides what that text is. Go's `d1/query.go`.

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Map, Value as Json};

use crate::records::{Column, Kind, Resource, Row, Value};
use crate::{Error, Result};

/// The most values D1 binds into one statement.
pub(crate) const MAX_PARAMS: usize = 100;

/// `v` as the text D1 binds, or `None` for a null, which has no text and is written as a
/// `NULL` literal instead.
pub(crate) fn bind(v: &Value) -> Option<String> {
    Some(match v {
        Value::Null => return None,
        Value::String(s) => s.clone(),
        Value::Int(n) => n.to_string(),
        Value::Uint(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => if *b { "1" } else { "0" }.to_owned(),
        Value::Bytes(b) => BASE64.encode(b),
        Value::Timestamp(t) => stamp(t),
    })
}

/// A timestamp at a fixed width, every fraction digit and always `Z`, so two compare as text
/// exactly as they compare as times.
fn stamp(t: &DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

/// One decoded result row as the values [`columns_to_message`](crate::records::columns_to_message)
/// reads, column by column through the descriptor.
pub(crate) fn row(res: &Resource, mut raw: Map<String, Json>) -> Result<Row> {
    let mut out = Row::new();
    for col in &res.columns {
        let Some(v) = raw.remove(&col.name) else {
            continue;
        };
        let value = from_wire(col, v)
            .map_err(|e| Error::Invalid(format!("d1: {}.{}: {e}", res.table, col.name)))?;
        out.insert(col.name.clone(), value);
    }
    Ok(out)
}

/// One JSON value as the kind its column holds.
fn from_wire(col: &Column, v: Json) -> std::result::Result<Value, String> {
    if v.is_null() {
        return Ok(Value::Null);
    }
    let text = || match &v {
        Json::String(s) => s.clone(),
        other => other.to_string(),
    };
    let bad = |what: &str| format!("{} is not {what}", text());
    Ok(match col.kind {
        Kind::Enum if !col.labels.is_empty() => Value::String(text()),
        Kind::Int | Kind::Enum => Value::Int(text().parse().map_err(|_| bad("an integer"))?),
        Kind::Uint => Value::Uint(text().parse().map_err(|_| bad("an unsigned integer"))?),
        Kind::Float => Value::Float(text().parse().map_err(|_| bad("a number"))?),
        Kind::Bool => match v {
            Json::Bool(b) => Value::Bool(b),
            _ => Value::Bool(text().parse::<f64>().map_err(|_| bad("a boolean"))? != 0.0),
        },
        Kind::Bytes => Value::Bytes(BASE64.decode(text()).map_err(|_| bad("base64"))?),
        Kind::Timestamp => Value::Timestamp(
            DateTime::parse_from_rfc3339(&text())
                .map_err(|_| bad("an RFC 3339 timestamp"))?
                .with_timezone(&Utc),
        ),
        Kind::String | Kind::Unknown => Value::String(text()),
    })
}

/// The SQLite type a column is stored as. The type picks the affinity, and the affinity is
/// what turns bound text back into what it was: `INTEGER` and `REAL` convert a numeric
/// string, `TEXT` keeps it. Bytes are base64, so `TEXT`: a numeric affinity would turn
/// base64 that happens to be all digits into a number.
pub(crate) fn column_type(col: &Column) -> &'static str {
    match col.kind {
        Kind::Enum if !col.labels.is_empty() => "TEXT",
        Kind::Int | Kind::Enum | Kind::Uint | Kind::Bool => "INTEGER",
        Kind::Float => "REAL",
        Kind::String | Kind::Bytes | Kind::Timestamp | Kind::Unknown => "TEXT",
    }
}

/// An identifier, double-quoted. Names come from a descriptor rather than a request, so
/// this is defense in depth, but a descriptor is data and data in an identifier gets quoted.
pub(crate) fn quote(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// `n` question marks, comma-separated.
pub(crate) fn placeholders(n: usize) -> String {
    vec!["?"; n].join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_stamps_are_fixed_width_so_text_order_is_time_order() {
        let whole = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 5).unwrap();
        let half = whole + chrono::Duration::milliseconds(500);
        let (a, b) = (stamp(&whole), stamp(&half));
        assert_eq!(a.len(), b.len());
        assert!(a < b, "{a} sorts after {b}");
    }

    #[test]
    fn test_bind_writes_bool_and_bytes_as_affinity_reads_them() {
        assert_eq!(bind(&Value::Bool(true)).unwrap(), "1");
        assert_eq!(bind(&Value::Bytes(vec![0, 1, 0xff])).unwrap(), "AAH/");
        assert_eq!(bind(&Value::Null), None);
    }
}
