//! A record as the store holds it: one JSON object of its columns, each typed by its column's
//! kind, so the file reads plainly and a value reads back as the kind it was written as.
//!
//! JSON has no bytes, times or non-finite floats, so those are strings: bytes in standard
//! base64, times in RFC 3339 with nanoseconds, and `NaN`, `Infinity` and `-Infinity`. A
//! `u64` past 2^53 stays exact, since serde_json keeps integers as integers.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Map, Number, Value as Json};

use crate::records::{Kind, Resource, Row, Value};
use crate::{Error, Result};

/// `row` as a JSON object.
pub(super) fn encode(row: &Row) -> Json {
    Json::Object(
        row.iter()
            .map(|(k, v)| (k.clone(), to_json(v)))
            .collect::<Map<_, _>>(),
    )
}

/// A stored object as a row of `res`'s columns; a column the object lacks is null.
pub(super) fn decode(res: &Resource, json: &Json) -> Result<Row> {
    let Json::Object(obj) = json else {
        return Err(corrupt(res, "a record is not a JSON object"));
    };
    let mut row = Row::new();
    for col in &res.columns {
        let v = match obj.get(&col.name) {
            None | Some(Json::Null) => Value::Null,
            Some(j) => from_json(col.kind, j)
                .ok_or_else(|| corrupt(res, &format!("column {:?} holds {j}", col.name)))?,
        };
        row.insert(col.name.clone(), v);
    }
    Ok(row)
}

fn to_json(v: &Value) -> Json {
    match v {
        Value::Null => Json::Null,
        Value::String(s) => Json::String(s.clone()),
        Value::Int(n) => Json::Number((*n).into()),
        Value::Uint(n) => Json::Number((*n).into()),
        Value::Float(f) => match Number::from_f64(*f) {
            Some(n) => Json::Number(n),
            None if f.is_nan() => Json::String("NaN".into()),
            None if *f > 0.0 => Json::String("Infinity".into()),
            None => Json::String("-Infinity".into()),
        },
        Value::Bool(b) => Json::Bool(*b),
        Value::Bytes(b) => Json::String(STANDARD.encode(b)),
        Value::Timestamp(t) => Json::String(t.to_rfc3339_opts(SecondsFormat::Nanos, true)),
    }
}

fn from_json(kind: Kind, j: &Json) -> Option<Value> {
    Some(match (kind, j) {
        (Kind::Int | Kind::Enum, Json::Number(n)) => Value::Int(n.as_i64()?),
        (Kind::Uint, Json::Number(n)) => Value::Uint(n.as_u64()?),
        (Kind::Float, Json::Number(n)) => Value::Float(n.as_f64()?),
        (Kind::Float, Json::String(s)) => Value::Float(match s.as_str() {
            "NaN" => f64::NAN,
            "Infinity" => f64::INFINITY,
            "-Infinity" => f64::NEG_INFINITY,
            _ => return None,
        }),
        (Kind::Bool, Json::Bool(b)) => Value::Bool(*b),
        (Kind::Bytes, Json::String(s)) => Value::Bytes(STANDARD.decode(s).ok()?),
        (Kind::Timestamp, Json::String(s)) => {
            Value::Timestamp(DateTime::parse_from_rfc3339(s).ok()?.with_timezone(&Utc))
        }
        (Kind::String | Kind::Unknown, Json::String(s)) => Value::String(s.clone()),
        _ => return None,
    })
}

fn corrupt(res: &Resource, what: &str) -> Error {
    Error::Invalid(format!("tauri-store: resource {:?}: {what}", res.name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_kind_reads_back_as_written() {
        for (kind, v) in [
            (Kind::Int, Value::Int(-7)),
            (Kind::Uint, Value::Uint(u64::MAX)),
            (Kind::Float, Value::Float(f64::NAN)),
            (Kind::Float, Value::Float(f64::NEG_INFINITY)),
            (Kind::Float, Value::Float(2.5)),
            (Kind::Bool, Value::Bool(true)),
            (Kind::Bytes, Value::Bytes(vec![0, 255])),
            (
                Kind::Timestamp,
                Value::Timestamp(DateTime::from_timestamp(1, 5).unwrap()),
            ),
            (Kind::String, Value::String("x".into())),
        ] {
            let back = from_json(kind, &to_json(&v)).unwrap();
            assert_eq!(format!("{back:?}"), format!("{v:?}"), "{kind:?}");
        }
    }
}
