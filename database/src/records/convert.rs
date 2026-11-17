//! Reads a column value as the type a proto field needs, accepting what databases hand
//! back. Go's `toInt64`, `toTime` and friends in `store/bridge.go`.

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};

use super::Value;

pub(super) fn to_i64(v: &Value) -> std::result::Result<i64, String> {
    match v {
        Value::Int(n) => Ok(*n),
        Value::Uint(n) => Ok(*n as i64),
        Value::Float(f) => Ok(*f as i64),
        other => Err(format!("cannot convert {other:?} to a signed integer")),
    }
}

pub(super) fn to_u64(v: &Value) -> std::result::Result<u64, String> {
    match v {
        Value::Uint(n) => Ok(*n),
        Value::Int(n) => Ok(*n as u64),
        Value::Float(f) => Ok(*f as u64),
        other => Err(format!("cannot convert {other:?} to an unsigned integer")),
    }
}

pub(super) fn to_f64(v: &Value) -> std::result::Result<f64, String> {
    match v {
        Value::Float(f) => Ok(*f),
        Value::Int(n) => Ok(*n as f64),
        Value::Uint(n) => Ok(*n as f64),
        other => Err(format!("cannot convert {other:?} to a float")),
    }
}

pub(super) fn to_bool(v: &Value) -> std::result::Result<bool, String> {
    match v {
        Value::Bool(b) => Ok(*b),
        Value::Int(n) => Ok(*n != 0),
        Value::Uint(n) => Ok(*n != 0),
        other => Err(format!("cannot convert {other:?} to a boolean")),
    }
}

pub(super) fn to_bytes(v: &Value) -> std::result::Result<Vec<u8>, String> {
    match v {
        Value::Bytes(b) => Ok(b.clone()),
        Value::String(s) => Ok(s.as_bytes().to_vec()),
        other => Err(format!("cannot convert {other:?} to bytes")),
    }
}

/// A time from a value, as Go's `toTime` reads one: a time, epoch seconds, or one of the
/// layouts databases print timestamps in.
pub(super) fn to_time(v: &Value) -> std::result::Result<DateTime<Utc>, String> {
    match v {
        Value::Timestamp(t) => Ok(*t),
        Value::Int(secs) => {
            DateTime::from_timestamp(*secs, 0).ok_or_else(|| format!("{secs} is out of range"))
        }
        Value::String(s) => parse_time(s),
        other => Err(format!("cannot convert {other:?} to a timestamp")),
    }
}

fn parse_time(s: &str) -> std::result::Result<DateTime<Utc>, String> {
    if let Ok(t) = DateTime::parse_from_rfc3339(s) {
        return Ok(t.with_timezone(&Utc));
    }
    for layout in ["%Y-%m-%d %H:%M:%S%.f%:z", "%Y-%m-%d %H:%M:%S%.f%#z"] {
        if let Ok(t) = DateTime::parse_from_str(s, layout) {
            return Ok(t.with_timezone(&Utc));
        }
    }
    for layout in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
    ] {
        if let Ok(t) = NaiveDateTime::parse_from_str(s, layout) {
            return Ok(t.and_utc());
        }
    }
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Ok(d.and_hms_opt(0, 0, 0).unwrap_or_default().and_utc());
    }
    Err(format!("cannot read {s:?} as a timestamp"))
}
