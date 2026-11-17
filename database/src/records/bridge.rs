//! Moves values between a proto message and a backend column map through the descriptor,
//! so no per-resource Rust types are needed. Go's `store/bridge.go`.

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use prost_reflect::{DynamicMessage, FieldDescriptor, Kind as ProtoKind, ReflectMessage};

use super::{Kind, Resource, Row, Value};
use crate::{Error, Result};

const TIMESTAMP: &str = "google.protobuf.Timestamp";

/// The message's columns. A managed column (no `field`) is skipped, for the driver to fill;
/// an unset optional field of a nullable column is [`Value::Null`]. Go's `MessageToColumns`.
pub fn message_to_columns(res: &Resource, msg: &impl ReflectMessage) -> Result<Row> {
    let msg = msg.transcode_to_dynamic();
    let mut out = Row::new();
    for col in res.columns.iter().filter(|c| !c.field.is_empty()) {
        let fd = field(res, &msg, &col.name, &col.field)?;
        if fd.supports_presence() && !msg.has_field(&fd) && !col.not_null {
            out.insert(col.name.clone(), Value::Null);
            continue;
        }
        let v = get_value(col.kind, &fd, &msg.get_field(&fd))
            .map_err(|e| column_error(res, &col.name, e))?;
        out.insert(col.name.clone(), v);
    }
    Ok(out)
}

/// A message of the resource's type from a row; missing and null columns stay unset. Go's
/// `ColumnsToMessage`.
pub fn columns_to_message(res: &Resource, row: &Row) -> Result<DynamicMessage> {
    let mut msg = res.new_message();
    for col in res.columns.iter().filter(|c| !c.field.is_empty()) {
        let Some(val) = row.get(&col.name).filter(|v| **v != Value::Null) else {
            continue;
        };
        let fd = field(res, &msg, &col.name, &col.field)?;
        let v = proto_value(col.kind, &fd, val).map_err(|e| column_error(res, &col.name, e))?;
        msg.set_field(&fd, v);
    }
    Ok(msg)
}

/// The message's primary-key value, as a string. Go's `KeyOf`.
pub fn key_of(res: &Resource, msg: &impl ReflectMessage) -> Result<String> {
    let pk = res
        .pk()
        .ok_or_else(|| Error::Invalid(format!("resource {:?} has no primary key", res.name)))?;
    let msg = msg.transcode_to_dynamic();
    let fd = field(res, &msg, &pk.name, &pk.field)?;
    Ok(match get_value(pk.kind, &fd, &msg.get_field(&fd)) {
        Ok(v) => v.key_string(),
        Err(_) => String::new(),
    })
}

/// Every row as a message. Go's `core.RowsToMessages`.
pub fn rows_to_messages(res: &Resource, rows: &[Row]) -> Result<Vec<DynamicMessage>> {
    rows.iter()
        .map(|row| columns_to_message(res, row))
        .collect()
}

fn field(
    res: &Resource,
    msg: &DynamicMessage,
    column: &str,
    name: &str,
) -> Result<FieldDescriptor> {
    msg.descriptor().get_field_by_name(name).ok_or_else(|| {
        Error::Invalid(format!(
            "resource {:?} column {column:?}: no proto field {name:?}",
            res.name
        ))
    })
}

fn column_error(res: &Resource, column: &str, err: String) -> Error {
    Error::Invalid(format!("resource {:?} column {column:?}: {err}", res.name))
}

fn get_value(
    kind: Kind,
    fd: &FieldDescriptor,
    v: &prost_reflect::Value,
) -> std::result::Result<Value, String> {
    use prost_reflect::Value as P;
    if kind == Kind::Timestamp {
        return match (fd.kind(), v) {
            (ProtoKind::Message(m), P::Message(tm)) if m.full_name() == TIMESTAMP => {
                let secs = tm
                    .get_field_by_name("seconds")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);
                let nanos = tm
                    .get_field_by_name("nanos")
                    .and_then(|v| v.as_i32())
                    .unwrap_or(0);
                DateTime::from_timestamp(secs, nanos.max(0) as u32)
                    .map(Value::Timestamp)
                    .ok_or_else(|| format!("timestamp {secs}s {nanos}ns is out of range"))
            }
            _ => Err(format!(
                "Timestamp kind on non-Timestamp field {:?}",
                fd.name()
            )),
        };
    }
    Ok(match (kind, v) {
        (Kind::Enum, P::EnumNumber(n)) => Value::Int(i64::from(*n)),
        (_, P::I32(n)) => Value::Int(i64::from(*n)),
        (_, P::I64(n)) => Value::Int(*n),
        (_, P::U32(n)) => Value::Uint(u64::from(*n)),
        (_, P::U64(n)) => Value::Uint(*n),
        (_, P::F32(f)) => Value::Float(f64::from(*f)),
        (_, P::F64(f)) => Value::Float(*f),
        (_, P::Bool(b)) => Value::Bool(*b),
        (_, P::Bytes(b)) => Value::Bytes(b.to_vec()),
        (_, P::String(s)) => Value::String(s.clone()),
        (_, P::EnumNumber(n)) => Value::Int(i64::from(*n)),
        (_, other) => return Err(format!("unsupported value {other:?} for {kind}")),
    })
}

fn proto_value(
    kind: Kind,
    fd: &FieldDescriptor,
    val: &Value,
) -> std::result::Result<prost_reflect::Value, String> {
    use prost_reflect::Value as P;
    match kind {
        Kind::Timestamp => {
            let t = to_time(val)?;
            let ProtoKind::Message(desc) = fd.kind() else {
                return Err(format!(
                    "Timestamp kind on non-message field {:?}",
                    fd.name()
                ));
            };
            let mut ts = DynamicMessage::new(desc);
            ts.set_field_by_name("seconds", P::I64(t.timestamp()));
            ts.set_field_by_name("nanos", P::I32(t.timestamp_subsec_nanos() as i32));
            return Ok(P::Message(ts));
        }
        Kind::Enum => return Ok(P::EnumNumber(to_i64(val)? as i32)),
        _ => {}
    }
    Ok(match fd.kind() {
        ProtoKind::String => P::String(match val {
            Value::String(s) => s.clone(),
            other => other.key_string(),
        }),
        ProtoKind::Bool => P::Bool(to_bool(val)?),
        ProtoKind::Bytes => P::Bytes(to_bytes(val)?.into()),
        ProtoKind::Int32 | ProtoKind::Sint32 | ProtoKind::Sfixed32 => P::I32(to_i64(val)? as i32),
        ProtoKind::Int64 | ProtoKind::Sint64 | ProtoKind::Sfixed64 => P::I64(to_i64(val)?),
        ProtoKind::Uint32 | ProtoKind::Fixed32 => P::U32(to_u64(val)? as u32),
        ProtoKind::Uint64 | ProtoKind::Fixed64 => P::U64(to_u64(val)?),
        ProtoKind::Float => P::F32(to_f64(val)? as f32),
        ProtoKind::Double => P::F64(to_f64(val)?),
        ProtoKind::Enum(_) => P::EnumNumber(to_i64(val)? as i32),
        other => {
            return Err(format!(
                "unsupported proto kind {other:?} for field {:?}",
                fd.name()
            ));
        }
    })
}

fn to_i64(v: &Value) -> std::result::Result<i64, String> {
    match v {
        Value::Int(n) => Ok(*n),
        Value::Uint(n) => Ok(*n as i64),
        Value::Float(f) => Ok(*f as i64),
        other => Err(format!("cannot convert {other:?} to a signed integer")),
    }
}

fn to_u64(v: &Value) -> std::result::Result<u64, String> {
    match v {
        Value::Uint(n) => Ok(*n),
        Value::Int(n) => Ok(*n as u64),
        Value::Float(f) => Ok(*f as u64),
        other => Err(format!("cannot convert {other:?} to an unsigned integer")),
    }
}

fn to_f64(v: &Value) -> std::result::Result<f64, String> {
    match v {
        Value::Float(f) => Ok(*f),
        Value::Int(n) => Ok(*n as f64),
        Value::Uint(n) => Ok(*n as f64),
        other => Err(format!("cannot convert {other:?} to a float")),
    }
}

fn to_bool(v: &Value) -> std::result::Result<bool, String> {
    match v {
        Value::Bool(b) => Ok(*b),
        Value::Int(n) => Ok(*n != 0),
        Value::Uint(n) => Ok(*n != 0),
        other => Err(format!("cannot convert {other:?} to a boolean")),
    }
}

fn to_bytes(v: &Value) -> std::result::Result<Vec<u8>, String> {
    match v {
        Value::Bytes(b) => Ok(b.clone()),
        Value::String(s) => Ok(s.as_bytes().to_vec()),
        other => Err(format!("cannot convert {other:?} to bytes")),
    }
}

/// A time from a value, as Go's `toTime` reads one: a time, epoch seconds, or one of the
/// layouts databases print timestamps in.
fn to_time(v: &Value) -> std::result::Result<DateTime<Utc>, String> {
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
