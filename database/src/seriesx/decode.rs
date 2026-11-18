//! Strict decoding of stored records: a missing column, or a value of the
//! wrong kind or out of the field's range, is an error rather than a default.
use crate::store::{Error, Record, Value};

fn take(record: &mut Record, name: &str) -> crate::Result<Value> {
    record
        .remove(name)
        .ok_or_else(|| Error::Invalid(format!("missing column {name}")))
}

fn wrong(name: &str, want: &str, got: &Value) -> Error {
    Error::Invalid(format!("column {name}: expected {want}, got {got}"))
}

/// Takes `name` out of `record` as a string.
pub fn string(record: &mut Record, name: &str) -> crate::Result<String> {
    match take(record, name)? {
        Value::String(s) => Ok(s),
        other => Err(wrong(name, "a string", &other)),
    }
}

/// Takes `name` out of `record` as a signed integer that fits `T`.
pub fn int<T: TryFrom<i64>>(record: &mut Record, name: &str) -> crate::Result<T> {
    let value = take(record, name)?;
    value
        .as_i64()
        .and_then(|n| T::try_from(n).ok())
        .ok_or_else(|| wrong(name, "an integer in range", &value))
}

/// Takes `name` out of `record` as an unsigned integer that fits `T`.
pub fn uint<T: TryFrom<u64>>(record: &mut Record, name: &str) -> crate::Result<T> {
    let value = take(record, name)?;
    value
        .as_u64()
        .and_then(|n| T::try_from(n).ok())
        .ok_or_else(|| wrong(name, "an unsigned integer in range", &value))
}

/// Takes `name` out of `record` as a float; an integer widens to one.
pub fn float(record: &mut Record, name: &str) -> crate::Result<f64> {
    let value = take(record, name)?;
    value
        .as_f64()
        .ok_or_else(|| wrong(name, "a number", &value))
}

/// Takes `name` out of `record` as a boolean.
pub fn boolean(record: &mut Record, name: &str) -> crate::Result<bool> {
    let value = take(record, name)?;
    value
        .as_bool()
        .ok_or_else(|| wrong(name, "a boolean", &value))
}
