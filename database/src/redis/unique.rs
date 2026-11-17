//! The unique-column bookkeeping. Redis has no constraints, so a column the descriptor marks
//! unique is only unique if this backend makes it so. Go's `database/redis/unique.go`.

use prost_reflect::DynamicMessage;

use crate::records::{Resource, Value, message_to_columns};

/// One unique column and the value a message carries for it. Go's `uniqueValue`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct UniqueValue {
    /// The column.
    pub(super) column: String,
    /// The value as Go's `fmt.Sprint` prints it, so both runtimes reserve the same key.
    pub(super) value: String,
}

/// The unique, non-key columns of `msg` that carry a value. The key is left out because the
/// record key already enforces it; an empty value is left out so one record leaving a
/// column unset does not lock every other out of leaving it unset. Go's `uniqueValues`.
pub(super) fn unique_values(res: &Resource, msg: &DynamicMessage) -> Vec<UniqueValue> {
    let Ok(cols) = message_to_columns(res, msg) else {
        return Vec::new();
    };
    res.columns
        .iter()
        .filter(|c| c.unique && !c.primary_key)
        .filter_map(|c| {
            let value = go_sprint(cols.get(&c.name)?)?;
            (!value.is_empty()).then(|| UniqueValue {
                column: c.name.clone(),
                value,
            })
        })
        .collect()
}

/// A value as Go's `fmt.Sprint` prints the one Go's bridge holds; `None` for null.
fn go_sprint(v: &Value) -> Option<String> {
    Some(match v {
        Value::Null => return None,
        Value::String(s) => s.clone(),
        Value::Int(n) => n.to_string(),
        Value::Uint(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Float(f) => go_float(*f),
        Value::Bytes(b) => {
            let parts: Vec<String> = b.iter().map(u8::to_string).collect();
            format!("[{}]", parts.join(" "))
        }
        Value::Timestamp(t) => {
            let nanos = t.timestamp_subsec_nanos();
            let frac = if nanos == 0 {
                String::new()
            } else {
                format!(".{nanos:09}").trim_end_matches('0').to_owned()
            };
            format!("{}{frac} +0000 UTC", t.format("%Y-%m-%d %H:%M:%S"))
        }
    })
}

/// `%v` of a float64: the shortest digits, in exponent form below 1e-4 or from 1e6, as
/// `strconv` decides for the shortest precision.
fn go_float(f: f64) -> String {
    if f.is_nan() {
        return "NaN".into();
    }
    if f.is_infinite() {
        return if f > 0.0 { "+Inf" } else { "-Inf" }.into();
    }
    let sci = format!("{f:e}");
    let (mantissa, exp) = sci.split_once('e').unwrap_or((&sci, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    if f != 0.0 && !(-4..6).contains(&exp) {
        let sign = if exp < 0 { '-' } else { '+' };
        return format!("{mantissa}e{sign}{:02}", exp.abs());
    }
    f.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_print_as_go_prints_them() {
        for (v, want) in [
            (Value::Float(9.5), "9.5"),
            (Value::Float(1e6), "1e+06"),
            (Value::Float(123456.0), "123456"),
            (Value::Float(1e21), "1e+21"),
            (Value::Float(0.00001), "1e-05"),
            (Value::Float(0.0001), "0.0001"),
            (Value::Bytes(vec![1, 2]), "[1 2]"),
            (Value::Bytes(vec![]), "[]"),
            (Value::Uint(u64::MAX), "18446744073709551615"),
        ] {
            assert_eq!(go_sprint(&v).unwrap(), want, "{v:?}");
        }
        let t = chrono::DateTime::from_timestamp(0, 500_000_000).unwrap();
        assert_eq!(
            go_sprint(&Value::Timestamp(t)).unwrap(),
            "1970-01-01 00:00:00.5 +0000 UTC"
        );
    }
}
