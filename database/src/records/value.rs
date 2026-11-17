//! Column values: what Go's `map[string]any` holds between a message and a backend.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};

/// One column's value. Go keeps these as `any`; here the set is closed, so a driver matches
/// every case it must encode.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// No value: an unset optional field, or SQL `NULL`.
    Null,
    /// A string.
    String(String),
    /// A signed integer.
    Int(i64),
    /// An unsigned integer.
    Uint(u64),
    /// A float.
    Float(f64),
    /// A boolean.
    Bool(bool),
    /// Bytes.
    Bytes(Vec<u8>),
    /// A UTC time.
    Timestamp(DateTime<Utc>),
}

impl Value {
    /// Whether this is null, or an empty string: what Go's `core.IsEmpty` treats as unset.
    pub fn is_empty(&self) -> bool {
        match self {
            Value::Null => true,
            Value::String(s) => s.is_empty(),
            _ => false,
        }
    }

    /// The value as a key string, as Go formats a key: the string itself, or its digits.
    pub fn key_string(&self) -> String {
        match self {
            Value::Null => String::new(),
            Value::String(s) => s.clone(),
            Value::Int(n) => n.to_string(),
            Value::Uint(n) => n.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
            Value::Timestamp(t) => t.to_rfc3339(),
        }
    }
}

/// A record as backend columns: column name to value. Go's `map[string]any`.
pub type Row = BTreeMap<String, Value>;
