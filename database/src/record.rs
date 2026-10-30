//! Backend-neutral scalar records and descriptor validation.

use crate::{Error, Kind, Resource, Result, Role};
use std::collections::BTreeMap;

/// A scalar JSON value. Integer values retain their full 64-bit precision.
/// Descriptors determine the value domain; arrays and objects are rejected.
pub use serde_json::Value;

/// One record keyed by stored column names, not by SDK field types.
pub type Record = BTreeMap<String, Value>;

impl Resource {
    /// Checks a complete point before I/O. Every declared column is required.
    /// Unknown columns and nulls are errors rather than silently discarded data.
    pub fn validate(&self, record: &Record) -> Result<()> {
        if record.len() != self.columns().len() {
            return Err(Error::Invalid(
                "record must contain every declared column only".into(),
            ));
        }
        for col in self.columns() {
            let value = record
                .get(&col.name)
                .ok_or_else(|| Error::Invalid(format!("missing column {}", col.name)))?;
            validate_value(col.kind, value)?;
            if matches!(col.role, Role::Tag | Role::Key) && value.as_str() == Some("") {
                return Err(Error::Invalid(format!(
                    "empty identity column {}",
                    col.name
                )));
            }
        }
        Ok(())
    }
}

/// Shared write and predicate validation; no coercion from strings to numbers.
pub(crate) fn validate_value(kind: Kind, value: &Value) -> Result<()> {
    let valid = match kind {
        Kind::String => value
            .as_str()
            .is_some_and(|s| !s.contains(['\n', '\r', '\0'])),
        Kind::Int | Kind::Timestamp => value.as_i64().is_some(),
        Kind::Uint => value.as_u64().is_some(),
        Kind::Float => value.as_f64().is_some_and(f64::is_finite),
        Kind::Bool => value.is_boolean(),
    };
    if !valid {
        return Err(Error::Invalid(format!("expected {kind:?}, got {value}")));
    }
    Ok(())
}
