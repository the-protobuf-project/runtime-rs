//! The columns a driver fills itself: generated keys and audit times. Go's `core/managed.go`.

use chrono::Utc;

use crate::records::{Generated, Resource, Row, Value};

/// Fills the columns a driver supplies: on create a generated key left empty and both audit
/// times, on update the update time. Go's `core.FillManaged`.
pub fn fill_managed(res: &Resource, cols: &mut Row, on_create: bool) {
    let now = Value::Timestamp(Utc::now());
    for c in &res.columns {
        let empty = cols.get(&c.name).is_none_or(Value::is_empty);
        match c.generated {
            Some(strategy) if on_create && empty => {
                cols.insert(c.name.clone(), Value::String(generate_id(strategy)));
            }
            _ if on_create && (c.auto_create || c.auto_update) => {
                cols.insert(c.name.clone(), now.clone());
            }
            _ if !on_create && c.auto_update => {
                cols.insert(c.name.clone(), now.clone());
            }
            _ => {}
        }
    }
}

/// Whether any column is filled by the driver.
pub fn has_managed(res: &Resource) -> bool {
    res.columns.iter().any(|c| c.managed())
}

/// A new identifier. Go's `core.GenerateID`.
pub fn generate_id(strategy: Generated) -> String {
    match strategy {
        Generated::Uuid => uuid::Uuid::new_v4().to_string(),
        Generated::Ulid => ulid::Ulid::generate().to_string(),
    }
}
