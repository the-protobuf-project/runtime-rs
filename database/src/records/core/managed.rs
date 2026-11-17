//! The columns a driver fills itself: generated keys and audit times. Go's `core/managed.go`.

use std::future::Future;

use chrono::{DateTime, Utc};

use crate::records::{Generated, Resource, Row, Value};

tokio::task_local! {
    static CLOCK: DateTime<Utc>;
}

/// Runs `work` with the audit times its writes fill pinned to `now`, rather than each
/// driver's own reading of the clock. A soft delete uses it so `update_time` is the
/// `delete_time` it writes, to the nanosecond, as Go's single `now` is. It holds on the task
/// that awaits `work`; drivers fill managed columns there.
pub async fn at<F: Future>(now: DateTime<Utc>, work: F) -> F::Output {
    CLOCK.scope(now, work).await
}

/// The time a write's audit columns get: the one [`at`] pinned, else now.
pub fn now() -> DateTime<Utc> {
    CLOCK.try_with(|t| *t).unwrap_or_else(|_| Utc::now())
}

/// Fills the columns a driver supplies: on create a generated key left empty and both audit
/// times, on update the update time. Go's `core.FillManaged`.
pub fn fill_managed(res: &Resource, cols: &mut Row, on_create: bool) {
    let now = Value::Timestamp(now());
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
