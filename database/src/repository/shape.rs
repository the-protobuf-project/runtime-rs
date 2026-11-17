//! [`Rules`] resolved against the message and the resource once, at construction: the
//! descriptors of the etag and soft-delete fields, the filter column `list` excludes deleted
//! records by, and which fields `update` and `update_output_only` may write.

use std::collections::BTreeSet;
use std::sync::Mutex;
use std::time::Duration;

use chrono::{DateTime, Utc};
use prost_reflect::{DynamicMessage, FieldDescriptor, Kind as ProtoKind, MessageDescriptor, Value};

use super::Rules;
use crate::records::Resource;
use crate::{Error, Result};

const TIMESTAMP: &str = "google.protobuf.Timestamp";

/// What the repository needs of `Rules`, as descriptors.
#[derive(Clone)]
pub(super) struct Shape {
    /// The etag field, a string.
    pub(super) etag: Option<FieldDescriptor>,
    /// The `delete_time` field and the column it is stored in.
    pub(super) delete_time: Option<(FieldDescriptor, String)>,
    /// The `expire_time` field.
    pub(super) expire_time: Option<FieldDescriptor>,
    /// How far past a delete `expire_time` is.
    pub(super) retention: Duration,
    /// What an empty `update` mask writes: every field but the output-only ones, the etag,
    /// and the audit times the driver fills.
    pub(super) mutable: Vec<FieldDescriptor>,
    /// What `update_output_only` may write: the output-only fields, less the etag, the
    /// soft-delete times and the audit times, which the repository and the driver own.
    pub(super) output: Vec<FieldDescriptor>,
}

impl Shape {
    pub(super) fn new(res: &Resource, rules: &Rules) -> Result<Self> {
        let msg = &res.message;
        let column_of = |field: &str| {
            res.columns
                .iter()
                .find(|c| c.field == field)
                .map(|c| c.name.clone())
                .ok_or_else(|| {
                    Error::Invalid(format!("resource {:?} stores no field {field:?}", res.name))
                })
        };
        let etag = match &rules.etag {
            Some(f) => {
                column_of(f)?;
                Some(typed(
                    msg,
                    f,
                    |k| matches!(k, ProtoKind::String),
                    "a string",
                )?)
            }
            None => None,
        };
        let is_time =
            |k: &ProtoKind| matches!(k, ProtoKind::Message(m) if m.full_name() == TIMESTAMP);
        let delete_time = match &rules.delete_time {
            Some(f) => Some((typed(msg, f, is_time, TIMESTAMP)?, column_of(f)?)),
            None => None,
        };
        let expire_time = match &rules.expire_time {
            Some(f) => {
                column_of(f)?;
                Some(typed(msg, f, is_time, TIMESTAMP)?)
            }
            None => None,
        };
        let mut owned: BTreeSet<&str> = [&rules.etag, &rules.delete_time, &rules.expire_time]
            .into_iter()
            .flatten()
            .map(String::as_str)
            .collect();
        owned.extend(
            res.columns
                .iter()
                .filter(|c| c.managed())
                .map(|c| c.field.as_str()),
        );
        let output_only: BTreeSet<&str> = rules.output_only.iter().map(String::as_str).collect();
        let fields = || msg.fields().filter(|f| !owned.contains(f.name()));
        Ok(Self {
            etag,
            delete_time,
            expire_time,
            retention: rules.retention,
            mutable: fields()
                .filter(|f| !output_only.contains(f.name()))
                .collect(),
            output: fields()
                .filter(|f| output_only.contains(f.name()))
                .collect(),
        })
    }

    /// Whether `msg` is soft-deleted.
    pub(super) fn deleted(&self, msg: &DynamicMessage) -> bool {
        self.delete_time
            .as_ref()
            .is_some_and(|(f, _)| msg.has_field(f))
    }

    /// The etag `msg` carries; empty without one.
    pub(super) fn etag_of(&self, msg: &DynamicMessage) -> String {
        self.etag
            .as_ref()
            .and_then(|f| msg.get_field(f).as_str().map(str::to_owned))
            .unwrap_or_default()
    }

    /// Refuses a write whose precondition `want` is not `stored`'s etag. An empty `want`, or
    /// a resource without an etag, carries no precondition.
    pub(super) fn check(&self, stored: &DynamicMessage, want: &str) -> Result<()> {
        if self.etag.is_none() || want.is_empty() || self.etag_of(stored) == want {
            Ok(())
        } else {
            Err(Error::Conflict)
        }
    }

    /// Gives `msg` a new etag, when the resource has one.
    pub(super) fn mint(&self, msg: &mut DynamicMessage) {
        if let Some(f) = &self.etag {
            msg.set_field(f, Value::String(new_etag()));
        }
    }
}

/// Sets the Timestamp field `f` of `msg` to `t`, or clears it for `None`.
pub(super) fn set_time(msg: &mut DynamicMessage, f: &FieldDescriptor, t: Option<DateTime<Utc>>) {
    let (Some(t), ProtoKind::Message(desc)) = (t, f.kind()) else {
        msg.clear_field(f);
        return;
    };
    let mut ts = DynamicMessage::new(desc);
    ts.set_field_by_name("seconds", Value::I64(t.timestamp()));
    ts.set_field_by_name("nanos", Value::I32(t.timestamp_subsec_nanos() as i32));
    msg.set_field(f, Value::Message(ts));
}

/// A new etag: a ULID, monotonic within the process, as Go's `repox.NewULID` mints one, so
/// etags minted in one millisecond still sort in the order they were minted.
pub fn new_etag() -> String {
    static GENERATOR: Mutex<ulid::Generator> = Mutex::new(ulid::Generator::new());
    let mut generator = GENERATOR.lock().unwrap_or_else(|e| e.into_inner());
    match generator.generate() {
        Ok(id) => id.to_string(),
        Err(overflow) => overflow.commit_overflow_increment().to_string(),
    }
}

fn typed(
    msg: &MessageDescriptor,
    name: &str,
    ok: impl Fn(&ProtoKind) -> bool,
    want: &str,
) -> Result<FieldDescriptor> {
    let f = msg
        .get_field_by_name(name)
        .ok_or_else(|| Error::Invalid(format!("{} has no field {name:?}", msg.full_name())))?;
    if f.is_list() || f.is_map() || !ok(&f.kind()) {
        return Err(Error::Invalid(format!(
            "{}.{name} must be {want} to carry this rule",
            msg.full_name()
        )));
    }
    Ok(f)
}
