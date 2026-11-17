//! The writes and point reads, as synchronous steps over the store. The driver runs each one
//! under its store's lock, so a check and the write that depends on it cannot interleave
//! with another writer's: that is what makes a unique reservation race-free here.

use prost_reflect::{DynamicMessage, ReflectMessage};
use serde_json::Value as Json;

use super::codec;
use super::driver::Inner;
use crate::records::{
    Resource, Row, Value, WriteResult, columns_to_message, core, message_to_columns,
};
use crate::{Error, Result};

impl<R: tauri::Runtime> Inner<R> {
    /// The row stored under `key`, if any.
    pub(super) fn load(&self, res: &Resource, key: &str) -> Result<Option<Row>> {
        match self.store.get(self.keys.record(res, key)) {
            Some(json) => codec::decode(res, &json).map(Some),
            None => Ok(None),
        }
    }

    pub(super) fn create(&self, res: &Resource, msg: &DynamicMessage) -> Result<WriteResult> {
        let mut row = message_to_columns(res, msg)?;
        core::fill_managed(res, &mut row, true);
        let key = key_of(res, &row)?;
        if self.store.has(self.keys.record(res, &key)) {
            return Err(Error::AlreadyExists);
        }
        self.reserve(res, &key, &row, None)?;
        self.store
            .set(self.keys.record(res, &key), codec::encode(&row));
        Ok(WriteResult::done(stored(res, msg, &row)?))
    }

    pub(super) fn update(&self, res: &Resource, msg: &DynamicMessage) -> Result<WriteResult> {
        let mut row = message_to_columns(res, msg)?;
        let key = key_of(res, &row)?;
        let old = self.load(res, &key)?.ok_or(Error::NotFound)?;
        core::fill_managed(res, &mut row, false);
        // The create time is the store's, as on SQL: an update cannot rewrite history.
        for col in res
            .columns
            .iter()
            .filter(|c| c.auto_create && !c.auto_update)
        {
            row.insert(
                col.name.clone(),
                old.get(&col.name).cloned().unwrap_or(Value::Null),
            );
        }
        self.reserve(res, &key, &row, Some(&old))?;
        self.release(res, &key, &old, Some(&row));
        self.store
            .set(self.keys.record(res, &key), codec::encode(&row));
        Ok(WriteResult::done(stored(res, msg, &row)?))
    }

    pub(super) fn delete(&self, res: &Resource, key: &str) -> Result<()> {
        let old = self.load(res, key)?.ok_or(Error::NotFound)?;
        self.release(res, key, &old, None);
        self.store.delete(self.keys.record(res, key));
        Ok(())
    }

    /// Reserves `row`'s unique values for `key`, refusing one another record holds. Checked in
    /// full before any is written, so a refusal leaves nothing behind.
    fn reserve(&self, res: &Resource, key: &str, row: &Row, old: Option<&Row>) -> Result<()> {
        let wanted: Vec<String> = unique_values(res, row)
            .filter(|(col, v)| old.is_none_or(|o| unique_value(o, col).as_deref() != Some(v)))
            .map(|(col, v)| self.keys.unique(res, &col, &v))
            .collect();
        for reservation in &wanted {
            if let Some(holder) = self.store.get(reservation)
                && holder.as_str() != Some(key)
            {
                return Err(Error::AlreadyExists);
            }
        }
        for reservation in wanted {
            self.store.set(reservation, Json::String(key.to_owned()));
        }
        Ok(())
    }

    /// Frees `old`'s reservations that `new` no longer holds, and only while `key` holds them.
    fn release(&self, res: &Resource, key: &str, old: &Row, new: Option<&Row>) {
        for (col, v) in unique_values(res, old) {
            if new.is_some_and(|n| unique_value(n, &col).as_deref() == Some(&v)) {
                continue;
            }
            let reservation = self.keys.unique(res, &col, &v);
            if self.store.get(&reservation).as_ref().and_then(Json::as_str) == Some(key) {
                self.store.delete(reservation);
            }
        }
    }
}

/// The row's primary-key value; empty is refused, as no record can be found under it.
fn key_of(res: &Resource, row: &Row) -> Result<String> {
    let key = row
        .get(&res.pk_column)
        .map(Value::key_string)
        .unwrap_or_default();
    if key.is_empty() {
        return Err(Error::Invalid(format!(
            "{}: a record needs a {} value",
            res.name, res.pk_column
        )));
    }
    Ok(key)
}

fn unique_values<'a>(
    res: &'a Resource,
    row: &'a Row,
) -> impl Iterator<Item = (String, String)> + 'a {
    res.columns
        .iter()
        .filter(|c| c.unique && !c.primary_key)
        .filter_map(|c| Some((c.name.clone(), unique_value(row, &c.name)?)))
}

fn unique_value(row: &Row, column: &str) -> Option<String> {
    row.get(column)
        .filter(|v| !matches!(v, Value::Null))
        .map(Value::key_string)
}

/// What was stored, as the caller's message type: `msg` with the managed columns the driver
/// filled, so fields no column maps survive.
fn stored(res: &Resource, msg: &DynamicMessage, row: &Row) -> Result<DynamicMessage> {
    let filled = columns_to_message(res, row)?;
    let mut out = msg.clone();
    for col in res
        .columns
        .iter()
        .filter(|c| c.managed() && !c.field.is_empty())
    {
        if let Some(fd) = filled.descriptor().get_field_by_name(&col.field)
            && filled.has_field(&fd)
        {
            out.set_field(&fd, filled.get_field(&fd).into_owned());
        }
    }
    Ok(out)
}
