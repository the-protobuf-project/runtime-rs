//! Serde models over the same contract, as runtime-go's `database.Collection`.
//!
//! Generated code implements [`Model`](crate::store::Model) and uses
//! [`For::new`]; hand-written structs use [`collection`] with a descriptor,
//! and are stored as the same rows.
use crate::store::{DB, Error, For, Record, Resource, Result};
use serde::{Serialize, de::DeserializeOwned};

/// A typed view of `db` storing `T` through serde, in `resource`'s table.
/// Field names must match column names, or use serde rename attributes.
pub fn collection<T: Serialize + DeserializeOwned>(db: &DB, resource: Resource) -> For<T> {
    For::with(db, resource, to_record::<T>, from_record::<T>)
}

fn to_record<T: Serialize>(value: &T) -> Result<Record> {
    match serde_json::to_value(value)? {
        serde_json::Value::Object(map) => Ok(map.into_iter().collect()),
        _ => Err(Error::Invalid("a model must serialize to an object".into())),
    }
}

fn from_record<T: DeserializeOwned>(record: Record) -> Result<T> {
    let map: serde_json::Map<String, serde_json::Value> = record.into_iter().collect();
    Ok(serde_json::from_value(serde_json::Value::Object(map))?)
}
