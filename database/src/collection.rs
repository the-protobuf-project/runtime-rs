//! Typed Serde views over the same descriptors used by dynamic callers.

use crate::{DB, Error, Range, Record, Resource, Result};
use serde::{Serialize, de::DeserializeOwned};
use std::marker::PhantomData;

/// A cheap typed binding. Serde names must match descriptor column names.
/// Models represent observation time as signed epoch nanoseconds (`i64`).
pub struct Collection<T> {
    db: DB,
    resource: Resource,
    marker: PhantomData<fn() -> T>,
}

impl<T: Serialize + DeserializeOwned> Collection<T> {
    /// Binds a model to a descriptor and database, with no network I/O.
    pub fn new(db: DB, resource: Resource) -> Self {
        Self {
            db,
            resource,
            marker: PhantomData,
        }
    }

    /// Descriptor for dynamic or specialized operations.
    pub fn resource(&self) -> &Resource {
        &self.resource
    }

    /// Underlying database with every capability preserved.
    pub fn db(&self) -> &DB {
        &self.db
    }

    /// Writes a batch as points. Serializes and validates the entire batch
    /// before sending any values to the backend.
    pub async fn write(&self, values: &[T]) -> Result<()> {
        let records: Vec<Record> = values
            .iter()
            .map(|value| {
                let value = serde_json::to_value(value)?;
                if !value.is_object() {
                    return Err(Error::Invalid("a model must serialize to an object".into()));
                }
                let record = serde_json::from_value(value)?;
                self.resource.validate(&record)?;
                Ok(record)
            })
            .collect::<Result<_>>()?;
        self.db.series()?.write(&self.resource, &records).await
    }

    /// Reads a bounded window and decodes each row as the model type.
    pub async fn range(&self, range: &Range) -> Result<Vec<T>> {
        self.db
            .series()?
            .range(&self.resource, range)
            .await?
            .into_iter()
            .map(|row| Ok(serde_json::from_value(serde_json::to_value(row)?)?))
            .collect()
    }
}
