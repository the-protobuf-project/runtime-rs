//! Typed producers share the same bounded background worker as dynamic calls.
use crate::{BatchWriter, Collection, IngestConfig, Result};
use serde::{Serialize, de::DeserializeOwned};
use std::marker::PhantomData;

/// Typed admission to a background writer. A successful write is queued, not
/// committed. Flush or close is the durability/error barrier.
pub struct TypedBatchWriter<T> {
    inner: BatchWriter,
    marker: PhantomData<fn() -> T>,
}

impl<T: Serialize> TypedBatchWriter<T> {
    /// Encodes a model, then validates it against the bound descriptor.
    pub async fn write(&self, value: &T) -> Result<()> {
        self.inner
            .write(serde_json::from_value(serde_json::to_value(value)?)?)
            .await
    }
    /// Waits for all records admitted before this call's barrier.
    pub async fn flush(&self) -> Result<()> {
        self.inner.flush().await
    }
    /// Stops admission and drains the queue, reporting any terminal failure.
    pub async fn close(self) -> Result<()> {
        self.inner.close().await
    }
}

impl<T: Serialize + DeserializeOwned> Collection<T> {
    /// Creates a typed, bounded producer with explicit flush/close semantics.
    pub fn batch_writer(&self, config: IngestConfig) -> Result<TypedBatchWriter<T>> {
        let inner = self.db().batch_writer(self.resource().clone(), config)?;
        Ok(TypedBatchWriter {
            inner,
            marker: PhantomData,
        })
    }
}
