//! Typed producers share the same bounded background worker as dynamic calls.
use crate::store::{For, Result};
use crate::{BatchWriter, IngestConfig};

/// Typed admission to a background writer. A successful write is queued, not
/// committed. Flush or close is the durability/error barrier.
pub struct TypedBatchWriter<T> {
    inner: BatchWriter,
    view: For<T>,
}

impl<T> TypedBatchWriter<T> {
    /// Encodes a model, then validates it against the bound descriptor.
    pub async fn write(&self, value: &T) -> Result<()> {
        self.inner.write(self.view.encode(value)?).await
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

impl<T> For<T> {
    /// Creates a typed, bounded producer with explicit flush/close semantics.
    pub fn batch_writer(&self, config: IngestConfig) -> Result<TypedBatchWriter<T>> {
        let (db, resource) = self.unwrap();
        Ok(TypedBatchWriter {
            inner: db.batch_writer(resource.clone(), config)?,
            view: self.clone(),
        })
    }
}
