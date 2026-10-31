//! Bounded continuous ingestion over the Batcher capability.
mod admission;
mod batch;
mod config;
mod queue;
mod worker;
mod writer;
pub use config::IngestConfig;
pub use writer::BatchWriter;

impl crate::store::DB {
    /// Starts a background writer bound to this resource and database.
    /// Flush or close it to observe durability and terminal errors.
    pub fn batch_writer(
        &self,
        resource: crate::store::Resource,
        config: IngestConfig,
    ) -> crate::store::Result<BatchWriter> {
        BatchWriter::observed(
            self.batch.clone(),
            resource,
            config,
            self.telemetry.clone(),
            self.backend,
        )
    }
}
