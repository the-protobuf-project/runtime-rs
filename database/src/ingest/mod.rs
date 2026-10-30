//! Bounded continuous ingestion over the portable TimeSeries capability.
mod config;
mod worker;
mod writer;
pub use config::IngestConfig;
pub use writer::BatchWriter;

impl crate::DB {
    /// Starts a background writer bound to this resource and database.
    /// Flush or close it to observe durability and terminal errors.
    pub fn batch_writer(
        &self,
        resource: crate::Resource,
        config: IngestConfig,
    ) -> crate::Result<BatchWriter> {
        self.series()?;
        BatchWriter::new(
            self.capabilities
                .series
                .as_ref()
                .expect("checked series")
                .clone(),
            resource,
            config,
        )
    }
}
