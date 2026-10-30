//! Additional DB accessors preserve the capability bundle when cloned.
use crate::{DB, Error, Result, RowDeletion, SeriesMaintenance, SeriesReader};

impl DB {
    /// Streaming and paginated series reads, or a backend-named refusal.
    pub fn reader(&self) -> Result<&dyn SeriesReader> {
        self.capabilities
            .reader
            .as_deref()
            .ok_or(Error::Unsupported {
                backend: self.backend,
                capability: "streaming/paged reads",
            })
    }

    /// Whole-table schema/data deletion, or a backend-named refusal.
    pub fn maintenance(&self) -> Result<&dyn SeriesMaintenance> {
        self.capabilities
            .maintenance
            .as_deref()
            .ok_or(Error::Unsupported {
                backend: self.backend,
                capability: "whole-table deletion",
            })
    }

    /// Selective historical deletion. Absent on InfluxDB 3 Core.
    pub fn row_deletion(&self) -> Result<&dyn RowDeletion> {
        self.capabilities
            .row_deletion
            .as_deref()
            .ok_or(Error::Unsupported {
                backend: self.backend,
                capability: "time-range row deletion",
            })
    }
}
