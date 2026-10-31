//! Database selection over a root directory of Arrow captures.

use super::ArrowDriver;
use super::errors::{BACKEND, backend, invalid};
use super::rotation::Rotation;
use crate::store::check_name;
use crate::store::{Capabilities, DB, Provider, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Arrow file provider. Each database is a directory under `root`.
pub struct ArrowProvider {
    root: PathBuf,
    sync: bool,
    rotation: Rotation,
    telemetry: telemetry::Telemetry,
    observer: telemetry::Observer,
}

impl ArrowProvider {
    /// Uses `root` as the parent of every database directory. No I/O happens
    /// until a database is created or selected.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            sync: false,
            rotation: Rotation::default(),
            telemetry: telemetry::Telemetry::default(),
            observer: telemetry::Telemetry::default().observer("database"),
        }
    }

    /// Waits for each write to reach the disk, not just the operating system,
    /// so a power loss cannot drop acknowledged batches. Slower per write.
    pub fn with_sync(mut self, sync: bool) -> Self {
        self.sync = sync;
        self
    }

    /// Sets when segments are closed and new files started.
    /// Defaults to [`Rotation::default`]: 1 GiB or 15 minutes.
    pub fn with_rotation(mut self, rotation: Rotation) -> Self {
        self.rotation = rotation;
        self
    }

    /// Instruments selection and every returned capability with shared signals.
    pub fn with_telemetry(mut self, signals: telemetry::Telemetry) -> Self {
        self.observer = signals.observer("database");
        self.telemetry = signals;
        self
    }

    /// Creates the database directory. Succeeds when it already exists.
    pub async fn create_database(&self, name: &str) -> Result<()> {
        check_name(name)?;
        tokio::fs::create_dir_all(self.root.join(name))
            .await
            .map_err(backend)
    }

    /// Directory holding one table's segments, for offline readers.
    pub fn table_dir(&self, database: &str, table: &str) -> PathBuf {
        self.root.join(database).join(table)
    }

    /// Root directory this provider was built with.
    pub fn root(&self) -> &Path {
        &self.root
    }
}

#[async_trait::async_trait]
impl Provider for ArrowProvider {
    fn backend(&self) -> &'static str {
        BACKEND
    }

    async fn set_database(&self, name: &str) -> Result<DB> {
        crate::observe::run(
            &self.observer,
            BACKEND,
            "select_database",
            self.select(name),
            0,
            |_| 0,
        )
        .await
    }
}

impl ArrowProvider {
    async fn select(&self, name: &str) -> Result<DB> {
        check_name(name)?;
        let dir = self.root.join(name);
        if !tokio::fs::metadata(&dir).await.is_ok_and(|m| m.is_dir()) {
            return Err(invalid(format!(
                "database {name} does not exist; create it with create_database"
            )));
        }
        let driver = Arc::new(ArrowDriver::new(dir, self.sync, self.rotation));
        Ok(crate::store::instrument(
            DB::build(
                BACKEND,
                name,
                Capabilities {
                    driver: Some(driver.clone()),
                    batch: Some(driver.clone()),
                    series: Some(driver.clone()),
                    schema: Some(driver.clone()),
                    release: Some(driver),
                    ..Capabilities::default()
                },
            ),
            self.telemetry.clone(),
        ))
    }
}
