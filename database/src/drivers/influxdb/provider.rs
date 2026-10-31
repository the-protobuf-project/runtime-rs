//! Immutable database selection with explicit SDK pool ownership.

use super::{InfluxAdmin, InfluxDriver, errors::sdk};
use crate::resource::check_name;
use crate::{Capabilities, DB, Error, Provider, Result};
use influxdb3_client::{Client, ClientConfig};
use std::{sync::Arc, time::Duration};

/// InfluxDB provider. SDK clients are bound to one database.
/// Configuration-based selection creates a separate pool per returned DB.
/// Share/clone a selected DB to reuse that pool. `from_client` reuses a
/// caller-owned pool and accepts only the database already bound to it.
pub struct InfluxProvider {
    source: Source,
    telemetry: telemetry::Telemetry,
    observer: telemetry::Observer,
    timeout: Duration,
}

enum Source {
    Config(Box<ClientConfig>),
    Client(Arc<Client>),
}

impl InfluxProvider {
    /// Saves immutable SDK configuration. Database selection clones it and
    /// overrides only the database; it never modifies an existing SDK client.
    pub fn new(config: ClientConfig) -> Self {
        Self {
            source: Source::Config(Box::new(config)),
            telemetry: telemetry::Telemetry::default(),
            observer: telemetry::Telemetry::default().observer("database"),
            timeout: Duration::from_secs(30),
        }
    }

    /// Reuses a caller-owned client. Selection must name its configured database.
    pub fn from_client(client: Arc<Client>) -> Self {
        Self {
            source: Source::Client(client),
            telemetry: telemetry::Telemetry::default(),
            observer: telemetry::Telemetry::default().observer("database"),
            timeout: Duration::from_secs(30),
        }
    }

    /// Instruments selection and every returned capability with shared signals.
    pub fn with_telemetry(mut self, signals: telemetry::Telemetry) -> Self {
        self.observer = signals.observer("database");
        self.telemetry = signals;
        self
    }

    /// Sets the complete operation/probe budget, including SDK retry delays.
    pub fn with_timeout(mut self, timeout: Duration) -> Result<Self> {
        if timeout.is_zero() {
            return Err(Error::Invalid("timeout must be positive".into()));
        }
        self.timeout = timeout;
        Ok(self)
    }
}

#[async_trait::async_trait]
impl Provider for InfluxProvider {
    async fn set_database(&self, name: &str) -> Result<DB> {
        crate::observe::run(
            &self.observer,
            "influxdb3",
            "select_database",
            self.select(name),
            0,
            |_| 0,
        )
        .await
    }
}

impl InfluxProvider {
    async fn select(&self, name: &str) -> Result<DB> {
        check_name(name)?;
        let client = match &self.source {
            Source::Config(config) => {
                let mut config = (**config).clone();
                config.database = name.to_owned();
                Arc::new(Client::new(config).await.map_err(sdk)?)
            }
            Source::Client(client) => {
                if client.config().database != name {
                    return Err(Error::Invalid("SDK client is bound to another database; select from configuration instead".into()));
                }
                client.clone()
            }
        };
        let maintenance =
            Arc::new(InfluxAdmin::new(client.config(), self.timeout)?.select(name.to_owned()));
        let driver = Arc::new(InfluxDriver::new(client, self.timeout)?);
        driver.probe().await?;
        Ok(crate::instrument(
            DB::new(
                "influxdb3",
                name.to_owned(),
                Capabilities {
                    series: Some(driver.clone()),
                    reader: Some(driver),
                    maintenance: Some(maintenance),
                    ..Capabilities::default()
                },
            ),
            self.telemetry.clone(),
        ))
    }
}
