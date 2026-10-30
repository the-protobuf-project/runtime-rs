//! Explicit database provisioning and table data maintenance for InfluxDB 3.
use super::admin_http::Http;
use crate::resource::check_name;
use crate::{Error, Resource, Result, Retention, SeriesMaintenance};
use influxdb3_client::ClientConfig;
use reqwest::Method;
use serde_json::json;
use std::time::Duration;

/// Administrative calls absent from the SDK, using the same authentication.
/// Creating databases is explicit and requires administrative permission.
/// Core retention is immutable; choosing a new period means a new database.
#[derive(Clone)]
pub struct InfluxAdmin {
    http: Http,
}

impl InfluxAdmin {
    /// Builds the supplemental HTTP pool with a complete request deadline.
    pub fn new(config: &ClientConfig, timeout: Duration) -> Result<Self> {
        Ok(Self {
            http: Http::new(config, timeout)?,
        })
    }

    /// Creates a database with explicit expiry. AlreadyExists is returned
    /// rather than accepting an existing database with different retention.
    pub async fn create_database(&self, name: &str, retention: Retention) -> Result<()> {
        check_name(name)?;
        let retention = match retention {
            Retention::Infinite => None,
            Retention::For(duration) => {
                if duration.as_secs() < 3600 || duration.as_nanos() % 60_000_000_000 != 0 {
                    return Err(Error::Invalid(
                        "retention must be whole minutes and at least one hour".into(),
                    ));
                }
                Some(format!("{}m", duration.as_secs() / 60))
            }
        };
        let mut body = json!({"db": name});
        if let Some(period) = retention {
            body["retention_period"] = period.into();
        }
        self.http
            .request(Method::POST, "/api/v3/configure/database", &[], Some(body))
            .await
    }

    /// Soft-deletes this database and all its data. Only call for a database
    /// whose removal is intended; server policy governs physical reclamation.
    pub async fn drop_database(&self, name: &str) -> Result<()> {
        check_name(name)?;
        self.http
            .request(
                Method::DELETE,
                "/api/v3/configure/database",
                &[("db", name)],
                None,
            )
            .await
    }

    pub(super) fn select(&self, database: String) -> Maintenance {
        Maintenance {
            http: self.http.clone(),
            database,
        }
    }
}

pub(super) struct Maintenance {
    http: Http,
    database: String,
}

#[async_trait::async_trait]
impl SeriesMaintenance for Maintenance {
    async fn drop_table(&self, resource: &Resource) -> Result<()> {
        self.http
            .request(
                Method::DELETE,
                "/api/v3/configure/table",
                &[("db", &self.database), ("table", resource.table())],
                None,
            )
            .await
    }
}
