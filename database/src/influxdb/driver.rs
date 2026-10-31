//! An SDK client adapted to the store contracts.
//!
//! InfluxDB identifies a point by table, tag set and timestamp, so it has no
//! record key: create and create_many write points, list and count read them,
//! and get, update, delete and exists are refused by name. Rewriting a point
//! identity merges its fields rather than failing as a duplicate.

use super::{codec, errors::sdk, sql};
use crate::store::{Error, Record, Resource, Result};
use influxdb3_client::{Client, Precision, WriteOptions};
use std::future::IntoFuture;
use std::{sync::Arc, time::Duration};

pub(super) const BACKEND: &str = "influxdb3";

/// Most points the SDK is given in one write request.
const MAX_POINTS: usize = 10_000;

/// Driver over one caller-owned, database-bound SDK client.
/// No background tasks are started. Dropping a driver releases only its Arc.
pub struct InfluxDriver {
    pub(super) client: Arc<Client>,
    pub(super) timeout: Duration,
}

impl InfluxDriver {
    /// Binds a client with a complete per-operation timeout, including retries.
    pub fn new(client: Arc<Client>, timeout: Duration) -> Result<Self> {
        if timeout.is_zero() {
            return Err(Error::Invalid("operation timeout must be positive".into()));
        }
        Ok(Self { client, timeout })
    }

    /// Access to native SDK features outside the portable contract.
    pub fn client(&self) -> &Arc<Client> {
        &self.client
    }

    pub(super) async fn probe(&self) -> Result<()> {
        let query = self
            .client
            .sql("SELECT table_name FROM information_schema.tables LIMIT 1");
        tokio::time::timeout(self.timeout, query.into_future())
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(sdk)?;
        Ok(())
    }

    pub(super) async fn query(&self, query: sql::Query) -> Result<Vec<Record>> {
        let request = self.client.sql(query.sql).params(query.params);
        let rows = tokio::time::timeout(self.timeout, request.into_future())
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(sdk)?
            .rows()
            .map_err(sdk)?;
        rows.into_iter().map(codec::row).collect()
    }

    pub(super) fn validate_resource(&self, resource: &Resource) -> Result<()> {
        let time_column = resource.time_column()?;
        if resource.key_column().is_some() {
            return Err(Error::Invalid(
                "InfluxDB cannot enforce a record key; describe point dimensions as tags".into(),
            ));
        }
        if resource
            .columns()
            .iter()
            .any(|c| c.name == "time" && c.name != time_column)
        {
            return Err(Error::Invalid(
                "InfluxDB reserves time for the observation timestamp".into(),
            ));
        }
        Ok(())
    }

    /// Writes validated points in requests of at most 10,000. Requests are
    /// not atomic together: a later failure leaves earlier ones committed.
    pub(super) async fn write(&self, resource: &Resource, records: &[Record]) -> Result<()> {
        self.validate_resource(resource)?;
        let points = records
            .iter()
            .map(|row| codec::point(resource, row))
            .collect::<Result<Vec<_>>>()?;
        // Explicit policy prevents client defaults adding undeclared tags,
        // changing precision, or weakening durability behind the descriptor.
        let options = WriteOptions {
            use_v2_api: false,
            accept_partial: false,
            no_sync: false,
            precision: Precision::Nanosecond,
            max_inflight: 1,
            // One request per chunk: each durable request waits for a WAL flush.
            batch_size: MAX_POINTS,
            ..WriteOptions::default()
        };
        let mut points = points.into_iter().peekable();
        while points.peek().is_some() {
            let chunk: Vec<_> = points.by_ref().take(MAX_POINTS).collect();
            let request = self.client.write(chunk).with_options(options.clone());
            tokio::time::timeout(self.timeout, request.into_future())
                .await
                .map_err(|_| Error::Timeout)?
                .map_err(sdk)?;
        }
        Ok(())
    }

    pub(super) fn refuse<T>(&self, capability: &'static str) -> Result<T> {
        Err(Error::Unimplemented {
            backend: BACKEND,
            capability,
        })
    }
}
