//! An SDK client adapted to the portable TimeSeries contract.

use super::{aggregate, codec, errors::sdk, sql};
use crate::{Aggregate, Buckets, Error, Range, Record, Resource, Result, TimeSeries};
use influxdb3_client::{Client, Precision, WriteOptions};
use std::{sync::Arc, time::Duration};

/// Time-series driver over one caller-owned, database-bound SDK client.
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
}

use std::future::IntoFuture;

#[async_trait::async_trait]
impl TimeSeries for InfluxDriver {
    async fn write(&self, resource: &Resource, records: &[Record]) -> Result<()> {
        self.validate_resource(resource)?;
        if records.len() > 10_000 {
            return Err(Error::Invalid("write batch exceeds 10000 points".into()));
        }
        let points = records
            .iter()
            .map(|row| codec::point(resource, row))
            .collect::<Result<Vec<_>>>()?;
        if points.is_empty() {
            return Ok(());
        }
        // Explicit policy prevents client defaults adding undeclared tags,
        // changing precision, or weakening durability behind the descriptor.
        let options = WriteOptions {
            use_v2_api: false,
            accept_partial: false,
            no_sync: false,
            precision: Precision::Nanosecond,
            max_inflight: 1,
            ..WriteOptions::default()
        };
        let request = self.client.write(points).with_options(options);
        tokio::time::timeout(self.timeout, request.into_future())
            .await
            .map_err(|_| Error::Timeout)?
            .map_err(sdk)
    }

    async fn range(&self, resource: &Resource, range: &Range) -> Result<Vec<Record>> {
        self.validate_resource(resource)?;
        self.query(sql::range(resource, range)?).await
    }

    async fn aggregate(&self, resource: &Resource, options: &Aggregate) -> Result<Buckets> {
        self.validate_resource(resource)?;
        self.query(aggregate::aggregate(resource, options)?).await
    }
}
