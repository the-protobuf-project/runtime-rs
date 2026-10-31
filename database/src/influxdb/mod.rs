//! InfluxDB 3 Core/Enterprise through the influxdb3-client Rust SDK.
//!
//! The time-series backend, as runtime-go's `database/timescale` is for
//! TimescaleDB. A point is a record without a key: [`Driver::create`] and
//! [`Batcher::create_many`] write points, list, count, [`TimeSeries::range`]
//! and [`TimeSeries::aggregate`] read them, and keyed operations and
//! transactions are refused by name.
//!
//! Writes use the V3 API with WAL synchronization and partial-line acceptance
//! disabled, in requests of at most 10,000 points that are not atomic
//! together. Queries use Arrow Flight SQL. Tables are created by the server
//! on first write; [`InfluxAdmin`] provisions databases with [`Retention`].
//!
//! [`Driver::create`]: crate::store::Driver::create
//! [`Batcher::create_many`]: crate::store::Batcher::create_many
//! [`TimeSeries::range`]: crate::store::TimeSeries::range
//! [`TimeSeries::aggregate`]: crate::store::TimeSeries::aggregate

mod admin;
mod admin_http;
mod aggregate;
mod codec;
mod crud;
mod cursor;
mod driver;
mod errors;
mod provider;
mod reader;
mod retention;
mod series;
mod sql;

pub use admin::InfluxAdmin;
pub use driver::InfluxDriver;
/// SDK types for constructing or sharing caller-owned clients.
pub use influxdb3_client::{Client, ClientConfig, RetryConfig};
pub use provider::InfluxProvider;
pub use retention::Retention;

#[cfg(test)]
mod sql_tests;

#[cfg(test)]
mod cursor_tests;
