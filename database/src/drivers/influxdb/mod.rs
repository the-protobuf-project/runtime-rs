//! InfluxDB 3 Core/Enterprise through the influxdb3-client Rust SDK.
//!
//! Writes use the V3 API with WAL synchronization and partial-line acceptance
//! disabled. Multiple batches are not atomic. Queries use Arrow Flight SQL.
//! Tables are created by the server on first write. InfluxAdmin provisions
//! databases; maintenance explicitly drops tables. Selective row deletion and
//! rollback transactions are not emulated on Core.

mod admin;
mod admin_http;
mod aggregate;
mod codec;
mod cursor;
mod driver;
mod errors;
mod provider;
mod reader;
mod sql;

pub use admin::InfluxAdmin;
pub use driver::InfluxDriver;
/// SDK types for constructing or sharing caller-owned clients.
pub use influxdb3_client::{Client, ClientConfig, RetryConfig};
pub use provider::InfluxProvider;

#[cfg(test)]
mod sql_tests;

#[cfg(test)]
mod cursor_tests;
