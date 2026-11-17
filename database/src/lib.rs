//! Descriptor-driven storage, shaped after runtime-go's `database` module so
//! one generator emits Go and Rust clients from the same template.
//!
//! [`store`] is the contract: a [`Provider`] selects a [`DB`], which is the
//! CRUD [`Driver`](store::Driver) plus every capability — transactions,
//! schema, batches, time series, streams — each always present and refusing
//! by name where a backend lacks it. Backends are [`influxdb`] and [`arrow`].
//!
//! [`records`] is the same shape for proto messages rather than flat points:
//! a CRUD driver over `DynamicMessage`s, with transactions and schema. Its
//! SQL backend, `orm` (feature `orm`, or one dialect with `orm-postgres`,
//! `orm-mysql` or `orm-sqlite`), runs every resource on PostgreSQL, MySQL or
//! SQLite through sqlx and sea-query.
//!
//! [`records`] is the proto-message record contract, runtime-go's `database/store`;
//! the `mongodb` feature adds `database::mongodb`, which implements it over MongoDB.
//!
//! # Generated models
//!
//! Generated code implements [`Model`](store::Model) for each message and
//! binds it with [`For`](store::For):
//!
//! ```ignore
//! let db = InfluxProvider::new(config).set_database("bms").await?;
//! let readings = store::For::<Reading>::new(&db);
//! readings.create(&reading).await?;
//! let (page, next) = readings.range(&RangeOptions { start, end, ..Default::default() }).await?;
//! ```
//!
//! Hand-written serde structs use [`collection`] with a descriptor and are
//! stored as the same rows.
//!
//! # Sustained writes and recordings
//!
//! [`buffered`] queues each `create` into a bounded background batch writer
//! and reports it with `pending: true`, so one call per point sustains tens
//! of thousands of points a second. [`mirror`] writes to two databases at
//! once, such as InfluxDB and an Arrow capture of every point:
//!
//! ```ignore
//! let live = mirror(influx_db, arrow_db);
//! let db = buffered(live, IngestConfig::default())?;
//! // … creates …
//! db.close().await?; // drains every queue, then closes both stores
//! ```
//!
//! Filters are the AIP-160 subset runtime-go accepts: conjunctions of
//! `column op value` with = != > >= < <=. Times are epoch nanoseconds.

#![warn(missing_docs)]

// Lets the mongodb unit tests include the integration fixtures, which name the crate.
#[cfg(all(test, feature = "mongodb"))]
extern crate self as database;

pub mod arrow;
mod buffered;
#[cfg(feature = "cached")]
pub mod cached;
mod collection;
#[cfg(feature = "influxdb")]
pub mod influxdb;
pub mod ingest;
mod mirror;
#[cfg(feature = "mongodb")]
pub mod mongodb;
mod observe;
#[cfg(any(
    feature = "orm-postgres",
    feature = "orm-mysql",
    feature = "orm-sqlite"
))]
pub mod orm;
pub mod records;
#[cfg(feature = "redis")]
pub mod redis;
pub mod store;
#[cfg(feature = "tauri-store")]
pub mod tauri_store;
mod typed_ingest;

pub use buffered::buffered;
pub use collection::collection;
pub use ingest::{BatchWriter, IngestConfig};
pub use mirror::mirror;
pub use store::{DB, Error, Provider, Result};
/// The signals [`store::instrument`] and the ingest workers report through, so a caller
/// configures them without naming the telemetry crate.
pub use telemetry::Telemetry;
pub use typed_ingest::TypedBatchWriter;

/// The README's Rust snippets, compiled as doctests so the guide cannot drift from the API.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
pub struct ReadmeDoctests;
