//! Descriptor-driven storage, shaped after runtime-go's `database` module so
//! one generator emits Go and Rust clients from the same template.
//!
//! [`store`] is the contract: a [`Provider`] selects a [`DB`], which is the
//! CRUD [`Driver`](store::Driver) plus every capability — transactions,
//! schema, batches, time series, streams — each always present and refusing
//! by name where a backend lacks it. Backends are [`influxdb`] and [`arrow`].
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

pub mod arrow;
mod buffered;
mod collection;
pub mod influxdb;
pub mod ingest;
mod mirror;
mod observe;
pub mod store;
mod typed_ingest;

pub use buffered::buffered;
pub use collection::collection;
pub use ingest::{BatchWriter, IngestConfig};
pub use mirror::mirror;
pub use store::{DB, Error, Provider, Result};
pub use typed_ingest::TypedBatchWriter;
