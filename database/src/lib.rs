//! Descriptor-driven database capabilities over pluggable backends.
//!
//! A Resource describes columns once. Dynamic Records and typed Serde models
//! travel through the same capability contracts. The first backend is InfluxDB
//! 3 Core/Enterprise, using its Rust SDK for writes and Arrow Flight queries.
//!
//! # Example
//!
//! ```no_run
//! use database::{Collection, Column, Kind, Provider, Range, Resource, Role};
//! use database::drivers::influxdb::{ClientConfig, InfluxProvider};
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct Reading { device: String, celsius: f64, observed_at: i64 }
//!
//! # async fn example() -> database::Result<()> {
//! let config = ClientConfig::builder()
//!     .host("http://localhost:8181")
//!     .token(std::env::var("INFLUX_TOKEN").expect("set INFLUX_TOKEN"))
//!     .database("sensors")
//!     .build().expect("valid SDK configuration");
//! let db = InfluxProvider::new(config).set_database("sensors").await?;
//! let resource = Resource::new("readings", vec![
//!     Column::new("device", Kind::String, Role::Tag),
//!     Column::new("celsius", Kind::Float, Role::Field),
//!     Column::new("observed_at", Kind::Timestamp, Role::Timestamp),
//! ])?;
//! let readings = Collection::<Reading>::new(db, resource);
//! readings.write(&[Reading {
//!     device: "boiler".into(), celsius: 22.5, observed_at: 1_800_000_000_000_000_000,
//! }]).await?;
//! let rows = readings.range(&Range::new(
//!     1_800_000_000_000_000_000, 1_800_000_001_000_000_000,
//! )).await?;
//! # let _ = rows;
//! # Ok(())
//! # }
//! ```
//!
//! # Guarantees and limits
//!
//! InfluxDB exposes TimeSeries, not RecordStore. A point is identified by its
//! table, complete tag set and explicit timestamp. Rewriting that identity
//! merges fields; it does not provide insert-only uniqueness or rollback.
//! All descriptor columns are required on writes. Tables with evolving nullable
//! fields can be queried dynamically; typed models must account for nulls.
//!
//! Range queries use half-open bounds and return at most 10,000 rows (100 by
//! default). Reader pages continue with scoped keyset cursors. Scans stream a
//! time window without a row cap unless one is requested. Neither is a snapshot. Aggregates
//! return timestamp, grouping tags and named reductions, with no gap filling.
//! InfluxDB 3 Core applies its own query-window and retention limits.
//!
//! The SDK owns encoding, HTTP retries, and Arrow Flight transport. The adapter
//! pins nanosecond precision, disables partial-line acceptance and WAL bypass,
//! and imposes a complete operation timeout. A failed or cancelled write may
//! have committed; multi-request batches are never advertised as atomic.
//! Writes contain at most 10,000 points per call. Large individual strings are
//! not byte-capped here: size limits remain application/server policy.
//!
//! Configuration-based Providers allocate a pool per selection because the SDK
//! binds a client to a database. Clone a DB to reuse it, or construct a Provider
//! from an `Arc<Client>` to retain caller ownership. Dropping a selection never
//! explicitly closes a shared pool. Database selection starts no workers; an
//! explicit batch_writer owns its bounded queue and draining close operation.
//!
//! Record CRUD is a separate extension seam for later backends. Transactions,
//! graphs, migrations, protobuf reflection and caching are intentionally later
//! slices; no backend is given a capability it cannot implement.
//!
//! # Provisioning and sustained ingestion
//!
//! InfluxAdmin creates databases from code with explicit Retention. Core expiry
//! is fixed at creation; AlreadyExists never silently changes a retention rule.
//! Administration uses a small supplemental HTTP pool because these endpoints
//! are absent from the SDK; point writes and Flight queries still use the SDK.
//!
//! Collection::batch_writer adds bounded queue admission, count/timer/manual
//! flushing and a draining close. Enqueue success is not a server receipt: flush
//! or close to observe commitment or a terminal failure. Dropping the writer
//! starts draining while Tokio remains alive; explicitly close to await it.
//!
//! Reader streams are pull-based, hold one decoded server batch and share a
//! complete stream deadline, including time spent by a slow consumer. Drop
//! cancels transport. Cursors are opaque but not encrypted or authorization;
//! reuse them only with the same database, descriptor, bounds, filter and order.
//! The descriptor must include every tag in the table to page point identities.
//!
//! SeriesMaintenance::drop_table removes both schema and all observations. Selective
//! historical row deletion is absent on Core and returns Unsupported. Core
//! ignores the Enterprise data_only flag; this adapter never sends it.

#![warn(missing_docs)]

mod observe;
pub use observe::instrument;
mod access;
mod collection;
pub mod drivers;
mod error;
pub mod ingest;
mod lifecycle;
mod query;
mod reading;
mod record;
mod resource;
mod store;
mod typed_ingest;
mod typed_reading;

pub use collection::Collection;
pub use error::{Error, Result};
pub use query::{Aggregate, Buckets, Comparison, Predicate, Range, Reduce, Reduction};
pub use record::{Record, Value};
pub use resource::{Column, Kind, Resource, Role};
pub use store::{Capabilities, DB, Provider, RecordStore, TimeSeries};

pub use lifecycle::{DeleteRange, Retention, RowDeletion, SeriesMaintenance};
pub use reading::{Page, RecordStream, Scan, SeriesReader};

pub use ingest::{BatchWriter, IngestConfig};
pub use typed_ingest::TypedBatchWriter;
pub use typed_reading::{TypedPage, TypedStream};
