//! The Cloudflare D1 backend of [`records`](crate::records): SQLite served over HTTP,
//! reached through the REST API's query endpoint with this workspace's
//! [`network`](https://docs.rs/tpp-network) HTTP client. The port of runtime-go's
//! `database/d1`, which reaches the same endpoint through Cloudflare's Go SDK; Cloudflare's
//! Rust SDK has no D1 endpoints.
//!
//! Two layers, as the SQL backend has a sqlx pool under its driver:
//!
//! - [`D1Client`] is SQL in and rows out, and knows nothing about proto:
//!   [`query`](D1Client::query) runs [`Statement`]s and returns an [`Outcome`] each, rows as
//!   JSON objects; [`ping`](D1Client::ping) and [`lookup`](D1Client::lookup) find a
//!   database. [`D1Client::connect`] builds it from an account and an API token.
//! - [`D1Provider`] selects a database and gives the records driver over it: proto
//!   messages read and written through their descriptors, as every records provider.
//!
//! ```ignore
//! use database::d1::{D1Client, D1Config, D1Provider};
//! use database::records::Provider;
//!
//! let client = D1Client::connect(D1Config {
//!     account_id: account.into(),
//!     api_token: token.into(),
//!     database_id: id.into(),
//!     ..D1Config::default()
//! }).await?;
//! let provider = D1Provider::new(std::sync::Arc::new(client));
//! let db = provider.set_database("").await?;          // the configured database
//! let tenant = provider.set_database("tenant_a").await?; // the D1 database named tenant_a
//! db.schema.ensure_schema(&books).await?;
//! ```
//!
//! - **A name is a database.** SQLite has no schemas, so [`set_database`](crate::records::Provider::set_database) looks
//!   the name up in the account and drives that database; an empty name is
//!   [`D1Config::database_id`]. Creating one is an account change (`wrangler d1 create`),
//!   not a migration, and a name that matches none is an error.
//! - **Values travel as text.** The endpoint binds parameters as strings, and SQLite's
//!   column affinity turns them back: an `INTEGER` column stores 42, not `'42'`. The column
//!   types are therefore this driver's to choose, and `sql_type` is not honored. Bytes are
//!   base64 in a `TEXT` column; timestamps are fixed-width RFC 3339 text in UTC, so comparing
//!   them as text is comparing them as times.
//! - **No transactions.** The API has no interactive transaction, only a batch fixed before
//!   it is sent, so [`Transactional`](crate::records::Transactional) refuses by name.
//!   [`Batcher::create_many`](crate::records::Batcher) sends its inserts as such a batch.
//! - **Writes are not retried.** D1 does not retry a write and neither does this driver: a
//!   create retried after it landed would report `AlreadyExists` for a record that is there.
//! - A unique or primary-key violation is [`Error::AlreadyExists`](crate::Error); a missing
//!   row on get, update or delete is [`Error::NotFound`](crate::Error).

mod api;
mod bulk;
mod client;
mod driver;
mod filter;
mod provider;
mod schema;
mod sql;
mod wire;
mod write;

pub use client::{D1Client, D1Config};
pub use provider::D1Provider;
pub use wire::{Outcome, Statement};

/// The backend name in errors and telemetry.
const BACKEND: &str = "d1";
