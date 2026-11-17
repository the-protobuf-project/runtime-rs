//! MongoDB as a [`records`](crate::records) backend: a resource is a collection, a record a
//! document, and the descriptor's columns the document's fields, so what lands in MongoDB is
//! queryable with the tools people already use rather than a blob only this program reads.
//! The port of runtime-go's `database/mongodb`, behind the `mongodb` feature.
//!
//! ```ignore
//! let client = MongoClient::connect(MongoConfig::address("localhost:27017")).await?;
//! let db = MongoProvider::new(client.clone()).set_database("tenant_a").await?;
//! db.schema.ensure_schema(&books).await?;      // the collection and its unique indexes
//! db.create(&books, &book).await?;
//! client.close().await;                       // yours: nothing here closes it
//! ```
//!
//! # What it has
//!
//! All four capabilities: [`Transactional`](crate::records::Transactional),
//! [`Migrator`](crate::records::Migrator), [`Batcher`](crate::records::Batcher) and
//! [`Watcher`](crate::records::Watcher), the last being the reason to reach for MongoDB when
//! something has to react to writes rather than poll.
//!
//! # It needs a replica set
//!
//! Transactions and change streams are replica-set features; a standalone server has
//! neither. A single-node replica set is enough, and is what `database/docker` runs.
//!
//! # The encoding
//!
//! The primary key is stored as `_id`, so there is one idea of identity rather than two that
//! can disagree. Integers are int64 and enums int32; times are BSON dates (milliseconds, UTC);
//! bytes are generic binary; an unset optional field is null. A `uint64` past `i64::MAX` is a
//! Decimal128: BSON has no unsigned 64-bit integer, and Decimal128 holds it exactly while
//! still sorting and comparing as a number on the server.
//!
//! # Filtering
//!
//! [`ListOptions::filter`](crate::records::ListOptions::filter) takes AIP-160 as
//! [`records::core::filter`](crate::records::core::filter) parses it: `AND`, `OR`, `NOT`,
//! comparisons and has, values typed by the column's kind. `NOT` is pushed down to the
//! comparisons rather than sent as `$nor`, and `!=` is `$nin: [value, null]`, so a null
//! field matches no comparison, as on SQL. What the parser refuses is refused by name.

mod bulk;
mod client;
mod codec;
mod crud;
mod driver;
mod error;
mod filter;
mod provider;
mod schema;
#[cfg(test)]
mod tests;
mod tx;
mod watch;

pub use client::{MongoClient, MongoConfig};
pub use driver::MongoDriver;
pub use provider::MongoProvider;

/// The backend's name, as [`DB::backend`](crate::records::DB::backend) and errors carry it.
pub const BACKEND: &str = "mongodb";
