//! Durable records in Redis: the [`records`](crate::records) contract over a key-value store,
//! each record its proto wire bytes under one key, with a sorted set of keys beside them so
//! they can be listed. The port of runtime-go's `database/redis`.
//!
//! # Records are proto bytes, not JSON
//!
//! Redis stores opaque bytes, so the only question is the encoding, and JSON answers it wrongly
//! in two ways that never announce themselves: a bytes column comes back as its base64 text, and
//! a `uint64` past 2^53 as a different number. Proto wire format loses neither, and a Go program
//! reads what a Rust one wrote.
//!
//! # What it has and what it does not
//!
//! CRUD, [`Migrator`](crate::records::Migrator) and [`Batcher`](crate::records::Batcher); not
//! [`Transactional`](crate::records::Transactional), which Redis cannot roll back across the
//! keys one write touches, so [`DB::tx`](crate::records::DB::tx) refuses naming `redis`.
//!
//! A column the descriptor marks unique is enforced here, as the server has no constraints: a
//! reservation key per value, claimed, moved and released in the same Lua script as the record,
//! so two writers racing on one value cannot both win and a failed write leaves nothing behind.
//!
//! `list` pages and orders by primary key in the server, either direction; another `order_by`
//! column falls back to the key. A filter is refused by name, as Go refuses it: Redis has no
//! query language, and filtering here would read the whole table to return ten rows.
//!
//! # One client for cache and database
//!
//! The provider takes a [`ConnectionManager`](::redis::aio::ConnectionManager) you own and never
//! closes it. Hand it the cache's (`cache::drivers::redis::RedisClient::connection_manager`) and
//! the two share one multiplexed connection, as Go's cache, database and streams share a client:
//!
//! ```ignore
//! let client = Arc::new(RedisClient::connect(config).await?);
//! let caches = cache::drivers::redis::RedisProvider::new(client.clone(), cache::Config::default());
//! let records = database::redis::RedisProvider::new(client.connection_manager().await?)
//!     .with_prefix("app");
//! let db = records.set_database("tenant_a").await?;
//! db.create(&books, &book).await?;
//! ```

mod bulk;
mod codec;
mod driver;
mod keys;
mod list;
mod provider;
mod schema;
mod scripts;
mod unique;
mod write;

pub use driver::RedisDriver;
pub use provider::RedisProvider;

/// The name this backend reports and refuses under. Go's `Provider.Backend`.
pub const BACKEND: &str = "redis";
