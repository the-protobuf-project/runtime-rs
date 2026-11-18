//! Durable records over proto messages: one CRUD contract for SQL, document and key-value
//! backends, driven by a [`Resource`] descriptor rather than per-resource Rust types. The
//! port of runtime-go's `database/store` and `database/core`.
//!
//! The steps are the same for every backend: a client you own, a database selected from it,
//! then CRUD through the [`DB`] that results.
//!
//! ```ignore
//! let db = provider.set_database("tenant_a").await?;
//! db.schema.ensure_schema(&books).await?;          // the table the descriptor describes
//! db.create(&books, &book.transcode_to_dynamic()).await?;
//! let books = Typed::<pb::Book>::new(&db, Arc::new(books))?;
//! let dune = books.get("books/dune").await?;       // already a pb::Book
//! db.close().await?;
//! ```
//!
//! A selected database name overrides [`Resource::schema`], which is what lets one set of
//! generated descriptors serve many tenants. Backends are not equally capable: a capability
//! a backend lacks is a refusal naming it ([`crate::Error::Unimplemented`]), never missing.
//!
//! The SQL backend is `database::orm` (features `orm`, `orm-postgres`, `orm-mysql`,
//! `orm-sqlite`): Postgres, MySQL and SQLite through one dynamic driver, as Go's
//! `database/orm`.
//!
//! This sits beside [`crate::store`], the time-series contract the InfluxDB and Arrow
//! backends implement over flat scalar records.

mod bridge;
mod capabilities;
mod convert;
pub mod core;
mod driver;
mod kind;
pub mod observe;
mod provider;
mod refuses;
mod resource;
mod typed;
mod value;

pub use bridge::{columns_to_message, key_of, message_to_columns, rows_to_messages};
pub use capabilities::{
    Batcher, Change, ChangeKind, Migrator, Transactional, TxWork, WatchOptions, Watcher,
};
pub use driver::{Driver, ListOptions, ListResult, WriteResult};
pub use provider::{Capabilities, DB, Provider, ReleaseFn, check_database_name};
pub use resource::{Column, ForeignKey, Generated, Kind, Resource};
pub use typed::{Registry, Typed};
pub use value::{Row, Value};
