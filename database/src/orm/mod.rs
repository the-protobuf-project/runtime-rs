//! The SQL backend of [`records`](crate::records): one dynamic driver for every resource over
//! PostgreSQL, MySQL or SQLite, built on [sqlx](https://docs.rs/sqlx) pools and
//! [sea-query](https://docs.rs/sea-query) statements, with no model types and no ORM. The
//! port of runtime-go's `database/orm`, which does the same over GORM's map + `Table` API.
//!
//! Each dialect is a feature (`orm-postgres`, `orm-mysql`, `orm-sqlite`; `orm` is all three),
//! so a service links only the client it talks to.
//!
//! ```ignore
//! use database::orm::{OrmProvider, sqlx::PgPool};
//! use database::records::Provider;
//!
//! let pool = PgPool::connect("postgres://…").await?;   // yours: this crate never closes it
//! let tenant = OrmProvider::new(pool).set_database("tenant_a").await?;
//! tenant.schema.ensure_schema(&books).await?;           // CREATE TABLE from the descriptor
//! tenant.tx.run(Box::new(move |tx| Box::pin(async move {
//!     tx.create(&books, &book).await.map(|_| ())
//! }))).await?;
//! ```
//!
//! What it implements, as Go's driver does: [`Driver`](crate::records::Driver),
//! [`Transactional`](crate::records::Transactional) (nested calls use savepoints) and
//! [`Migrator`](crate::records::Migrator); batches and watches refuse by name.
//!
//! - Identifiers are quoted per dialect (backticks on MySQL, double quotes elsewhere) and
//!   filter values are bound, typed by their column's kind. An AIP-160 filter becomes a
//!   sea-query `Condition` as Go's gorm engine writes it: `NOT COALESCE(x, FALSE)` and
//!   `LOWER(col) LIKE '%v%'` for contains and free text.
//! - A unique or primary-key violation is [`Error::AlreadyExists`](crate::Error); a missing
//!   row on get, update or delete is [`Error::NotFound`](crate::Error).
//! - Keys and audit times are filled by [`core::fill_managed`](crate::records::core), and
//!   `create`/`update` return what was stored.
//! - Pages are offset tokens; `omit_total` skips the count and reads one row past the page.
//!
//! On SQLite an in-memory database is per connection: open such a pool with one connection.

mod dialect;
mod driver;
mod engine;
#[cfg(feature = "orm-mysql")]
mod mysql;
#[cfg(feature = "orm-postgres")]
mod postgres;
mod provider;
mod query;
mod rows;
mod schema;
#[cfg(feature = "orm-sqlite")]
mod sqlite;
mod values;

pub use dialect::Dialect;
pub use provider::{OrmProvider, driver};
/// The sqlx this backend is built on, so a caller opens its pool with the same version.
pub use sqlx;

/// A pool of one of the dialects this crate was built with. Converts from each sqlx pool,
/// so [`OrmProvider::new`] takes the pool itself.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Pool {
    /// PostgreSQL.
    #[cfg(feature = "orm-postgres")]
    Postgres(sqlx::PgPool),
    /// MySQL.
    #[cfg(feature = "orm-mysql")]
    MySql(sqlx::MySqlPool),
    /// SQLite.
    #[cfg(feature = "orm-sqlite")]
    Sqlite(sqlx::SqlitePool),
}

impl Pool {
    /// The pool's dialect.
    pub fn dialect(&self) -> Dialect {
        match self {
            #[cfg(feature = "orm-postgres")]
            Pool::Postgres(_) => Dialect::Postgres,
            #[cfg(feature = "orm-mysql")]
            Pool::MySql(_) => Dialect::MySql,
            #[cfg(feature = "orm-sqlite")]
            Pool::Sqlite(_) => Dialect::Sqlite,
        }
    }
}

#[cfg(feature = "orm-postgres")]
impl From<sqlx::PgPool> for Pool {
    fn from(p: sqlx::PgPool) -> Self {
        Pool::Postgres(p)
    }
}

#[cfg(feature = "orm-mysql")]
impl From<sqlx::MySqlPool> for Pool {
    fn from(p: sqlx::MySqlPool) -> Self {
        Pool::MySql(p)
    }
}

#[cfg(feature = "orm-sqlite")]
impl From<sqlx::SqlitePool> for Pool {
    fn from(p: sqlx::SqlitePool) -> Self {
        Pool::Sqlite(p)
    }
}
