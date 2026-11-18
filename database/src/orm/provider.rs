//! A relational backend over a pool the caller owns, and the [`DB`] a selected database
//! gives. Go's `orm/provider.go` and `orm.New`.

use sqlx::{Connection, Database};

use super::Pool;
use super::driver::SqlDriver;
use super::engine::{BACKEND, Conn, Engine, context, map};
use crate::Result;
use crate::records::{DB, Provider, check_database_name};

/// A relational backend bound to a pool you own: this crate does not open it and does not
/// close it, so hand the same one to several providers and they share it. Go's
/// `orm.Provider`.
#[derive(Clone, Debug)]
pub struct OrmProvider {
    pool: Pool,
}

impl OrmProvider {
    /// A provider over `pool`. Go's `orm.NewProvider`.
    pub fn new(pool: impl Into<Pool>) -> Self {
        Self { pool: pool.into() }
    }

    /// The pool, e.g. for the statements this backend does not issue.
    pub fn pool(&self) -> &Pool {
        &self.pool
    }
}

/// The [`DB`] over `pool` with each resource on the schema its descriptor names: no
/// selection and no round trip. Use [`OrmProvider::set_database`] to select one at runtime.
/// Go's `orm.New`.
pub fn driver(pool: impl Into<Pool>) -> DB {
    build(&pool.into(), String::new())
}

fn build(pool: &Pool, schema: String) -> DB {
    fn db<E: Engine>(pool: &sqlx::Pool<E>, schema: String) -> DB {
        SqlDriver::<E> {
            conn: Conn::Pool(pool.clone()),
            schema,
        }
        .into_db()
    }
    match pool {
        #[cfg(feature = "orm-postgres")]
        Pool::Postgres(p) => db(p, schema),
        #[cfg(feature = "orm-mysql")]
        Pool::MySql(p) => db(p, schema),
        #[cfg(feature = "orm-sqlite")]
        Pool::Sqlite(p) => db(p, schema),
    }
}

async fn ping<E: Database>(pool: &sqlx::Pool<E>) -> sqlx::Result<()> {
    pool.acquire().await?.ping().await
}

#[async_trait::async_trait]
impl Provider for OrmProvider {
    /// Selects database `name` and returns the driver over it, after reaching the server, so
    /// an unreachable database fails on this line rather than three layers into a handler.
    ///
    /// The name qualifies every table, overriding [`Resource::schema`]: on Postgres it is a
    /// schema, on MySQL a database, on SQLite an attached database, and it must exist. It is
    /// checked with [`check_database_name`] first, since in a multi-tenant program it comes
    /// from a request. Empty keeps each resource's own schema. `close` is a no-op: the pool
    /// is yours. Go's `Provider.SetDatabase`.
    ///
    /// [`Resource::schema`]: crate::records::Resource::schema
    async fn set_database(&self, name: &str) -> Result<DB> {
        check_database_name(name)?;
        let reached = match &self.pool {
            #[cfg(feature = "orm-postgres")]
            Pool::Postgres(p) => ping(p).await,
            #[cfg(feature = "orm-mysql")]
            Pool::MySql(p) => ping(p).await,
            #[cfg(feature = "orm-sqlite")]
            Pool::Sqlite(p) => ping(p).await,
        };
        let what = || format!("database {name:?} is not reachable");
        reached.map_err(|e| context(what(), map(e)))?;
        Ok(crate::records::observe::instrument_default(build(
            &self.pool,
            name.to_owned(),
        )))
    }

    /// `orm`.
    fn backend(&self) -> &'static str {
        BACKEND
    }
}
