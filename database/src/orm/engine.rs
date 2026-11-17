//! The seam between the dialect-neutral driver and one sqlx database: running a statement,
//! reading a column by its kind, and asking whether a table exists. A connection is either
//! the pool or the one transaction a [`Transactional::run`](crate::records::Transactional)
//! holds, so the same driver code runs inside and outside one, as Go's `*gorm.DB` does.

use std::sync::Arc;

use sea_query::QueryBuilder;
use sea_query_sqlx::SqlxValues;
use sqlx::{Database, Pool, Transaction};
use tokio::sync::Mutex;

use super::Dialect;
use crate::records::{Column, Value};
use crate::{Error, Result};

/// The backend name errors and the [`DB`](crate::records::DB) report.
pub(crate) const BACKEND: &str = "orm";

/// One sqlx database, as the driver needs it. Implemented for Postgres, MySQL and SQLite.
pub(crate) trait Engine: Database {
    /// The dialect, for quoting and default column types.
    const DIALECT: Dialect;
    /// sea-query's renderer for this dialect.
    type Builder: QueryBuilder + Default + Send;
    /// Runs a statement; the rows it affected.
    fn execute(c: &mut Self::Connection, sql: String, v: SqlxValues) -> Run<'_, u64>;
    /// Runs a query; its rows.
    fn fetch(c: &mut Self::Connection, sql: String, v: SqlxValues) -> Run<'_, Vec<Self::Row>>;
    /// Column `col` of `row` read as its kind: `None` when the row has no such column, so a
    /// table wider or narrower than the descriptor reads as Go's `map[string]any` would.
    fn decode(row: &Self::Row, col: &Column) -> Result<Option<Value>>;
    /// The first column of `row` as an integer, for `COUNT(*)`.
    fn count(row: &Self::Row) -> Result<i64>;
    /// A query whose single count is non-zero when table `table` exists in `schema` (the
    /// connection's current one when empty). Go's `Migrator().HasTable`.
    fn has_table(schema: &str, table: &str) -> (String, SqlxValues);
}

/// A statement in flight on a borrowed connection.
pub(crate) type Run<'c, T> = futures::future::BoxFuture<'c, sqlx::Result<T>>;

/// The transaction a [`Conn::Tx`] runs on; `None` once committed or rolled back.
pub(crate) type SharedTx<E> = Arc<Mutex<Option<Transaction<'static, E>>>>;

/// Where statements run: the pool, or one open transaction at a savepoint depth.
pub(crate) enum Conn<E: Database> {
    /// A connection from the pool per statement.
    Pool(Pool<E>),
    /// The transaction, and how many transactions are nested in it (1 at the outermost).
    Tx(SharedTx<E>, u32),
}

impl<E: Database> Clone for Conn<E> {
    fn clone(&self) -> Self {
        match self {
            Conn::Pool(p) => Conn::Pool(p.clone()),
            Conn::Tx(t, depth) => Conn::Tx(t.clone(), *depth),
        }
    }
}

impl<E: Engine> Conn<E> {
    /// Runs a statement; the rows it affected.
    pub(crate) async fn execute(&self, (sql, v): (String, SqlxValues)) -> Result<u64> {
        let n = match self {
            Conn::Pool(p) => E::execute(&mut *p.acquire().await.map_err(map)?, sql, v).await,
            Conn::Tx(t, _) => E::execute(&mut *open(&mut *t.lock().await)?, sql, v).await,
        };
        n.map_err(map)
    }

    /// Runs a query; its rows.
    pub(crate) async fn fetch(&self, (sql, v): (String, SqlxValues)) -> Result<Vec<E::Row>> {
        let rows = match self {
            Conn::Pool(p) => E::fetch(&mut *p.acquire().await.map_err(map)?, sql, v).await,
            Conn::Tx(t, _) => E::fetch(&mut *open(&mut *t.lock().await)?, sql, v).await,
        };
        rows.map_err(map)
    }

    /// Runs a `COUNT(*)` query.
    pub(crate) async fn count(&self, q: (String, SqlxValues)) -> Result<i64> {
        match self.fetch(q).await?.first() {
            Some(row) => E::count(row),
            None => Err(Error::Invalid("a count returned no row".into())),
        }
    }
}

fn open<'a, E: Database>(
    tx: &'a mut Option<Transaction<'static, E>>,
) -> Result<&'a mut E::Connection> {
    tx.as_deref_mut().ok_or_else(|| {
        Error::Invalid("the transaction this database was bound to has already finished".into())
    })
}

/// A sqlx error as the record contract reports it: a unique or primary-key violation is
/// [`Error::AlreadyExists`], what GORM's `TranslateError` turns into `ErrDuplicatedKey`.
pub(crate) fn map(err: sqlx::Error) -> Error {
    match &err {
        sqlx::Error::Database(db) if db.is_unique_violation() => Error::AlreadyExists,
        sqlx::Error::RowNotFound => Error::NotFound,
        _ => Error::Backend {
            backend: BACKEND,
            source: Box::new(err),
        },
    }
}

/// A failure with what was being attempted, as Go's `fmt.Errorf("gorm: cannot …: %w")`.
pub(crate) fn context(what: String, err: Error) -> Error {
    match err {
        Error::Backend { source, .. } => Error::Backend {
            backend: BACKEND,
            source: format!("{what}: {source}").into(),
        },
        other => other,
    }
}

/// `execute` and `fetch` for one engine: a statement without parameters (DDL, savepoints)
/// goes over the simple protocol, everything else is prepared with its bound values.
macro_rules! run_statements {
    () => {
        fn execute(
            c: &mut Self::Connection,
            sql: String,
            v: sea_query_sqlx::SqlxValues,
        ) -> super::engine::Run<'_, u64> {
            use sqlx::Executor as _;
            Box::pin(async move {
                let done = if v.0.0.is_empty() {
                    c.execute(sqlx::raw_sql(sqlx::AssertSqlSafe(sql))).await?
                } else {
                    c.execute(sqlx::query_with(sqlx::AssertSqlSafe(sql), v))
                        .await?
                };
                Ok(done.rows_affected())
            })
        }

        fn fetch(
            c: &mut Self::Connection,
            sql: String,
            v: sea_query_sqlx::SqlxValues,
        ) -> super::engine::Run<'_, Vec<Self::Row>> {
            use sqlx::Executor as _;
            Box::pin(async move {
                c.fetch_all(sqlx::query_with(sqlx::AssertSqlSafe(sql), v))
                    .await
            })
        }
    };
}
pub(crate) use run_statements;

/// One string parameter list, for the catalogue queries written by hand.
pub(crate) fn strings<const N: usize>(values: [&str; N]) -> SqlxValues {
    let v = values.map(|s| sea_query::Value::String(Some(s.to_owned())));
    SqlxValues(sea_query::Values(v.to_vec()))
}
