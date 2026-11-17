//! Transactions and tables: the capabilities a relational backend has beyond CRUD. Go's
//! `orm/schema.go`.

use std::sync::Arc;

use sea_query_sqlx::SqlxValues;
use tokio::sync::Mutex;

use super::Dialect;
use super::driver::SqlDriver;
use super::engine::{Conn, Engine, context, map};
use crate::records::{Migrator, Resource, Transactional, TxWork};
use crate::{Error, Result};

fn bare(sql: String) -> (String, SqlxValues) {
    (sql, SqlxValues(sea_query::Values(Vec::new())))
}

#[async_trait::async_trait]
impl<E: Engine> Transactional for SqlDriver<E> {
    /// Runs `work` in one transaction: committed when it returns `Ok`, rolled back on an
    /// error, which is returned as it was, or a panic. The [`DB`](crate::records::DB) `work`
    /// is given is bound to the transaction and carries the same schema selection, so a call
    /// inside behaves as one outside except for when it becomes visible. Called on that `DB`,
    /// `run` nests on a savepoint, as GORM's nested `Transaction` does. Go's `Driver.Run`.
    async fn run(&self, work: TxWork) -> Result<()> {
        let bound = |conn| SqlDriver::<E> {
            conn,
            schema: self.schema.clone(),
        };
        match &self.conn {
            Conn::Pool(pool) => {
                let shared = Arc::new(Mutex::new(Some(pool.begin().await.map_err(map)?)));
                let db = bound(Conn::Tx(shared.clone(), 1)).into_db();
                let out = work(&db).await;
                let Some(tx) = shared.lock().await.take() else {
                    return Err(Error::Invalid("the transaction finished twice".into()));
                };
                match out {
                    Ok(()) => tx.commit().await.map_err(map),
                    Err(e) => {
                        // The caller's error is the one worth reporting; a failed rollback
                        // still ends the transaction when the connection is dropped.
                        let _ = tx.rollback().await;
                        Err(e)
                    }
                }
            }
            Conn::Tx(shared, depth) => {
                let sp = format!("sp{depth}");
                self.conn.execute(bare(format!("SAVEPOINT {sp}"))).await?;
                let db = bound(Conn::Tx(shared.clone(), depth + 1)).into_db();
                match work(&db).await {
                    Ok(()) => {
                        let release = format!("RELEASE SAVEPOINT {sp}");
                        self.conn.execute(bare(release)).await.map(|_| ())
                    }
                    Err(e) => {
                        let undo = format!("ROLLBACK TO SAVEPOINT {sp}");
                        let _ = self.conn.execute(bare(undo)).await;
                        Err(e)
                    }
                }
            }
        }
    }
}

impl<E: Engine> SqlDriver<E> {
    fn quoted_table(&self, res: &Resource) -> String {
        E::DIALECT.quote_table(self.schema_of(res), &res.table)
    }
}

#[async_trait::async_trait]
impl<E: Engine> Migrator for SqlDriver<E> {
    /// `CREATE TABLE IF NOT EXISTS` from the descriptor: each column's `sql_type` (or the
    /// dialect's default for its kind), `PRIMARY KEY`, `UNIQUE`, `NOT NULL`. It creates and
    /// never alters: a table that exists in another shape is left as it is, for the first
    /// statement that cannot run to report. It does not create a Postgres schema or a MySQL
    /// database; a selected one must exist. Go's `EnsureSchema`.
    async fn ensure_schema(&self, res: &Resource) -> Result<()> {
        if res.columns.is_empty() {
            return Err(Error::Invalid(format!(
                "resource {:?} describes no columns",
                res.name
            )));
        }
        for stmt in enum_types(E::DIALECT, res) {
            if let Err(e) = self.conn.execute(bare(stmt)).await {
                return Err(context(
                    format!("cannot create the enum types of {}", res.name),
                    e,
                ));
            }
        }
        let table = self.quoted_table(res);
        let defs = E::DIALECT.column_defs(res);
        let stmt = format!("CREATE TABLE IF NOT EXISTS {table} ({defs})");
        match self.conn.execute(bare(stmt)).await {
            Ok(_) => Ok(()),
            Err(e) => Err(context(format!("cannot create {table}"), e)),
        }
    }

    /// `DROP TABLE IF EXISTS`: the table and everything in it. Go's `DropSchema`.
    async fn drop_schema(&self, res: &Resource) -> Result<()> {
        let table = self.quoted_table(res);
        match self
            .conn
            .execute(bare(format!("DROP TABLE IF EXISTS {table}")))
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(context(format!("cannot drop {table}"), e)),
        }
    }

    /// Whether the table is in the catalogue of its schema. Go's `HasSchema`.
    async fn has_schema(&self, res: &Resource) -> Result<bool> {
        let q = E::has_table(self.schema_of(res), &res.table);
        Ok(self.conn.count(q).await? > 0)
    }
}

/// On Postgres, a `CREATE TYPE ... AS ENUM` for each enum column stored by label whose
/// `sql_type` names its type, in number order, skipped when the type exists: the type the
/// sql target's DDL declares. Elsewhere none: MySQL declares `ENUM(...)` inline and SQLite
/// stores text.
fn enum_types(d: Dialect, res: &Resource) -> Vec<String> {
    if d != Dialect::Postgres {
        return Vec::new();
    }
    let labelled = res.columns.iter();
    let labelled = labelled.filter(|c| !c.labels.is_empty() && !c.sql_type.is_empty());
    labelled
        .map(|c| {
            let mut labels = c.labels.clone();
            labels.sort();
            let labels: Vec<String> = labels
                .iter()
                .map(|(_, l)| format!("'{}'", l.replace('\'', "''")))
                .collect();
            format!(
                "DO $$ BEGIN CREATE TYPE {} AS ENUM ({}); \
                 EXCEPTION WHEN duplicate_object THEN NULL; END $$",
                c.sql_type,
                labels.join(", ")
            )
        })
        .collect()
}
