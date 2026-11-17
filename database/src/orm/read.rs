//! Reading rows: `SELECT *` decoded through the descriptor, and one record by key, locked
//! for update when a transaction is about to write it. Go's `fetch` and `First`.

use prost_reflect::DynamicMessage;
use sea_query::{Asterisk, LockType, Query, SelectStatement};

use super::driver::SqlDriver;
use super::engine::Engine;
use crate::records::{self, Resource, Row};
use crate::{Error, Result};

impl<E: Engine> SqlDriver<E> {
    /// Rows of a `SELECT *` as column maps keyed by the descriptor. Go's `fetch`.
    pub(super) async fn fetch(&self, res: &Resource, q: &SelectStatement) -> Result<Vec<Row>> {
        let rows = self.conn.fetch(self.sql(q)).await?;
        rows.iter()
            .map(|row| {
                let mut out = Row::new();
                for col in &res.columns {
                    if let Some(v) = E::decode(row, col)? {
                        out.insert(col.name.clone(), v);
                    }
                }
                Ok(out)
            })
            .collect()
    }

    /// The record under `key`, its row locked for update when `lock`. SQLite renders no
    /// lock: it serializes writers by locking the whole database.
    pub(super) async fn read(
        &self,
        res: &Resource,
        key: &str,
        lock: bool,
    ) -> Result<DynamicMessage> {
        let mut q = Query::select();
        q.column(Asterisk)
            .from(self.table(res))
            .and_where(self.by_key(res, key)?)
            .limit(1);
        if lock {
            q.lock(LockType::Update);
        }
        match self.fetch(res, &q).await?.first() {
            Some(row) => records::columns_to_message(res, row),
            None => Err(Error::NotFound),
        }
    }
}
