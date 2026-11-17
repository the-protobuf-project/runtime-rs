//! CRUD over any engine through sea-query statements, every resource driven by its
//! descriptor rather than a model type. Go's `orm/driver.go`.

use std::sync::Arc;

use prost_reflect::DynamicMessage;
use sea_query::{Alias, Asterisk, Expr, ExprTrait, Query, TableRef};
use sea_query_sqlx::SqlxBinder;

use super::engine::{Conn, Engine};
use super::{query, values};
use crate::records::{
    self, Capabilities, DB, ListOptions, ListResult, Resource, Row, WriteResult, core,
};
use crate::{Error, Result};

/// The driver over one engine, bound to the pool or to a transaction. `schema`, when set,
/// overrides each resource's: the descriptor says what a Book is, the selection says whose.
pub(crate) struct SqlDriver<E: Engine> {
    pub(crate) conn: Conn<E>,
    pub(crate) schema: String,
}

impl<E: Engine> SqlDriver<E> {
    /// The [`DB`] over this driver: CRUD, transactions and schema; no batches or watches.
    pub(crate) fn into_db(self) -> DB {
        let name = self.schema.clone();
        let d = Arc::new(self);
        let caps = Capabilities {
            tx: Some(d.clone()),
            schema: Some(d.clone()),
            ..Capabilities::new(d)
        };
        DB::build(super::engine::BACKEND, name, caps, None)
    }

    /// The schema statements run in: the selected one, else the resource's own.
    pub(crate) fn schema_of<'a>(&'a self, res: &'a Resource) -> &'a str {
        if self.schema.is_empty() {
            &res.schema
        } else {
            &self.schema
        }
    }

    /// The table, schema-qualified when there is a schema. Go's `Driver.table`.
    pub(super) fn table(&self, res: &Resource) -> TableRef {
        match self.schema_of(res) {
            "" => Alias::new(&res.table).into(),
            s => (Alias::new(s), Alias::new(&res.table)).into(),
        }
    }

    pub(super) fn sql(&self, q: &impl SqlxBinder) -> (String, sea_query_sqlx::SqlxValues) {
        q.build_sqlx(E::Builder::default())
    }

    pub(super) fn by_key(&self, res: &Resource, key: &str) -> Result<Expr> {
        let (pk, v) = values::key(E::DIALECT, res, key)?;
        Ok(Expr::col(Alias::new(pk)).eq(v))
    }

    /// Each column's value as an expression: bound, and cast where the column needs it.
    fn values(&self, res: &Resource, cols: &Row) -> Result<Vec<(Alias, Expr)>> {
        cols.iter()
            .map(|(k, v)| {
                let col = res.lookup_column(k);
                let kind = col.map(|c| c.kind).unwrap_or_default();
                let bound = values::bind(E::DIALECT, kind, v)?;
                Ok((Alias::new(k), values::expr(E::DIALECT, col, bound)))
            })
            .collect()
    }
}

#[async_trait::async_trait]
impl<E: Engine> records::Driver for SqlDriver<E> {
    async fn create(&self, res: &Resource, msg: &DynamicMessage) -> Result<WriteResult> {
        let mut cols = records::message_to_columns(res, msg)?;
        core::fill_managed(res, &mut cols, true);
        let (names, vals): (Vec<_>, Vec<_>) = self.values(res, &cols)?.into_iter().unzip();
        let mut q = Query::insert();
        q.into_table(self.table(res)).columns(names);
        q.values(vals).map_err(|e| Error::Invalid(e.to_string()))?;
        self.conn.execute(self.sql(&q)).await?;
        // What was stored, generated key and audit times included, not what was handed in.
        Ok(WriteResult::done(records::columns_to_message(res, &cols)?))
    }

    async fn get(&self, res: &Resource, key: &str) -> Result<DynamicMessage> {
        self.read(res, key, false).await
    }

    /// `FOR UPDATE` inside a transaction; SQLite, which locks the database instead, has none.
    async fn get_for_update(&self, res: &Resource, key: &str) -> Result<DynamicMessage> {
        self.read(res, key, matches!(self.conn, Conn::Tx(..))).await
    }

    async fn update(&self, res: &Resource, msg: &DynamicMessage) -> Result<WriteResult> {
        let mut cols = records::message_to_columns(res, msg)?;
        let key = records::key_of(res, msg)?;
        core::fill_managed(res, &mut cols, false);
        // The key is the lookup, not part of SET, but it belongs in the message returned.
        let mut set = cols.clone();
        set.remove(&res.pk_column);
        if set.is_empty() {
            return match self.exists(res, &key).await? {
                true => Ok(WriteResult::done(records::columns_to_message(res, &cols)?)),
                false => Err(Error::NotFound),
            };
        }
        let mut q = Query::update();
        q.table(self.table(res)).and_where(self.by_key(res, &key)?);
        q.values(self.values(res, &set)?);
        if self.conn.execute(self.sql(&q)).await? == 0 {
            return Err(Error::NotFound);
        }
        Ok(WriteResult::done(records::columns_to_message(res, &cols)?))
    }

    async fn delete(&self, res: &Resource, key: &str) -> Result<()> {
        let mut q = Query::delete();
        q.from_table(self.table(res))
            .and_where(self.by_key(res, key)?);
        match self.conn.execute(self.sql(&q)).await? {
            0 => Err(Error::NotFound),
            _ => Ok(()),
        }
    }

    async fn list(&self, res: &Resource, opts: &ListOptions) -> Result<ListResult> {
        let cond = query::filter(E::DIALECT, res, opts)?;
        let total = match opts.omit_total {
            true => core::NO_TOTAL,
            false => self.count(res, opts).await?,
        };
        let limit = core::page_size(opts.page_size);
        let offset = core::decode_token(&opts.page_token);
        let mut q = Query::select();
        q.column(Asterisk).from(self.table(res)).cond_where(cond);
        for (col, dir) in query::order(res, &opts.order_by) {
            q.order_by(Alias::new(col), dir);
        }
        q.limit(core::fetch_limit(limit, opts.omit_total) as u64);
        if offset > 0 {
            q.offset(offset as u64);
        }
        let rows = self.fetch(res, &q).await?;
        // Trimmed before decoding: the row read only to prove another page exists is dropped
        // as a map, not after becoming a message nobody sees.
        let (rows, next) = core::trim_page(rows, offset, limit, total, opts.omit_total);
        Ok(ListResult {
            items: records::rows_to_messages(res, &rows)?,
            next_page_token: next,
            total,
        })
    }

    async fn count(&self, res: &Resource, opts: &ListOptions) -> Result<i64> {
        let mut q = Query::select();
        q.expr(Expr::col(Asterisk).count()).from(self.table(res));
        q.cond_where(query::filter(E::DIALECT, res, opts)?);
        self.conn.count(self.sql(&q)).await
    }

    async fn exists(&self, res: &Resource, key: &str) -> Result<bool> {
        let mut q = Query::select();
        q.expr(Expr::col(Asterisk).count()).from(self.table(res));
        q.and_where(self.by_key(res, key)?);
        Ok(self.conn.count(self.sql(&q)).await? > 0)
    }
}
