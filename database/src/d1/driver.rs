//! CRUD over one D1 database. Go's `d1/driver.go`.

use std::sync::Arc;

use prost_reflect::DynamicMessage;

use super::BACKEND;
use super::client::D1Client;
use super::filter;
use super::sql::{self, bind, quote};
use super::wire::{Outcome, Statement};
use super::write::{by_key, insert};
use crate::records::{
    self, Capabilities, DB, ListOptions, ListResult, Resource, Row, WriteResult, core,
};
use crate::{Error, Result};

/// The driver over one database.
pub(crate) struct D1Driver {
    pub(crate) client: Arc<D1Client>,
    pub(crate) id: String,
}

impl D1Driver {
    pub(crate) fn into_db(self, name: &str) -> DB {
        let d = Arc::new(self);
        let caps = Capabilities {
            schema: Some(d.clone()),
            batch: Some(d.clone()),
            ..Capabilities::new(d)
        };
        DB::build(BACKEND, name, caps, None)
    }

    pub(crate) async fn run(&self, stmt: Statement) -> Result<Outcome> {
        let mut out = self.client.query(&self.id, &[stmt]).await?;
        Ok(out.remove(0))
    }

    pub(crate) async fn select(&self, res: &Resource, stmt: Statement) -> Result<Vec<Row>> {
        self.run(stmt)
            .await?
            .rows
            .into_iter()
            .map(|raw| sql::row(res, raw))
            .collect()
    }

    async fn count_where(&self, res: &Resource, opts: &ListOptions) -> Result<i64> {
        let mut text = format!("SELECT COUNT(*) AS n FROM {}", quote(&res.table));
        let mut args = Vec::new();
        if let Some((pred, bound)) = filter::filter(res, opts)? {
            text.push_str(&format!(" WHERE {pred}"));
            args = bound;
        }
        let out = self.run(Statement::new(text, args)).await?;
        out.rows
            .first()
            .and_then(|r| r.get("n"))
            .and_then(serde_json::Value::as_i64)
            .ok_or_else(|| Error::Backend {
                backend: BACKEND,
                source: "a count came back without a number".into(),
            })
    }
}

#[async_trait::async_trait]
impl records::Driver for D1Driver {
    async fn create(&self, res: &Resource, msg: &DynamicMessage) -> Result<WriteResult> {
        let (stmt, cols) = insert(res, msg)?;
        self.run(stmt).await?;
        // What was stored, generated key and audit times included, not what was handed in.
        Ok(WriteResult::done(records::columns_to_message(res, &cols)?))
    }

    async fn get(&self, res: &Resource, key: &str) -> Result<DynamicMessage> {
        let text = format!(
            "SELECT * FROM {} WHERE {} LIMIT 1",
            quote(&res.table),
            by_key(res)
        );
        let rows = self
            .select(res, Statement::new(text, vec![key.to_owned()]))
            .await?;
        match rows.first() {
            Some(row) => records::columns_to_message(res, row),
            None => Err(Error::NotFound),
        }
    }

    async fn update(&self, res: &Resource, msg: &DynamicMessage) -> Result<WriteResult> {
        let mut cols = records::message_to_columns(res, msg)?;
        let key = records::key_of(res, msg)?;
        core::fill_managed(res, &mut cols, false);
        let (mut sets, mut args) = (Vec::new(), Vec::new());
        for (name, v) in cols.iter().filter(|(n, _)| **n != res.pk_column) {
            match bind(v) {
                Some(text) => {
                    sets.push(format!("{} = ?", quote(name)));
                    args.push(text);
                }
                None => sets.push(format!("{} = NULL", quote(name))),
            }
        }
        if sets.is_empty() {
            return match self.exists(res, &key).await? {
                true => Ok(WriteResult::done(records::columns_to_message(res, &cols)?)),
                false => Err(Error::NotFound),
            };
        }
        args.push(key);
        let text = format!(
            "UPDATE {} SET {} WHERE {}",
            quote(&res.table),
            sets.join(", "),
            by_key(res)
        );
        if self.run(Statement::new(text, args)).await?.changes == 0 {
            return Err(Error::NotFound);
        }
        Ok(WriteResult::done(records::columns_to_message(res, &cols)?))
    }

    async fn delete(&self, res: &Resource, key: &str) -> Result<()> {
        let text = format!("DELETE FROM {} WHERE {}", quote(&res.table), by_key(res));
        match self
            .run(Statement::new(text, vec![key.to_owned()]))
            .await?
            .changes
        {
            0 => Err(Error::NotFound),
            _ => Ok(()),
        }
    }

    async fn list(&self, res: &Resource, opts: &ListOptions) -> Result<ListResult> {
        let total = match opts.omit_total {
            true => core::NO_TOTAL,
            false => self.count_where(res, opts).await?,
        };
        let limit = core::page_size(opts.page_size);
        let offset = core::decode_token(&opts.page_token);
        let mut text = format!("SELECT * FROM {}", quote(&res.table));
        let mut args = Vec::new();
        if let Some((pred, bound)) = filter::filter(res, opts)? {
            text.push_str(&format!(" WHERE {pred}"));
            args = bound;
        }
        text.push_str(&format!(
            " ORDER BY {} LIMIT {} OFFSET {}",
            filter::order(res, &opts.order_by),
            core::fetch_limit(limit, opts.omit_total),
            offset
        ));
        let rows = self.select(res, Statement::new(text, args)).await?;
        let (rows, next) = core::trim_page(rows, offset, limit, total, opts.omit_total);
        Ok(ListResult {
            items: records::rows_to_messages(res, &rows)?,
            next_page_token: next,
            total,
        })
    }

    async fn count(&self, res: &Resource, opts: &ListOptions) -> Result<i64> {
        self.count_where(res, opts).await
    }

    async fn exists(&self, res: &Resource, key: &str) -> Result<bool> {
        let text = format!(
            "SELECT 1 AS hit FROM {} WHERE {} LIMIT 1",
            quote(&res.table),
            by_key(res)
        );
        Ok(!self
            .run(Statement::new(text, vec![key.to_owned()]))
            .await?
            .rows
            .is_empty())
    }
}
