//! Many records a request. Go's `d1/bulk.go`.

use std::collections::HashMap;

use prost_reflect::DynamicMessage;

use super::driver::D1Driver;
use super::sql::{self, MAX_PARAMS, quote};
use super::wire::Statement;
use super::write::insert;
use crate::Result;
use crate::records::{self, Batcher, Resource, WriteResult};

/// How many inserts go into one request. D1 bounds a request by time, thirty seconds for
/// the whole batch, rather than by count: this stays well inside it while keeping round
/// trips from being the cost.
const BATCH_SIZE: usize = 50;
#[async_trait::async_trait]
impl Batcher for D1Driver {
    /// Inserts a batch per request. Each request is one unit: a duplicate anywhere in it
    /// fails all of it, without saying which record it was, and requests before the failing
    /// one have already landed.
    async fn create_many(
        &self,
        res: &Resource,
        msgs: &[DynamicMessage],
    ) -> Result<Vec<WriteResult>> {
        let mut out = Vec::with_capacity(msgs.len());
        for chunk in msgs.chunks(BATCH_SIZE) {
            let (stmts, stored): (Vec<_>, Vec<_>) = chunk
                .iter()
                .map(|m| insert(res, m))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .unzip();
            self.client.query(&self.id, &stmts).await?;
            for cols in &stored {
                out.push(WriteResult::done(records::columns_to_message(res, cols)?));
            }
        }
        Ok(out)
    }

    /// The records under `keys`, in order, read in pieces of at most the bind limit.
    async fn get_many(
        &self,
        res: &Resource,
        keys: &[String],
    ) -> Result<Vec<Option<DynamicMessage>>> {
        let mut found = HashMap::with_capacity(keys.len());
        for chunk in keys.chunks(MAX_PARAMS) {
            let text = format!(
                "SELECT * FROM {} WHERE {} IN ({})",
                quote(&res.table),
                quote(&res.pk_column),
                sql::placeholders(chunk.len())
            );
            for row in self
                .select(res, Statement::new(text, chunk.to_vec()))
                .await?
            {
                let key = row
                    .get(&res.pk_column)
                    .map(|v| v.key_string())
                    .unwrap_or_default();
                found.insert(key, records::columns_to_message(res, &row)?);
            }
        }
        Ok(keys.iter().map(|k| found.get(k).cloned()).collect())
    }
}
