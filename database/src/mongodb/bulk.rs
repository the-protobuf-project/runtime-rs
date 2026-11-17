//! [`Batcher`] over MongoDB: one ordered insert, one `$in` read per 512 keys. Go's
//! `mongodb/bulk.go`.

use std::collections::HashMap;

use bson::{Bson, doc};
use prost_reflect::DynamicMessage;

use super::MongoDriver;
use super::codec::{from_document, key_bson, to_document};
use super::driver::exec;
use super::error::{backend, invalid, write};
use crate::Result;
use crate::records::{
    Batcher, Resource, WriteResult, columns_to_message, core, key_of, message_to_columns,
};

/// How many keys go into one bulk read.
const BATCH_SIZE: usize = 512;

#[async_trait::async_trait]
impl Batcher for MongoDriver {
    /// Inserts every message in one ordered round trip, so it stops at the first failure. A
    /// taken key or unique value is `AlreadyExists`; the documents before it are written, as
    /// the server's own ordered insert leaves them.
    async fn create_many(
        &self,
        res: &Resource,
        msgs: &[DynamicMessage],
    ) -> Result<Vec<WriteResult>> {
        if msgs.is_empty() {
            return Ok(Vec::new());
        }
        let coll = self.collection(res)?;
        let (mut docs, mut out) = (Vec::with_capacity(msgs.len()), Vec::new());
        for (i, msg) in msgs.iter().enumerate() {
            let stopped = |e| invalid(format!("create_many stopped at index {i}: {e}"));
            let mut cols = message_to_columns(res, msg).map_err(stopped)?;
            core::fill_managed(res, &mut cols, true);
            docs.push(to_document(res, &cols).map_err(stopped)?);
            out.push(WriteResult::done(
                columns_to_message(res, &cols).map_err(stopped)?,
            ));
        }
        exec!(self, coll.insert_many(docs).ordered(true))
            .map_err(|e| write(format!("cannot insert into {}", res.table), e))?;
        Ok(out)
    }

    /// The records under `keys` in the order asked, `None` where one is missing, which is
    /// ordinary for a bulk read racing a delete. A `$in` returns documents in index order, so
    /// they are put back in place by key.
    async fn get_many(
        &self,
        res: &Resource,
        keys: &[String],
    ) -> Result<Vec<Option<DynamicMessage>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let coll = self.collection(res)?;
        let mut at: HashMap<&str, Vec<usize>> = HashMap::new();
        for (i, key) in keys.iter().enumerate() {
            at.entry(key.as_str()).or_default().push(i);
        }
        let mut out = vec![None; keys.len()];
        for chunk in keys.chunks(BATCH_SIZE) {
            let ids: Vec<Bson> = chunk.iter().map(|k| key_bson(res, k)).collect();
            let docs = self
                .find(&coll, doc! { "_id": { "$in": ids } }, None)
                .await
                .map_err(|e| backend(format!("cannot read {}", res.table), e))?;
            for doc in docs {
                let msg = from_document(res, doc)?;
                let key = key_of(res, &msg)?;
                for &i in at.get(key.as_str()).into_iter().flatten() {
                    out[i] = Some(msg.clone());
                }
            }
        }
        Ok(out)
    }
}
