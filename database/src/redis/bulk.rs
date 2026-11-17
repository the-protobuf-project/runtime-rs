//! Bulk reads and writes. Go's `database/redis/bulk.go`.

use prost_reflect::DynamicMessage;

use super::RedisDriver;
use super::codec::{decode, failed};
use crate::Result;
use crate::records::{Batcher, Resource, WriteResult};

/// Keys per `MGET`: large enough that the per-command overhead disappears, small enough that
/// one reply does not hold a megabyte hostage. Go's `batchSize`.
const BATCH_SIZE: usize = 256;

impl RedisDriver {
    /// The records under `keys`, in order, `None` for a miss, in one round trip: one `MGET`
    /// per [`BATCH_SIZE`] keys, all in one pipeline. Go pipelines single `GET`s, one round
    /// trip per batch, because `MGET` needs one hash slot on a cluster; this driver runs on a
    /// connection manager, which is never a cluster, so it takes the cheaper command.
    pub(super) async fn fetch(
        &self,
        res: &Resource,
        keys: &[String],
    ) -> Result<Vec<Option<DynamicMessage>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipe = redis::pipe();
        for batch in keys.chunks(BATCH_SIZE) {
            let cmd = pipe.cmd("MGET");
            for key in batch {
                cmd.arg(self.keys.record(res, key));
            }
        }
        let bodies: Vec<Vec<Option<Vec<u8>>>> = pipe
            .query_async(&mut self.conn())
            .await
            .map_err(|e| failed(format!("cannot read {}", res.name), e))?;
        bodies
            .into_iter()
            .flatten()
            .map(|body| body.map(|b| decode(res, &b)).transpose())
            .collect()
    }
}

#[async_trait::async_trait]
impl Batcher for RedisDriver {
    /// Creates each message in turn, stopping at the first failure, which is returned as
    /// itself. A loop, not a pipeline, as in Go: each create is atomic, but the batch is not,
    /// and a pipeline would write the messages after a conflicting one. Go also returns the
    /// results before the failure; this contract returns the error alone.
    async fn create_many(
        &self,
        res: &Resource,
        msgs: &[DynamicMessage],
    ) -> Result<Vec<WriteResult>> {
        let mut out = Vec::with_capacity(msgs.len());
        for msg in msgs {
            out.push(self.create_one(res, msg).await?);
        }
        Ok(out)
    }

    /// The records under `keys`, in order, with `None` where one is missing: a listing racing
    /// a delete is ordinary, and the caller decides what a gap means. Go's `GetMany`.
    async fn get_many(
        &self,
        res: &Resource,
        keys: &[String],
    ) -> Result<Vec<Option<DynamicMessage>>> {
        self.fetch(res, keys).await
    }
}
