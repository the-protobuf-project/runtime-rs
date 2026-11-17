//! Schema over a store that has none. Go's `database/redis/schema.go`.

use super::RedisDriver;
use super::codec::failed;
use crate::Result;
use crate::records::{Migrator, Resource};

/// The `SCAN` count hint: large enough that round trips are not the cost, small enough that
/// one reply is not. Go's `scanBatch`.
const SCAN_BATCH: usize = 256;

#[async_trait::async_trait]
impl Migrator for RedisDriver {
    /// Succeeds without doing anything: a key holds whatever it is given, so a resource is
    /// ready the moment something stores it, and a program that migrates on startup runs
    /// unchanged here and against SQL. Go's `EnsureSchema`.
    async fn ensure_schema(&self, _: &Resource) -> Result<()> {
        Ok(())
    }

    /// Removes every key of the resource: records, index and reservations. A cursor walk of
    /// the whole keyspace, since a namespace is a naming convention Redis keeps no index of,
    /// and not atomic: a write landing mid-walk may survive. Fine for a teardown. Go's
    /// `DropSchema`.
    async fn drop_schema(&self, res: &Resource) -> Result<()> {
        let pattern = self.keys.pattern(res);
        let walk = |e| failed(format!("cannot walk the keyspace of {}", res.name), e);
        let mut cursor = 0_u64;
        loop {
            let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(SCAN_BATCH)
                .query_async(&mut self.conn())
                .await
                .map_err(walk)?;
            if !batch.is_empty() {
                let _: i64 = redis::cmd("DEL")
                    .arg(&batch)
                    .query_async(&mut self.conn())
                    .await
                    .map_err(|e| failed(format!("cannot drop {}", res.name), e))?;
            }
            if next == 0 {
                return Ok(());
            }
            cursor = next;
        }
    }

    /// True for any resource: there is nothing that could be absent, so a caller deciding
    /// whether to migrate correctly decides not to. Go's `HasSchema`.
    async fn has_schema(&self, _: &Resource) -> Result<bool> {
        Ok(true)
    }
}
