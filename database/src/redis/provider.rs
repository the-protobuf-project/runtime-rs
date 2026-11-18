//! Selecting a database from a Redis connection you own. Go's `database/redis/provider.go`.

use std::sync::Arc;

use redis::aio::ConnectionManager;

use super::codec::failed;
use super::{BACKEND, RedisDriver};
use crate::Result;
use crate::records::{Capabilities, DB, Provider, check_database_name};

/// A Redis records backend bound to a connection you own. Go's `redis.Provider`.
///
/// The connection is yours: this neither dials nor closes it, and no [`DB`] it returns does.
/// Hand it the one the cache uses (`cache::drivers::redis::RedisClient::connection_manager`)
/// and both share one multiplexed connection, as Go's cache, database and streams layers
/// share one client.
#[derive(Clone)]
pub struct RedisProvider {
    conn: ConnectionManager,
    prefix: String,
}

impl RedisProvider {
    /// A provider over `conn`. Go's `NewProvider`.
    pub fn new(conn: ConnectionManager) -> Self {
        Self {
            conn,
            prefix: String::new(),
        }
    }

    /// Namespaces every key under `prefix`, to share one Redis with another program or with
    /// a cache; the database a [`Provider::set_database`] selects nests inside it. Go's
    /// `WithPrefix`.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }
}

#[async_trait::async_trait]
impl Provider for RedisProvider {
    /// The records of database `name`: a key segment, not a numbered Redis database. Those
    /// are a property of the connection, so one would need a second connection, Redis ships
    /// sixteen, and a cluster has only 0; a segment has no ceiling and shares the connection.
    /// What it does not give is server-side isolation: a `FLUSHDB` reaches every name.
    ///
    /// Pings first, so an unreachable server or a refused AUTH fails here rather than three
    /// layers into a request. The [`DB`] has CRUD, schema and batches; transactions refuse
    /// naming `redis`. Go's `SetDatabase`.
    async fn set_database(&self, name: &str) -> Result<DB> {
        check_database_name(name)?;
        let _: String = redis::cmd("PING")
            .query_async(&mut self.conn.clone())
            .await
            .map_err(|e| failed(format!("database {name:?} is not reachable"), e))?;
        let driver = Arc::new(RedisDriver::at(self.conn.clone(), &self.prefix, name));
        let caps = Capabilities {
            schema: Some(driver.clone()),
            batch: Some(driver.clone()),
            ..Capabilities::new(driver)
        };
        Ok(crate::records::observe::instrument_default(DB::build(
            BACKEND, name, caps, None,
        )))
    }

    /// `redis`.
    fn backend(&self) -> &'static str {
        BACKEND
    }
}
