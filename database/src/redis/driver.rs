//! The Redis [`Driver`]: one key per record, its proto wire bytes, and a sorted set of keys
//! beside them. Go's `database/redis/driver.go`.

use prost_reflect::DynamicMessage;
use redis::aio::ConnectionManager;

use super::codec::{decode, failed, refuse_filter};
use super::keys::Keys;
use crate::Result;
use crate::records::{Driver, ListOptions, ListResult, Resource, WriteResult};

/// A records [`Driver`] over Redis, which is also its [`Migrator`](crate::records::Migrator)
/// and [`Batcher`](crate::records::Batcher). Go's `redis.Driver`.
///
/// Cloning is cheap and shares the connection. The connection is the caller's: the driver
/// neither dials nor closes it.
#[derive(Clone)]
pub struct RedisDriver {
    pub(super) conn: ConnectionManager,
    pub(super) keys: Keys,
}

impl RedisDriver {
    /// A driver over a connection you own, with no prefix and no database segment. Go's
    /// `redis.New`; use [`super::RedisProvider`] to select a database at runtime.
    pub fn new(conn: ConnectionManager) -> Self {
        Self::at(conn, "", "")
    }

    /// Namespaces every key under `prefix`, to share one Redis with another program or with
    /// a cache. Separate from the database a provider selects: the prefix separates programs,
    /// the database tenants. Go's `WithPrefix`.
    pub fn with_prefix(self, prefix: &str) -> Self {
        Self::at(self.conn, prefix, "")
    }

    pub(super) fn at(conn: ConnectionManager, prefix: &str, database: &str) -> Self {
        Self {
            conn,
            keys: Keys::new(prefix, database),
        }
    }

    /// A handle on the shared connection for one command: a clone shares the multiplexed
    /// connection, it does not open one.
    pub(super) fn conn(&self) -> ConnectionManager {
        self.conn.clone()
    }

    /// The stored bytes under `key`, or `None`.
    pub(super) async fn read(&self, res: &Resource, key: &str) -> Result<Option<Vec<u8>>> {
        redis::cmd("GET")
            .arg(self.keys.record(res, key))
            .query_async(&mut self.conn())
            .await
            .map_err(|e| failed(format!("cannot read {key}"), e))
    }
}

#[async_trait::async_trait]
impl Driver for RedisDriver {
    /// Stores `msg` in one script: the record, its index entry and its unique reservations
    /// together, so `AlreadyExists` is decided by the server and a refusal leaves nothing.
    async fn create(&self, res: &Resource, msg: &DynamicMessage) -> Result<WriteResult> {
        self.create_one(res, msg).await
    }

    /// The record under `key`, or `NotFound`.
    async fn get(&self, res: &Resource, key: &str) -> Result<DynamicMessage> {
        match self.read(res, key).await? {
            Some(body) => decode(res, &body),
            None => Err(crate::Error::NotFound),
        }
    }

    /// Overwrites the record `msg` names, moving its unique reservations in the same script:
    /// a changed value releases the old reservation, so it is not held forever.
    async fn update(&self, res: &Resource, msg: &DynamicMessage) -> Result<WriteResult> {
        self.update_one(res, msg).await
    }

    /// Removes the record and its reservations. A missing record is `NotFound`, unlike a cache:
    /// records do not expire, so deleting one that is not there means the caller is wrong.
    async fn delete(&self, res: &Resource, key: &str) -> Result<()> {
        self.delete_one(res, key).await
    }

    /// A page in primary-key order, either direction, cut by the server from the sorted key
    /// set: total and page in one pipeline, or one key past the page with `omit_total`. Any
    /// other `order_by` column orders by the key; a filter is refused.
    async fn list(&self, res: &Resource, opts: &ListOptions) -> Result<ListResult> {
        self.list_page(res, opts).await
    }

    /// How many records the resource holds, in O(1). A filter is refused.
    async fn count(&self, res: &Resource, opts: &ListOptions) -> Result<i64> {
        refuse_filter(&opts.filter)?;
        redis::cmd("ZCARD")
            .arg(self.keys.ids(res))
            .query_async(&mut self.conn())
            .await
            .map_err(|e| failed(format!("cannot count {}", res.name), e))
    }

    /// Whether a record is under `key`.
    async fn exists(&self, res: &Resource, key: &str) -> Result<bool> {
        redis::cmd("EXISTS")
            .arg(self.keys.record(res, key))
            .query_async(&mut self.conn())
            .await
            .map_err(|e| failed(format!("cannot check {key}"), e))
    }
}
