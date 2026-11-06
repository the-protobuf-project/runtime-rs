//! [`Streams`](crate::Streams) over Redis, wire-compatible with runtime-go's
//! `streams/redis`.
//!
//! You bring the client:
//!
//! ```no_run
//! # fn example() -> redis::RedisResult<()> {
//! use streams::drivers::redis::{RedisConfig, RedisProvider};
//!
//! let client = redis::Client::open("redis://127.0.0.1:6379/2")?;
//! let config = RedisConfig { prefix: "orders".into(), ..Default::default() };
//!
//! let immediate = RedisProvider::new(client.clone(), config.clone()); // pub/sub
//! let scheduled = RedisProvider::scheduled(client.clone(), config.clone()); // TTL expiry
//! let durable = RedisProvider::durable(client, config); // Redis Streams
//! # Ok(()) }
//! ```
//!
//! A [`redis::Client`] is connection settings, not a connection: the provider
//! opens what it needs from it (one shared reconnecting connection for
//! commands, plus a dedicated one per subscription, because pub/sub and
//! blocking reads monopolise a connection) and never holds it anywhere but in
//! the provider.
//!
//! # Which constructor
//!
//! [`RedisProvider::new`] and [`RedisProvider::scheduled`] hand a message over
//! and forget it: a subscriber that is not attached when it fires never sees
//! it, and one that dies holding it loses it. [`RedisProvider::durable`]
//! appends to a log, keeps each named consumer's position on the server, and
//! keeps a delivered message pending until it is acknowledged, so it is the
//! only one whose manager answers [`as_durable`](crate::as_durable) and
//! [`as_positioned`](crate::as_positioned).
//!
//! They are separate constructors rather than one with a flag because the
//! difference is not a setting: a program that believes it has at-least-once
//! delivery and actually has at-most-once loses messages under exactly the
//! conditions it added the durability for. Each lives in its own key
//! namespace, so one never lists another's streams.
//!
//! # Scheduled delivery needs a server flag
//!
//! [`RedisProvider::scheduled`] rides on keyspace notifications, so the server
//! must run with `--notify-keyspace-events Ex`. Without it, scheduled messages
//! never fire.
//!
//! # Interoperating with Go
//!
//! Keys, fields, and channels are spelled as runtime-go spells them and every
//! payload is framed with [`provider::pack`](crate::provider::pack), so a Go
//! service and a Rust one under the same prefix share streams, subjects, and
//! consumer groups.

mod durable;
mod keys;
mod pubsub;
mod stream;

use std::{sync::Arc, time::Duration};

use redis::aio::ConnectionManager;
use tokio::sync::OnceCell;

use crate::{Registry, Result, StreamError, provider::Metrics};

/// The client crate this provider is built on, so callers name the same
/// version of [`redis::Client`] the constructors take.
pub use ::redis;
pub use durable::DurableManager;
pub use pubsub::PubSubManager;

use keys::{Keys, Kind};

/// How long a delivered but unacknowledged message sits with its consumer
/// before another may take it over, when [`RedisConfig::reclaim_after`] is not
/// changed.
///
/// It has to be longer than the slowest handler still making progress, or a
/// working consumer has its message stolen and the work is done twice. Thirty
/// seconds is generous for that and short enough that a process killed
/// mid-message does not strand it for long.
pub const DEFAULT_RECLAIM: Duration = Duration::from_secs(30);

/// Settings shared by every Redis provider constructor.
#[derive(Clone)]
pub struct RedisConfig {
    /// Namespaces every key these streams read and write, so several
    /// independent sets of streams can share one database. Empty adds nothing.
    pub prefix: String,

    /// The database whose expiry events a scheduled provider watches.
    ///
    /// Keyspace notifications are published per database as
    /// `__keyevent@<db>__:expired`, so this has to match the database the
    /// client selects. `None` reads it from the client's connection info,
    /// which is right unless something else selects the database.
    pub database: Option<i64>,

    /// Caps how many messages a durable subject retains, discarding the oldest
    /// beyond it. `None` retains everything.
    ///
    /// The cap is approximate, because Redis trims on node boundaries, which is
    /// what lets it trim without scanning: a stream capped at 1000 holds at
    /// least that many and a little more. A retained log is the point of a
    /// durable stream, and an unbounded one is a memory leak with a slow fuse;
    /// this is where that choice is made. Pub/sub and scheduled streams keep
    /// nothing to trim and ignore it.
    pub max_len: Option<usize>,

    /// How long a message may sit unacknowledged with one consumer before
    /// another may take it over. Zero disables reclaiming.
    ///
    /// This is what makes redelivery real rather than promised: a consumer that
    /// dies holding a message acknowledges nothing, and without a reclaim the
    /// message stays in the dead consumer's pending list forever.
    pub reclaim_after: Duration,

    /// The codecs a received frame may name. JSON is always accepted.
    pub registry: Registry,

    /// Delivery instruments for durable consumers; the default records nothing.
    pub metrics: Metrics,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            prefix: String::new(),
            database: None,
            max_len: None,
            reclaim_after: DEFAULT_RECLAIM,
            registry: Registry::new(),
            metrics: Metrics::default(),
        }
    }
}

/// Stream lifecycle over Redis, in one of three delivery modes chosen by
/// constructor.
///
/// It is cheap to clone; clones share the client and its connections.
#[derive(Clone)]
pub struct RedisProvider {
    inner: Arc<Inner>,
}

/// State every manager bound from one provider shares.
pub(super) struct Inner {
    /// The caller's client, from which dedicated connections are opened.
    client: redis::Client,
    /// The shared, reconnecting connection for ordinary commands. Opened on
    /// first use so constructing a provider does no I/O, as in Go.
    commands: OnceCell<ConnectionManager>,
    keys: Keys,
    kind: Kind,
    database: i64,
    max_len: Option<usize>,
    reclaim_after: Duration,
    registry: Registry,
    metrics: Metrics,
}

impl RedisProvider {
    /// Streams that deliver immediately, over pub/sub.
    ///
    /// The client is yours: this provider opens connections from it and never
    /// keeps it anywhere else.
    pub fn new(client: redis::Client, config: RedisConfig) -> Self {
        Self::build(client, config, Kind::Stream)
    }

    /// Streams that deliver a message when its TTL expires rather than when it
    /// is published: a reminder, a lease timeout, a delayed retry.
    ///
    /// A publish here requires [`Options::ttl`](crate::Options::ttl), where an
    /// immediate stream refuses one. The server must run with
    /// `--notify-keyspace-events Ex`.
    pub fn scheduled(client: redis::Client, config: RedisConfig) -> Self {
        Self::build(client, config, Kind::Notify)
    }

    /// Streams backed by Redis Streams, which remember what each named
    /// consumer has handled and redeliver what it has not.
    ///
    /// This is the constructor to reach for when losing a message matters; its
    /// managers answer [`as_durable`](crate::as_durable) and
    /// [`as_positioned`](crate::as_positioned).
    pub fn durable(client: redis::Client, config: RedisConfig) -> Self {
        Self::build(client, config, Kind::Durable)
    }

    fn build(client: redis::Client, config: RedisConfig, kind: Kind) -> Self {
        let database = config
            .database
            .unwrap_or_else(|| client.get_connection_info().redis_settings().db());
        Self {
            inner: Arc::new(Inner {
                client,
                commands: OnceCell::new(),
                keys: Keys::new(&config.prefix, kind),
                kind,
                database,
                max_len: config.max_len,
                reclaim_after: config.reclaim_after,
                registry: config.registry,
                metrics: config.metrics,
            }),
        }
    }
}

impl Inner {
    /// The shared command connection, opened on first use.
    ///
    /// A clone of a [`ConnectionManager`] addresses the same multiplexed
    /// connection, so handing one out per call costs nothing and lets commands
    /// run concurrently.
    async fn conn(&self) -> Result<ConnectionManager> {
        self.commands
            .get_or_try_init(|| self.client.get_connection_manager())
            .await
            .cloned()
            .map_err(|error| StreamError::internal("redis: cannot connect", error))
    }

    /// A connection of its own for a blocking read.
    ///
    /// Redis answers one connection's commands in order, so a blocking
    /// XREADGROUP on the shared connection would stall every ack and publish
    /// behind it. The response timeout sits above the block, which the
    /// client's default of half a second does not.
    async fn blocking_conn(&self) -> Result<redis::aio::MultiplexedConnection> {
        let config = redis::AsyncConnectionConfig::new()
            .set_response_timeout(Some(durable::BLOCK_FOR + Duration::from_secs(5)));
        self.client
            .get_multiplexed_async_connection_with_config(&config)
            .await
            .map_err(|error| StreamError::internal("redis: cannot connect", error))
    }
}

/// Wraps a Redis failure with what was being attempted.
fn fail(context: impl AsRef<str>) -> impl FnOnce(redis::RedisError) -> StreamError {
    move |error| StreamError::internal(context.as_ref(), error)
}
