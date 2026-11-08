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
//! # Surviving a server restart
//!
//! The shared connection speaks RESP3 whatever the client asks for, because
//! that is how it hears the server hang up: the disconnect arrives as an
//! event, the connection is replaced in the background, and the first command
//! after a restart goes out on the new one instead of failing on the dead one.
//! A server too old for RESP3 (before 6.0) gets the client's own protocol and
//! loses only that early warning. Credentials, TLS, and the database come from
//! the client unchanged, so an ACL user or `rediss://user:pass@host` works as
//! it does anywhere else.
//!
//! A command that still meets a dropped connection is repeated once on the new
//! one only when repeating it cannot do its work twice: reads, acks, group
//! changes, deletes, and the metadata writes. A PUBLISH, an XADD of a message,
//! or a scheduled message is never sent twice, because a connection that drops
//! after the server ran the command and before its reply arrived looks exactly
//! like one that dropped before sending it, and a repeat would deliver the
//! message twice. Those are repeated only when the connection was refused
//! outright, which proves nothing was sent; otherwise the caller decides, with
//! [`with_retry`](crate::with_retry) where a duplicate is acceptable.
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

use redis::{
    ProtocolVersion, RedisError, RedisResult,
    aio::{ConnectionManager, ConnectionManagerConfig},
};
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

    /// Whether a durable [`Delivery::ack`](crate::Delivery::ack) waits for
    /// Redis to answer the XACK before returning. On by default.
    ///
    /// Waiting is what lets an ack report a failure, but it costs a round trip
    /// per message to a consumer that acknowledges one message before reading
    /// the next, which caps it at one message per round trip. Off, an ack is
    /// queued and returns at once; queued acks go out together as one
    /// multi-id XACK, and a failure is logged rather than returned.
    ///
    /// Turning it off does not weaken at-least-once: an entry stays pending
    /// until its XACK lands, so one whose XACK is lost (a dropped connection,
    /// a process killed with acks still queued) is reclaimed and delivered
    /// again, exactly as if the handler had died before acknowledging. What is
    /// given up is hearing about it, and an ack that returned may still be
    /// followed by a redelivery, which handlers built for at-least-once already
    /// tolerate. NATS JetStream's plain ack makes the same trade.
    pub confirm_acks: bool,

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
            confirm_acks: true,
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
    confirm_acks: bool,
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
                confirm_acks: config.confirm_acks,
                registry: config.registry,
                metrics: config.metrics,
            }),
        }
    }
}

/// How many times the shared connection retries reaching the server after it
/// went away, and the longest wait between tries: about ten seconds in all,
/// enough to ride out a restart rather than giving up halfway through one.
const RECONNECT_ATTEMPTS: usize = 12;
const RECONNECT_WAIT_UP_TO: Duration = Duration::from_secs(1);

/// How long a command on the shared connection may wait for its reply.
///
/// The client's default of half a second is shorter than a loaded server
/// takes: one with `appendfsync always` answers a burst of XADDs only as fast
/// as its disk syncs, and every reply queued behind a slow fsync times out
/// together. A timed-out publish is the worst failure there is, because it may
/// have landed and is never repeated (see [`Replay::Once`]), so the caller
/// cannot tell a loss from a success. A dead server is noticed by the
/// connection dropping, not by this, so a generous bound costs nothing there.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(5);

/// Whether a command may be sent a second time after the connection dropped
/// under it. See "Surviving a server restart" in the module docs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Replay {
    /// Running it twice leaves the server as running it once does.
    Idempotent,
    /// Running it twice does its work twice: a PUBLISH delivers twice, an
    /// XADD appends a second entry. Repeated only when provably never sent.
    Once,
}

impl Replay {
    /// Whether `error` permits the one repeat.
    fn again(self, error: &RedisError) -> bool {
        // A refused connection carried nothing, so even a publish may go again.
        if error.is_connection_refusal() {
            return true;
        }
        self == Self::Idempotent && error.is_connection_dropped()
    }
}

impl Inner {
    /// The shared command connection, opened on first use.
    ///
    /// A clone of a [`ConnectionManager`] addresses the same multiplexed
    /// connection, so handing one out per call costs nothing and lets commands
    /// run concurrently.
    async fn manager(&self) -> RedisResult<ConnectionManager> {
        self.commands
            .get_or_try_init(|| async {
                let config = ConnectionManagerConfig::new()
                    .set_number_of_retries(RECONNECT_ATTEMPTS)
                    .set_max_delay(RECONNECT_WAIT_UP_TO)
                    .set_response_timeout(Some(COMMAND_TIMEOUT));
                let info = self.client.get_connection_info();
                if info.redis_settings().protocol().supports_resp3() {
                    return self.client.get_connection_manager_with_config(config).await;
                }
                let resp3 = redis::Client::open(
                    info.clone().set_redis_settings(
                        info.redis_settings()
                            .clone()
                            .set_protocol(ProtocolVersion::RESP3),
                    ),
                )?;
                // One plain attempt first, because the manager retries every
                // failure for seconds, a refused HELLO included.
                let client = match resp3.get_multiplexed_async_connection().await {
                    Err(error) if error.kind() == redis::ErrorKind::RESP3NotSupported => {
                        tracing::info!("redis: server predates RESP3; using the client's protocol");
                        self.client.clone()
                    }
                    _ => resp3,
                };
                client.get_connection_manager_with_config(config).await
            })
            .await
            .cloned()
    }

    /// Runs one command on the shared connection, repeating it once on the
    /// replacement connection when the first one had dropped and `replay`
    /// allows it.
    pub(super) async fn query<T: redis::FromRedisValue>(
        &self,
        cmd: &redis::Cmd,
        replay: Replay,
    ) -> RedisResult<T> {
        let mut conn = self.manager().await?;
        match cmd.query_async(&mut conn).await {
            Err(error) if replay.again(&error) => {
                tracing::debug!(%error, "redis: connection dropped; sending once more");
                cmd.query_async(&mut conn).await
            }
            done => done,
        }
    }

    /// [`Inner::query`] for a pipeline or transaction, which is repeated
    /// whole or not at all.
    pub(super) async fn query_pipe<T: redis::FromRedisValue>(
        &self,
        pipe: &redis::Pipeline,
        replay: Replay,
    ) -> RedisResult<T> {
        let mut conn = self.manager().await?;
        match pipe.query_async(&mut conn).await {
            Err(error) if replay.again(&error) => {
                tracing::debug!(%error, "redis: connection dropped; sending once more");
                pipe.query_async(&mut conn).await
            }
            done => done,
        }
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

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    fn io(kind: io::ErrorKind) -> RedisError {
        RedisError::from(io::Error::from(kind))
    }

    #[test]
    fn a_dropped_connection_repeats_only_what_is_idempotent() {
        for kind in [
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::ConnectionReset,
            io::ErrorKind::UnexpectedEof,
        ] {
            assert!(Replay::Idempotent.again(&io(kind)), "{kind:?}");
            assert!(!Replay::Once.again(&io(kind)), "{kind:?}");
        }
    }

    #[test]
    fn a_refused_connection_repeats_anything() {
        let refused = io(io::ErrorKind::ConnectionRefused);
        assert!(Replay::Idempotent.again(&refused));
        assert!(Replay::Once.again(&refused));
    }

    #[test]
    fn nothing_repeats_a_timeout_or_an_answer() {
        let answers = [
            io(io::ErrorKind::TimedOut),
            RedisError::from((redis::ErrorKind::AuthenticationFailed, "WRONGPASS")),
            RedisError::from((redis::ErrorKind::Client, "NOPERM")),
        ];
        for error in answers {
            assert!(!Replay::Idempotent.again(&error), "{error}");
            assert!(!Replay::Once.again(&error), "{error}");
        }
    }
}
