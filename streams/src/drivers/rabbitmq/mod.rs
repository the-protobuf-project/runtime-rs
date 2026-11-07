//! Streams over RabbitMQ (AMQP 0-9-1), through `lapin`.
//!
//! The provider implements [`Publisher`](crate::Publisher),
//! [`Subscriber`](crate::Subscriber), [`Durable`](crate::Durable), and
//! [`Batch`](crate::Batch), which sends every payload before awaiting any
//! confirmation, but not
//! [`Positioned`](crate::Positioned). A durable queue really does hold a
//! consumer's messages while it is away, and RabbitMQ has a true negative
//! acknowledgement, so [`Delivery::nak`](crate::Delivery::nak) hands a message
//! straight back rather than waiting out a timeout. But a queue is not a log:
//! a message that has been acknowledged is gone, so there is no position but
//! now, and [`as_positioned`](crate::as_positioned) refuses by name.
//!
//! # How a stream maps onto AMQP
//!
//! A stream is a durable topic exchange and each subject is a routing key. A
//! durable consumer is a durable queue bound to the exchange for its subject
//! and named after the consumer, which is what makes the name, rather than the
//! process, the thing that outlives a restart. Two processes consuming under
//! one name take from one queue and so split the work; that is AMQP's
//! round-robin rather than anything this module arranges.
//!
//! [`Subscriber::subscribe`](crate::Subscriber::subscribe) binds an exclusive,
//! auto-deleting queue instead, on a channel of its own. Dropping the
//! subscription cancels the consumer and closes the channel, and the broker
//! deletes the queue, so nothing accumulates for a subscriber that has gone
//! away.
//!
//! # Names on the broker
//!
//! Exchange and queue names, the AMQP properties on every publish, and the
//! frame inside it are runtime-go's, so a Go and a Rust service configured with
//! the same [`RabbitConfig::prefix`] share streams and consumers:
//!
//! ```text
//! exchange  <prefix>.<stream id>                            (or <stream id>)
//! queue     <exchange>.<safe consumer>.<safe subject>
//! ```
//!
//! # Declarations are local
//!
//! The exchange behind a stream is real and survives a broker restart, but
//! AMQP has nowhere to hang a description or a user id, so stream metadata
//! lives in this process, as it does in runtime-go. Two processes share a
//! stream by creating it under the same id.
//!
//! # The connection is yours
//!
//! Unlike runtime-go, which dials a URL, this provider takes a
//! [`lapin::Connection`] you built, so TLS, credentials, heartbeats and
//! reconnection policy stay with the application. It opens channels on it,
//! one for publishing and declaring and one per subscription, because a
//! channel is AMQP's unit of prefetch and of consumer cancellation.
//! [`Streams::close`] closes only the publishing channel; the connection is
//! never closed here.
//!
//! # Publishes are confirmed
//!
//! The publishing channel is in confirm mode, as runtime-go's is, so
//! [`Publisher::publish`](crate::Publisher::publish) returns only once the
//! broker has taken responsibility for the message: for a persistent message
//! routed to a durable queue, that means written to disk. A negative confirm
//! is an error rather than a silent loss. When the broker closes the
//! publishing channel, for instance because a stream's exchange was deleted
//! elsewhere, the next publish opens a fresh one instead of failing forever.

mod manager;
mod topic;

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};

use lapin::{
    Channel, Connection, ExchangeKind,
    options::{ConfirmSelectOptions, ExchangeDeclareOptions, ExchangeDeleteOptions},
    types::{FieldTable, ShortString},
};

use crate::{Manager, Registry, Result, Stream, StreamError, Streams, provider::Metrics};

use manager::RabbitManager;
use topic::safe_name;

/// How many unacknowledged messages one consumer may hold when neither
/// [`RabbitConfig::prefetch`] nor [`Options::prefetch`](crate::Options::prefetch)
/// says otherwise; runtime-go's default.
///
/// One would serialize a consumer that does slow work; unbounded would let a
/// single consumer take the whole queue and starve its peers, which is the
/// failure that makes people think round-robin is broken.
pub const DEFAULT_PREFETCH: u16 = 32;

/// Settings for a [`RabbitProvider`].
///
/// It is deliberately not `Debug`: [`Metrics`] is not, and nothing here is
/// worth printing that a log line about the provider would not already say.
#[derive(Clone)]
pub struct RabbitConfig {
    /// Namespaces every exchange and queue this provider declares, so several
    /// independent sets can share a virtual host. Empty means no namespace.
    ///
    /// A Go peer must use the same prefix to see the same streams.
    pub prefix: String,

    /// How many unacknowledged messages one consumer may hold, sent to the
    /// broker as `basic.qos`. A per-call
    /// [`Options::prefetch`](crate::Options::prefetch) overrides it.
    ///
    /// This is what makes two consumers under one name share work evenly:
    /// RabbitMQ hands a consumer up to this many messages before waiting for an
    /// acknowledgement, so a low number spreads a burst and a high one lets a
    /// fast consumer run ahead.
    pub prefetch: u16,

    /// The codecs a subscriber accepts. A frame naming any other is dropped,
    /// or on a durable consumer rejected, rather than delivered as bytes nobody
    /// can read.
    pub registry: Registry,

    /// Delivery and settlement instruments for durable consumers.
    /// `Metrics::default()` records nothing.
    pub metrics: Metrics,
}

impl Default for RabbitConfig {
    fn default() -> Self {
        Self {
            prefix: String::new(),
            prefetch: DEFAULT_PREFETCH,
            registry: Registry::new(),
            metrics: Metrics::default(),
        }
    }
}

impl RabbitConfig {
    /// Namespaces every exchange and queue under `prefix`.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Lets each consumer hold `n` unacknowledged messages by default.
    pub fn with_prefetch(mut self, n: u16) -> Self {
        self.prefetch = n;
        self
    }

    /// Accepts frames written by the codecs in `registry`.
    pub fn with_registry(mut self, registry: Registry) -> Self {
        self.registry = registry;
        self
    }

    /// Records delivery metrics through `metrics`.
    pub fn with_metrics(mut self, metrics: Metrics) -> Self {
        self.metrics = metrics;
        self
    }
}

/// [`Streams`] over a caller-owned RabbitMQ connection.
///
/// Cheap to clone: clones share the publishing channel and the declarations.
#[derive(Clone)]
pub struct RabbitProvider {
    shared: Arc<Shared>,
}

/// What the provider and every manager bound from it share.
pub(crate) struct Shared {
    /// The caller's connection, which consumers open their channels on.
    conn: Arc<Connection>,
    /// One channel, in confirm mode, for publishing and declaring. A lapin
    /// channel is safe for concurrent use; the lock only guards replacing it
    /// after the broker closed it.
    channel: Mutex<Channel>,
    /// Set by [`Streams::close`], after which the channel is not reopened.
    closed: AtomicBool,
    config: RabbitConfig,
    /// Streams this process has declared, ordered by id so `list` is too.
    declared: RwLock<BTreeMap<String, Stream>>,
}

impl RabbitProvider {
    /// Builds a provider on `conn`, opening the channel it publishes and
    /// declares on and putting it in confirm mode.
    ///
    /// It fails rather than returning a provider when the channel cannot be
    /// opened, which is worth hearing at startup rather than at the first
    /// publish.
    pub async fn new(conn: Arc<Connection>, config: RabbitConfig) -> Result<Self> {
        let channel = open_publisher(&conn).await?;
        Ok(Self {
            shared: Arc::new(Shared {
                conn,
                channel: Mutex::new(channel),
                closed: AtomicBool::new(false),
                config,
                declared: RwLock::new(BTreeMap::new()),
            }),
        })
    }
}

/// Opens a channel in publisher-confirm mode.
async fn open_publisher(conn: &Connection) -> Result<Channel> {
    let channel = conn
        .create_channel()
        .await
        .map_err(|e| StreamError::internal("rabbitmq: cannot open a channel", e))?;
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .map_err(|e| StreamError::internal("rabbitmq: cannot enable publisher confirms", e))?;
    Ok(channel)
}

impl Shared {
    /// The publishing channel, reopened when the broker closed it while the
    /// connection stayed up.
    ///
    /// AMQP answers a channel-level error, such as publishing to an exchange
    /// that no longer exists, by closing the channel. Without reopening, one
    /// publish to a deleted stream would fail every later publish on every
    /// stream. A dead connection is left to fail: reconnecting is the
    /// caller's.
    pub(crate) async fn channel(&self) -> Result<Channel> {
        let current = self.current_channel();
        if current.status().connected()
            || !self.conn.status().connected()
            || self.closed.load(Ordering::Acquire)
        {
            return Ok(current);
        }
        let fresh = open_publisher(&self.conn).await?;
        let mut slot = self.channel.lock().expect("channel lock poisoned");
        if slot.status().connected() {
            // Another caller reopened it first; theirs wins.
            return Ok(slot.clone());
        }
        tracing::info!("rabbitmq: reopened the publishing channel");
        *slot = fresh.clone();
        Ok(fresh)
    }

    fn current_channel(&self) -> Channel {
        self.channel.lock().expect("channel lock poisoned").clone()
    }

    /// The topic exchange carrying one stream's subjects.
    pub(crate) fn exchange(&self, stream_id: &str) -> String {
        exchange_name(&self.config.prefix, stream_id)
    }

    fn get(&self, id: &str) -> Result<Stream> {
        self.declared
            .read()
            .expect("declarations lock poisoned")
            .get(id)
            .cloned()
            .ok_or_else(|| StreamError::NotFound(format!("stream {id}")))
    }
}

/// `<prefix>.<stream id>`, or the bare id without a prefix, as in runtime-go.
fn exchange_name(prefix: &str, stream_id: &str) -> String {
    if prefix.is_empty() {
        stream_id.to_owned()
    } else {
        format!("{prefix}.{stream_id}")
    }
}

/// The durable queue a named consumer of one subject reads from.
///
/// The subject is part of the name because a queue is bound to a routing key:
/// one queue per consumer per subject is what lets a consumer take one subject
/// without receiving, and having to acknowledge, the rest.
fn queue_name(exchange: &str, consumer: &str, subject: &str) -> String {
    format!("{exchange}.{}.{}", safe_name(consumer), safe_name(subject))
}

/// Converts a name to an AMQP short string, refusing one over 255 bytes rather
/// than letting lapin panic on it.
pub(crate) fn short(what: &str, value: impl Into<String>) -> Result<ShortString> {
    let value = value.into();
    let len = value.len();
    ShortString::try_new(value).map_err(|_| {
        StreamError::Unsupported(format!(
            "the {what} is {len} bytes, and AMQP limits names and routing keys to 255"
        ))
    })
}

/// A stream as stored: the caller's fields under `id`, and active.
fn stored(id: String, stream: Stream) -> Stream {
    Stream {
        id,
        active: true,
        ..stream
    }
}

#[async_trait::async_trait]
impl Streams for RabbitProvider {
    /// Declares a stream's exchange, generating an id when one is not supplied.
    ///
    /// The exchange is durable, so it outlives a broker restart: a stream that
    /// vanished when the broker bounced would take its consumers' bindings with
    /// it. Declaring is idempotent, so creating a stream another process
    /// already declared under the same id is how two processes share it.
    async fn create(&self, stream: Stream) -> Result<Stream> {
        let id = if stream.id.is_empty() {
            safe_name(&crate::provider::new_stream_id(&stream.name))
        } else {
            stream.id.clone()
        };
        let exchange = short("exchange name", self.shared.exchange(&id))?;
        self.shared
            .channel()
            .await?
            .exchange_declare(
                exchange,
                ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| {
                tracing::error!(id = %id, error = %e, "rabbitmq: could not declare the exchange");
                StreamError::internal(&format!("rabbitmq: cannot create stream {id}"), e)
            })?;

        let out = stored(id.clone(), stream);
        self.shared
            .declared
            .write()
            .expect("declarations lock poisoned")
            .insert(id.clone(), out.clone());
        tracing::info!(id = %id, name = %out.name, subjects = ?out.subjects, "rabbitmq: stream created");
        Ok(out)
    }

    async fn get(&self, id: &str) -> Result<Stream> {
        self.shared.get(id)
    }

    async fn bind(&self, id: &str) -> Result<Arc<dyn Manager>> {
        let stream = self.shared.get(id)?;
        Ok(Arc::new(RabbitManager::new(self.shared.clone(), stream)))
    }

    /// Replaces a stream's metadata, preserving its id. The exchange is
    /// unchanged; managers already bound keep the subjects they were bound with.
    async fn update(&self, id: &str, stream: Stream) -> Result<Stream> {
        let mut declared = self
            .shared
            .declared
            .write()
            .expect("declarations lock poisoned");
        let entry = declared
            .get_mut(id)
            .ok_or_else(|| StreamError::NotFound(format!("stream {id}")))?;
        *entry = stored(id.to_owned(), stream);
        tracing::info!(id = %id, "rabbitmq: stream updated");
        Ok(entry.clone())
    }

    /// Removes a stream and the exchange behind it.
    ///
    /// Queues bound to it are left alone: a durable consumer's queue holds
    /// messages it has not handled, and discarding those because the stream was
    /// redeclared elsewhere would lose work the consumer never saw.
    async fn delete(&self, id: &str) -> Result<()> {
        if self
            .shared
            .declared
            .write()
            .expect("declarations lock poisoned")
            .remove(id)
            .is_none()
        {
            return Err(StreamError::NotFound(format!("stream {id}")));
        }
        let exchange = short("exchange name", self.shared.exchange(id))?;
        self.shared
            .channel()
            .await?
            .exchange_delete(exchange, ExchangeDeleteOptions::default())
            .await
            .map_err(|e| {
                StreamError::internal(&format!("rabbitmq: cannot delete stream {id}"), e)
            })?;
        tracing::info!(id = %id, "rabbitmq: stream deleted");
        Ok(())
    }

    /// Every stream this process has declared, ordered by id.
    async fn list(&self) -> Result<Vec<Stream>> {
        Ok(self
            .shared
            .declared
            .read()
            .expect("declarations lock poisoned")
            .values()
            .cloned()
            .collect())
    }

    /// Closes the publishing channel. The connection is the caller's and stays
    /// open; subscriptions hold channels of their own and end when dropped.
    async fn close(&self) -> Result<()> {
        self.shared.closed.store(true, Ordering::Release);
        let channel = self.shared.current_channel();
        match channel.close(200, "OK".into()).await {
            Ok(()) => Ok(()),
            // Already shut, by an earlier close or by the connection going
            // away, is what closing wanted.
            Err(_) if !channel.status().connected() => Ok(()),
            Err(e) => Err(StreamError::internal(
                "rabbitmq: cannot close the channel",
                e,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_match_runtime_go() {
        assert_eq!(exchange_name("", "s-1"), "s-1");
        assert_eq!(exchange_name("app", "s-1"), "app.s-1");
        assert_eq!(
            queue_name("app.s-1", "billing.eu", "user.*"),
            "app.s-1.billing-eu.user--"
        );
        assert_eq!(
            queue_name("s-1", "workers", "job.queued"),
            "s-1.workers.job-queued"
        );
    }

    #[test]
    fn generated_ids_cannot_change_the_topology() {
        let id = safe_name(&crate::provider::new_stream_id("user events.v2"));
        assert!(id.ends_with(":user-events-v2"), "{id}");
        assert!(!id.contains(['.', '*', '#', ' ']), "{id}");
    }

    #[test]
    fn overlong_names_are_refused_not_panicked_on() {
        assert!(short("exchange name", "x".repeat(255)).is_ok());
        let error = short("exchange name", "x".repeat(256)).unwrap_err();
        assert!(matches!(error, StreamError::Unsupported(_)), "{error}");
    }

    #[test]
    fn stored_marks_active_and_keeps_fields() {
        let out = stored(
            "id-1".into(),
            Stream {
                id: "ignored".into(),
                name: "n".into(),
                subjects: vec!["a.b".into()],
                ..Default::default()
            },
        );
        assert_eq!(out.id, "id-1");
        assert!(out.active);
        assert_eq!(out.subjects, ["a.b"]);
    }
}
