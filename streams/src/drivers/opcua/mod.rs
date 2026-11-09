//! Streams over OPC UA client/server, through `async-opcua`.
//!
//! OPC UA is how PLCs, SCADA systems, and building controllers expose their
//! state: a server holds an address space of nodes, and a client reads,
//! writes, and monitors their values. This provider maps the stream contract
//! onto that, and refuses the parts that have no OPC UA meaning instead of
//! inventing one.
//!
//! The provider implements [`Publisher`](crate::Publisher),
//! [`Subscriber`](crate::Subscriber), and [`Batch`](crate::Batch) as one Write
//! per value, in order. [`as_durable`](crate::as_durable) and
//! [`as_positioned`](crate::as_positioned) refuse: a node holds its current
//! value, not a queue or a log.
//!
//! # The mapping
//!
//! | Contract | OPC UA |
//! | --- | --- |
//! | subject | a node id in string form, `ns=3;s=Temperature` or `nsu=urn:plant;i=1001` |
//! | [`publish`](crate::Publisher::publish) | a Write to the node's Value attribute |
//! | [`subscribe`](crate::Subscriber::subscribe) | a data-change monitored item in the provider's client subscription |
//! | [`publish_batch`](crate::Batch::publish_batch) | one Write per value, each awaited before the next |
//! | payload | a [`DataValue`] in OPC UA binary encoding: [`OpcUaValue`] |
//! | stream | a declaration this provider holds: OPC UA has nowhere to keep one |
//!
//! **Subjects are node ids**, declared when the stream is created; anything
//! else fails with [`StreamError::UnknownSubject`], and a declaration that is
//! not a node id fails `create`. A namespace index (`ns=3`) is a position in
//! the server's namespace table and can change when the server's
//! configuration does; a namespace URI (`nsu=...`) is stable, resolved
//! against the server when the stream is bound, and again on each new server
//! session, so a server that renumbers its namespaces when it restarts is
//! still written to, and monitored on, the node the URI names. Matching is
//! literal: OPC UA has no wildcards over node ids.
//!
//! **A batch is a Write per value**, each answered before the next is sent.
//! A batch's values are all for one node, and a server applies the values of
//! one Write request in whatever order it chooses (Part 4, 5.11.4.1), so a
//! single request could leave the node holding any of them rather than the
//! last.
//!
//! **Publish** writes the payload's DataValue, status and timestamps
//! included, and returns once the server has answered the Write. A bad status
//! is an error naming it: `BadTypeMismatch` (the node's data type is not the
//! value's) is [`StreamError::Codec`]; `BadNodeIdUnknown`, `BadNotWritable`,
//! `BadUserAccessDenied`, and `BadWriteNotSupported` (many servers refuse
//! writes that carry a status or timestamps) are [`StreamError::Unsupported`],
//! since retrying cannot change them; anything else is
//! [`StreamError::Internal`]. **OPC UA has no message id**: the id returned is
//! [`Options::id`](crate::Options::id) or a new ULID, and it does not travel.
//! A subscriber's messages carry ids of their own, minted on receipt.
//!
//! **Subscribe** adds a monitored item on the node's Value, reporting on
//! status, value, or timestamp change, so a write that repeats the last value
//! is still a message. [`Options::prefetch`](crate::Options::prefetch) is the
//! item's queue size on the server, which is what OPC UA holds for a client
//! between publishing cycles; [`OpcUaConfig::queue_size`] is used without it.
//! The server reports an item's current value as soon as it is created; that
//! value was not published after the subscription, so it is skipped unless
//! [`OpcUaConfig::initial_value`] says otherwise. To know it, subscribe reads
//! the node first, and skips the first notification only when it is the value
//! read: a write that displaced it from a full queue is delivered. Each
//! notification becomes a
//! [`Message`](crate::Message) on the node id's subject, codec
//! [`CODEC_NAME`], carrying the DataValue with its status code and both
//! timestamps. Dropping the stream deletes the monitored item, and the
//! subscription with the last one.
//!
//! A subscription is a sampled view of a value, not a queue of writes. A
//! server that samples a node more slowly than it is written, or whose
//! queue for the item overflows before the next publishing cycle, reports
//! fewer values than were written; that is OPC UA doing what it promises.
//!
//! # Refused
//!
//! - [`Options::group`](crate::Options::group): OPC UA has no consumer
//!   groups; every monitored item receives every change.
//! - [`Options::ttl`](crate::Options::ttl): a Write applies now; OPC UA does
//!   not schedule one.
//! - [`Durable`](crate::Durable): a notification is acknowledged by the
//!   subscription's publish cycle, not by the reader, and a value nobody read
//!   is replaced by the next one rather than redelivered.
//! - Headers, on [`Stream::headers`] at `create` and `update` and on
//!   [`Options::headers`](crate::Options::headers) at publish: a Write sets a
//!   node's value, status and timestamps, and has nowhere to carry anything
//!   else, so headers would be dropped without a word. Use
//!   [`pubsub`], which carries them as MQTT 5 user properties.
//! - [`Options::expires`](crate::Options::expires): a Write applies at once
//!   or fails; nothing waits that could expire.
//! - [`Options::retain`](crate::Options::retain): a node always holds its
//!   last value, and a new monitored item reports it when
//!   [`OpcUaConfig::initial_value`] asks; there is no retain flag to set.
//! - [`Positioned`](crate::Positioned): there is no log to seek. HistoryRead
//!   reads a historian's archive, where a server has one, by time range; it
//!   is a query, not a position in a stream, and is left to the session.
//!
//! [`Options::partition_key`](crate::Options::partition_key) is ignored, as
//! on every provider without partitions.
//!
//! # The session is yours
//!
//! The provider takes a connected [`Session`] the caller built, with its
//! endpoint, security policy, certificates, and user identity, and whose
//! [`SessionEventLoop`](opcua::client::SessionEventLoop) the caller polls.
//! Nothing here connects or disconnects it. [`Streams::close`] deletes the
//! subscription this provider created and ends its streams.
//!
//! When the connection drops, the session's event loop reconnects, and on the
//! new session async-opcua transfers the subscription or, when the server has
//! lost it (a restart), recreates it with its monitored items. The streams
//! carry on across that; a write made while the session is down fails rather
//! than waiting. After a recreate the server reports each item's current
//! value, and that one is delivered: it is what was written while the
//! session was gone.
//!
//! # Two async-opcua 0.19 behaviours worked around
//!
//! - After a reconnect that recreates the subscription, the session's
//!   publishing loop waits for a trigger that has already fired, so the
//!   subscription is silent though the server holds it. The provider hears
//!   every message the subscription sends, keep-alives included, and calls
//!   [`Session::trigger_publish_now`] when it has been silent past twice its
//!   keep-alive period.
//! - A Publish response from the OPC Foundation's .NET stack holding more
//!   than about 1,800 notifications drops the connection as
//!   `BadDecodingError`, so [`OpcUaConfig::max_notifications_per_publish`]
//!   bounds it at 1000 by default.
//!
//! # The payload is OPC UA's
//!
//! There is no runtime-go frame here: a node's value is what PLCs and SCADA
//! read, so it must be a native typed value. [`OpcUaValue`] is the codec; its
//! bytes are a DataValue's OPC UA binary encoding and nothing else, which is
//! also the DataValue field encoding OPC UA PubSub uses.
//!
//! # PubSub
//!
//! [`pubsub`] is the other half of OPC UA: Part 14 PubSub over an MQTT
//! broker, where DataSets travel as JSON NetworkMessages and are delivered
//! as the same [`OpcUaValue`] messages this provider's subscriptions give.

mod codec;
mod hub;
mod manager;
mod node;
pub mod pubsub;

use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
    time::Duration,
};

pub use opcua::client::Session;
pub use opcua::types::{DataValue, StatusCode, Variant};

use crate::{Manager, Result, Stream, StreamError, Streams, provider};

pub use codec::{CODEC_NAME, OpcUaValue};

use hub::Hub;
use manager::OpcUaManager;

/// How often the server sends a subscription's notifications when
/// [`OpcUaConfig::publishing_interval`] is not changed.
///
/// A tenth of a second keeps a notification's delay short without a publish
/// cycle per write; servers revise it up to their own minimum.
pub const DEFAULT_PUBLISHING_INTERVAL: Duration = Duration::from_millis(100);

/// Settings for an [`OpcUaProvider`].
#[derive(Clone, Debug)]
pub struct OpcUaConfig {
    /// How often the server sends the subscription's queued notifications.
    pub publishing_interval: Duration,

    /// How often the server samples a monitored node. Zero asks for the
    /// fastest it offers; a server reporting writes directly, as async-opcua's
    /// does, is not limited by it.
    pub sampling_interval: Duration,

    /// A monitored item's queue size on the server when the subscribe gives no
    /// [`Options::prefetch`](crate::Options::prefetch): how many changes the
    /// server holds for this client between publishing cycles, discarding the
    /// oldest past it.
    pub queue_size: u32,

    /// How many notifications a stream holds ahead of its reader before
    /// dropping the newest, so a stalled reader cannot stall the session.
    pub buffer: usize,

    /// Publishing intervals without a notification before the server sends
    /// a keep-alive.
    pub keep_alive_count: u32,

    /// Publishing intervals without a client publish request before the
    /// server deletes the subscription. At least three keep-alives.
    pub lifetime_count: u32,

    /// Whether a new stream delivers the node's current value first, as OPC
    /// UA reports it. Off by default: a stream delivers what is published
    /// after it subscribes.
    pub initial_value: bool,

    /// The most notifications the server puts in one Publish response; the
    /// rest wait for the next one, which the client asks for at once. Zero
    /// lets the server choose, which means no limit.
    ///
    /// Bounded by default because async-opcua 0.19 drops the connection, as
    /// `BadDecodingError`, on a Publish response from the OPC Foundation's
    /// .NET stack once it holds about 1,800 notifications (roughly one
    /// 64 KiB chunk): the subscription then stalls behind a reconnect. A
    /// thousand keeps a response well inside one chunk.
    pub max_notifications_per_publish: u32,
}

impl Default for OpcUaConfig {
    /// No initial value, and at most 1,000 notifications per Publish response.
    fn default() -> Self {
        Self {
            publishing_interval: DEFAULT_PUBLISHING_INTERVAL,
            sampling_interval: Duration::ZERO,
            queue_size: provider::DEFAULT_PREFETCH as u32,
            buffer: 4096,
            keep_alive_count: 10,
            lifetime_count: 60,
            initial_value: false,
            max_notifications_per_publish: 1000,
        }
    }
}

/// Streams over an OPC UA server, through a session the caller owns.
///
/// Cloning is cheap and shares the session, the declarations, and the client
/// subscription.
#[derive(Clone)]
pub struct OpcUaProvider {
    inner: Arc<Inner>,
}

/// What every bound manager shares with the provider.
pub(crate) struct Inner {
    session: Arc<Session>,
    config: OpcUaConfig,
    hub: Arc<Hub>,
    declared: RwLock<BTreeMap<String, Stream>>,
}

impl OpcUaProvider {
    /// A provider over `session`, which the caller connected and whose event
    /// loop the caller polls.
    pub fn new(session: Arc<Session>, config: OpcUaConfig) -> Self {
        let hub = Hub::new(session.clone(), config.clone());
        Self {
            inner: Arc::new(Inner {
                session,
                config,
                hub,
                declared: RwLock::new(BTreeMap::new()),
            }),
        }
    }

    /// The session the provider writes and monitors through.
    pub fn session(&self) -> &Arc<Session> {
        &self.inner.session
    }

    /// How many monitored items this provider's streams hold on the server.
    pub fn monitored_items(&self) -> usize {
        self.inner.hub.items()
    }

    /// Reads the declared streams, recovering from a poisoned lock.
    fn read(&self) -> std::sync::RwLockReadGuard<'_, BTreeMap<String, Stream>> {
        self.inner
            .declared
            .read()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// Writes the declared streams, recovering from a poisoned lock.
    fn write(&self) -> std::sync::RwLockWriteGuard<'_, BTreeMap<String, Stream>> {
        self.inner
            .declared
            .write()
            .unwrap_or_else(|e| e.into_inner())
    }
}

/// The error for a stream this provider has not declared.
fn not_found(id: &str) -> StreamError {
    StreamError::NotFound(format!("stream {id}"))
}

/// Checks that every subject is a node id and that the stream has no default
/// headers, and stores the stream under `id`, active.
fn declaration(id: String, stream: Stream) -> Result<Stream> {
    if !stream.headers.is_empty() {
        return Err(StreamError::Unsupported(manager::NO_HEADERS.into()));
    }
    for subject in &stream.subjects {
        node::parse(subject)?;
    }
    Ok(Stream {
        id,
        active: true,
        ..stream
    })
}

#[async_trait::async_trait]
impl Streams for OpcUaProvider {
    /// Refuses default headers and any subject that is not a node id.
    async fn create(&self, stream: Stream) -> Result<Stream> {
        let id = if stream.id.is_empty() {
            provider::new_stream_id(&stream.name)
        } else {
            stream.id.clone()
        };
        let out = declaration(id.clone(), stream)?;
        self.write().insert(id.clone(), out.clone());
        tracing::info!(stream = %id, name = %out.name, subjects = ?out.subjects, "opcua: stream declared");
        Ok(out)
    }

    /// Reads the in-process declaration; nothing is asked of the server.
    async fn get(&self, id: &str) -> Result<Stream> {
        self.read().get(id).cloned().ok_or_else(|| not_found(id))
    }

    /// Binds a stream, resolving every `nsu=` subject's namespace against the
    /// server, so it fails here, not on the first write, when the server does
    /// not have the namespace.
    async fn bind(&self, id: &str) -> Result<Arc<dyn Manager>> {
        let stream = self.get(id).await?;
        Ok(Arc::new(
            OpcUaManager::bind(self.inner.clone(), stream).await?,
        ))
    }

    /// Revalidates as create does; managers already bound keep the old declaration.
    async fn update(&self, id: &str, stream: Stream) -> Result<Stream> {
        let out = declaration(id.to_owned(), stream)?;
        let mut declared = self.write();
        let slot = declared.get_mut(id).ok_or_else(|| not_found(id))?;
        *slot = out;
        Ok(slot.clone())
    }

    /// Forgets the declaration; items already monitored stay until their streams drop.
    async fn delete(&self, id: &str) -> Result<()> {
        self.write()
            .remove(id)
            .map(|_| ())
            .ok_or_else(|| not_found(id))
    }

    /// Lists only the streams this process declared, in id order.
    async fn list(&self) -> Result<Vec<Stream>> {
        Ok(self.read().values().cloned().collect())
    }

    /// Deletes this provider's client subscription and ends its streams. The
    /// session stays connected: it is the caller's.
    async fn close(&self) -> Result<()> {
        self.inner.hub.close().await;
        Ok(())
    }
}
