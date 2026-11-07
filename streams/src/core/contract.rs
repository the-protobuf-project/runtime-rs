//! The traits every provider implements.

use std::sync::Arc;

use futures::stream::BoxStream;

use crate::{Payload, Result, StreamError};

use super::{Delivery, Message, Options, Position, Stream};

/// Messages from one subject. Dropping it ends the subscription and releases
/// the server-side state behind it.
pub type Subscription = BoxStream<'static, Message>;

/// Lent messages from one durable consumer. Dropping it stops delivery; the
/// unsettled messages in flight are redelivered.
pub type Deliveries = BoxStream<'static, Delivery>;

/// Sends payloads to a subject on a stream.
#[async_trait::async_trait]
pub trait Publisher: Send + Sync {
    /// Sends a payload on a subject, once, and returns its id.
    ///
    /// It does not wait for subscribers, and fails with
    /// [`StreamError::UnknownSubject`] when the stream does not declare the
    /// subject.
    async fn publish(&self, subject: &str, payload: Payload, opts: &Options) -> Result<String>;
}

/// Receives messages from a subject on a stream.
#[async_trait::async_trait]
pub trait Subscriber: Send + Sync {
    /// Subscribes to a subject.
    ///
    /// The subscription is active before this returns, so a payload published
    /// afterwards is delivered rather than raced. Dropping the returned stream
    /// is the only way to stop it, and it is deliberate: in Rust that cannot be
    /// forgotten the way an uncancelled context can.
    async fn subscribe(&self, subject: &str, opts: &Options) -> Result<Subscription>;
}

/// A provider that remembers a consumer's position and redelivers until a
/// message is acknowledged.
///
/// NATS JetStream, Kafka, RabbitMQ, Redis Streams, and MQTT persistent sessions
/// have it; Redis Pub/Sub, core NATS, and ZeroMQ do not, and the capability is absent there rather than
/// approximated with a client-side buffer that would lose the same messages one
/// layer higher up.
#[async_trait::async_trait]
pub trait Durable: Send + Sync {
    /// Delivers messages under a named consumer whose position the server keeps.
    ///
    /// The name is the identity that survives a restart: two processes under
    /// one name share the position and split the work, and a process that comes
    /// back resumes where the name left off.
    async fn consume(&self, subject: &str, consumer: &str, opts: &Options) -> Result<Deliveries>;
}

/// A provider whose stored log can be read from somewhere other than now.
#[async_trait::async_trait]
pub trait Positioned: Send + Sync {
    /// [`Durable::consume`] starting at a chosen position.
    async fn consume_from(
        &self,
        subject: &str,
        consumer: &str,
        at: Position,
        opts: &Options,
    ) -> Result<Deliveries>;
}

/// A provider that can publish several payloads in one go.
///
/// Every provider here implements it, worst case through
/// [`provider::publish_each`](crate::provider::publish_each). Reaching for it
/// pays where the backend has a real batch: on Kafka a thousand single
/// publishes are a thousand acknowledgements, and a batch is a handful.
#[async_trait::async_trait]
pub trait Batch: Send + Sync {
    /// Sends payloads on a subject and returns their ids, one per payload and
    /// in order.
    ///
    /// On a partial failure the error is [`StreamError::PartialBatch`], which
    /// still carries every id. [`Options::id`] is refused: one identifier
    /// cannot name several messages.
    async fn publish_batch(
        &self,
        subject: &str,
        payloads: Vec<Payload>,
        opts: &Options,
    ) -> Result<Vec<String>>;
}

/// A publisher and subscriber bound to one stream, plus whichever optional
/// capabilities its backend actually has.
///
/// The capability accessors return `None` by default. A provider overrides one
/// only when its backend has the capability; callers reach them through
/// [`as_durable`], [`as_positioned`], and [`as_batch`], which turn `None` into
/// a sentence.
pub trait Manager: Publisher + Subscriber {
    /// The durable-consumer half, when the backend keeps consumer positions.
    fn durable(&self) -> Option<&dyn Durable> {
        None
    }

    /// The replay half, when the backend retains a log.
    fn positioned(&self) -> Option<&dyn Positioned> {
        None
    }

    /// The batch-publishing half.
    fn batch(&self) -> Option<&dyn Batch> {
        None
    }
}

/// Returns a manager's durable-consumer half, or says why there is none.
///
/// The caller asked for redelivery, and "this provider does not keep a log to
/// redeliver from" is the answer, not `None` fifty lines from the wiring that
/// chose the provider.
pub fn as_durable(manager: &dyn Manager) -> Result<&dyn Durable> {
    manager.durable().ok_or_else(|| {
        StreamError::Unsupported(
            "this provider delivers a message once and does not remember whether it was handled; \
             reach for NATS JetStream, Kafka, RabbitMQ, Redis Streams, or MQTT where redelivery matters"
                .into(),
        )
    })
}

/// Returns a manager's replay half, on the same terms as [`as_durable`].
pub fn as_positioned(manager: &dyn Manager) -> Result<&dyn Positioned> {
    manager.positioned().ok_or_else(|| {
        StreamError::Unsupported(
            "this provider keeps no log to read from, so there is no position but now".into(),
        )
    })
}

/// Returns a manager's batch half, on the same terms as [`as_durable`].
pub fn as_batch(manager: &dyn Manager) -> Result<&dyn Batch> {
    manager.batch().ok_or_else(|| {
        StreamError::Unsupported("this provider publishes one payload at a time".into())
    })
}

/// The lifecycle contract every provider satisfies.
///
/// Providers take a client the caller owns, so nothing here dials or closes a
/// connection the caller handed over.
#[async_trait::async_trait]
pub trait Streams: Send + Sync {
    /// Declares a stream and returns it with its assigned id.
    async fn create(&self, stream: Stream) -> Result<Stream>;

    /// Retrieves a stream, failing with [`StreamError::NotFound`] when there is
    /// none.
    async fn get(&self, id: &str) -> Result<Stream>;

    /// Returns a publisher and subscriber for an existing stream.
    async fn bind(&self, id: &str) -> Result<Arc<dyn Manager>>;

    /// Replaces a stream's configuration.
    async fn update(&self, id: &str, stream: Stream) -> Result<Stream>;

    /// Removes a stream.
    async fn delete(&self, id: &str) -> Result<()>;

    /// Returns every stream.
    async fn list(&self) -> Result<Vec<Stream>>;

    /// Releases resources the provider created for itself. The caller's client
    /// is never closed here; most providers have nothing to release.
    async fn close(&self) -> Result<()> {
        Ok(())
    }
}
