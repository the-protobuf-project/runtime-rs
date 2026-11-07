//! A stream bound to its exchange: publishing, subscribing, and durable
//! consumers.

use std::sync::Arc;

use futures::StreamExt;
use lapin::{
    BasicProperties, Confirmation, Consumer, PublisherConfirm,
    message::Delivery as AmqpDelivery,
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions,
        BasicQosOptions, BasicRejectOptions, QueueBindOptions, QueueDeclareOptions,
    },
    types::{AMQPValue, FieldTable},
};

use crate::{
    Acker, Batch, Deliveries, Delivery, Durable, Manager, Options, Payload, Publisher, Result,
    Stream, StreamError, Subscriber, Subscription,
    provider::{self, Metrics},
};

use super::{
    Shared, queue_name, short,
    topic::{declares, has_wildcard, safe_name},
};

/// AMQP delivery mode 2: the broker writes the message to disk.
const PERSISTENT: u8 = 2;

/// The content type runtime-go stamps on every publish. The frame inside names
/// the real codec, so this is kept for byte-identical properties rather than
/// for what it says.
const CONTENT_TYPE: &str = "application/json";

/// Publishes to and consumes from one stream.
pub(crate) struct RabbitManager {
    shared: Arc<Shared>,
    stream: Stream,
    exchange: String,
}

/// Whether a queue belongs to one subscription or to a named consumer.
enum Attach<'a> {
    /// Exclusive, auto-deleting, server-named, auto-acknowledged.
    Transient,
    /// Durable, named after the consumer, acknowledged by the caller.
    Durable(&'a str),
}

impl RabbitManager {
    pub(crate) fn new(shared: Arc<Shared>, stream: Stream) -> Self {
        let exchange = shared.exchange(&stream.id);
        Self {
            shared,
            stream,
            exchange,
        }
    }

    /// Rejects a subject the stream does not declare.
    ///
    /// An AMQP binding key is a pattern, so a stream declaring `user.*` accepts
    /// `user.created`, matched on AMQP's wildcards rather than NATS's.
    fn check_subject(&self, subject: &str) -> Result<()> {
        if declares(&self.stream.subjects, subject) {
            return Ok(());
        }
        tracing::error!(
            subject,
            stream = %self.stream.id,
            declared = ?self.stream.subjects,
            "rabbitmq: subject is not declared by this stream"
        );
        Err(provider::unknown_subject(
            &self.stream.id,
            subject,
            &self.stream.subjects,
        ))
    }

    /// Opens a channel, declares and binds a queue for `subject`, and starts
    /// consuming from it.
    ///
    /// The returned consumer holds the channel: dropping it cancels the
    /// consumer and closes the channel, which is how a dropped subscription
    /// releases its server-side state without a task of its own.
    async fn attach(&self, subject: &str, kind: Attach<'_>, opts: &Options) -> Result<Consumer> {
        let fail = |what: &str, e: lapin::Error| {
            StreamError::internal(&format!("rabbitmq: cannot {what} for {subject:?}"), e)
        };
        let routing_key = short("routing key", subject)?;
        let exchange = short("exchange name", self.exchange.as_str())?;
        let (name, transient) = match kind {
            Attach::Transient => (short("queue name", "")?, true),
            Attach::Durable(consumer) => (
                short("queue name", queue_name(&self.exchange, consumer, subject))?,
                false,
            ),
        };

        let channel = self
            .shared
            .conn
            .create_channel()
            .await
            .map_err(|e| fail("open a channel", e))?;

        // From here on an early return drops `channel`, the last handle to it,
        // and lapin closes it; nothing half-declared is left consuming.

        // Prefetch is per channel, which is why each consumer gets its own.
        channel
            .basic_qos(
                qos(opts, self.shared.config.prefetch),
                BasicQosOptions::default(),
            )
            .await
            .map_err(|e| fail("set prefetch", e))?;

        let queue = channel
            .queue_declare(
                name,
                QueueDeclareOptions {
                    durable: !transient,
                    exclusive: transient,
                    auto_delete: transient,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| fail("declare a queue", e))?;
        channel
            .queue_bind(
                queue.name().clone(),
                exchange,
                routing_key,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| fail("bind", e))?;

        // A subscription auto-acknowledges, as in runtime-go: it promised no
        // redelivery, and the channel closing ends it either way. A durable
        // consumer leaves acknowledgement to the caller.
        channel
            .basic_consume(
                queue.name().clone(),
                short("consumer tag", "")?,
                BasicConsumeOptions {
                    no_ack: transient,
                    exclusive: transient,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| fail("consume", e))
    }
}

/// The `basic.qos` prefetch count: the call's, else the provider's.
///
/// Zero means "let the provider choose", as it does in runtime-go, rather than
/// AMQP's "unlimited", which is the one setting that lets a single consumer
/// starve its peers.
fn qos(opts: &Options, default: u16) -> u16 {
    match opts.prefetch {
        Some(0) | None => default,
        Some(n) => u16::try_from(n).unwrap_or(u16::MAX),
    }
}

#[async_trait::async_trait]
impl Publisher for RabbitManager {
    /// Sends a payload as a routing key on the stream's exchange.
    ///
    /// The message is marked persistent, so the broker writes it to disk rather
    /// than holding it only in memory. Without that a durable queue would still
    /// lose its contents to a broker restart, which is most of what "durable"
    /// is for.
    async fn publish(&self, subject: &str, payload: Payload, opts: &Options) -> Result<String> {
        let id = provider::publish_id(opts);
        let pending = self.send(subject, &id, &payload, opts).await?;
        confirmed(subject, &id, pending).await?;
        Ok(id)
    }
}

impl RabbitManager {
    /// Refuses what a publish cannot honour, before anything is sent.
    fn check_publish(&self, subject: &str, opts: &Options) -> Result<()> {
        self.check_subject(subject)?;
        if has_wildcard(subject) {
            // A binding key routes nothing when published on; it is declared
            // for subscribers, so to a publisher it is not a subject at all.
            return Err(provider::unknown_subject(
                &self.stream.id,
                subject,
                &self.stream.subjects,
            ));
        }
        if opts.delay().is_some() {
            return Err(StreamError::Unsupported(
                "this provider publishes immediately; RabbitMQ delays need a dead-letter \
                 exchange or the delayed-message plugin"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Hands one persistent message to the broker and returns its pending
    /// confirmation without waiting for it, so a batch can send everything
    /// before awaiting anything.
    async fn send(
        &self,
        subject: &str,
        id: &str,
        payload: &Payload,
        opts: &Options,
    ) -> Result<PublisherConfirm> {
        self.check_publish(subject, opts)?;
        let body = provider::pack(id, payload);
        let properties = BasicProperties::default()
            .with_message_id(short("message id", id)?)
            .with_delivery_mode(PERSISTENT)
            .with_content_type(CONTENT_TYPE.into());
        let exchange = short("exchange name", self.exchange.as_str())?;
        let routing_key = short("routing key", subject)?;
        self.shared
            .channel()
            .await?
            .basic_publish(
                exchange,
                routing_key,
                BasicPublishOptions::default(),
                &body,
                properties,
            )
            .await
            .map_err(|e| {
                tracing::error!(subject, id, error = %e, "rabbitmq: could not publish");
                StreamError::internal(&format!("rabbitmq: cannot publish on {subject:?}"), e)
            })
    }
}

/// Waits for the broker's publisher confirm.
///
/// An ack means the broker has the message, on disk for a persistent message
/// routed to a durable queue; a nack means it refused it, and an error that
/// the channel closed before saying either way. Only the first is a publish.
async fn confirmed(subject: &str, id: &str, pending: PublisherConfirm) -> Result<()> {
    let refused = match pending.await {
        Ok(Confirmation::Ack(_) | Confirmation::NotRequested) => {
            tracing::debug!(subject, id, "rabbitmq: published");
            return Ok(());
        }
        Ok(Confirmation::Nack(_)) => StreamError::Internal(format!(
            "rabbitmq: the broker refused the message on {subject:?} (negative confirm)"
        )),
        Err(e) => StreamError::internal(
            &format!("rabbitmq: the publish on {subject:?} was not confirmed"),
            e,
        ),
    };
    tracing::error!(subject, id, error = %refused, "rabbitmq: could not publish");
    Err(refused)
}

#[async_trait::async_trait]
impl Subscriber for RabbitManager {
    /// Subscribes through an exclusive, auto-deleting queue of its own.
    ///
    /// The queue belongs to this subscription and goes when it is dropped, so
    /// nothing piles up for a subscriber that has gone away. That is what makes
    /// this the undurable half; reach for [`Durable::consume`] when a message
    /// must survive the reader.
    ///
    /// [`Options::group`] is refused: sharing a subject is what a named durable
    /// consumer is, and quietly fanning out instead would double the work.
    async fn subscribe(&self, subject: &str, opts: &Options) -> Result<Subscription> {
        self.check_subject(subject)?;
        if let Some(group) = &opts.group {
            return Err(StreamError::Unsupported(format!(
                "a RabbitMQ subscription is a queue of its own; to share {subject:?} under \
                 {group:?}, consume it durably with {group:?} as the consumer name"
            )));
        }
        let consumer = self.attach(subject, Attach::Transient, opts).await?;
        tracing::info!(subject, stream = %self.stream.id, "rabbitmq: subscribed");

        let registry = self.shared.config.registry.clone();
        let wanted = subject.to_owned();
        let messages = consumer.filter_map(move |delivery| {
            let message = match delivery {
                Ok(d) => provider::unpack(&registry, d.routing_key.as_str(), d.data)
                    .inspect_err(|e| {
                        tracing::warn!(subject = %wanted, error = %e, "rabbitmq: dropping a malformed message");
                    })
                    .ok(),
                Err(e) => {
                    tracing::warn!(subject = %wanted, error = %e, "rabbitmq: subscription failed");
                    None
                }
            };
            std::future::ready(message)
        });
        Ok(messages.boxed())
    }
}

#[async_trait::async_trait]
impl Durable for RabbitManager {
    /// Delivers messages from a durable queue named after `consumer`.
    ///
    /// The broker holds the queue, so it outlives the process reading it:
    /// messages published while nobody is consuming are waiting when the name
    /// comes back, and a message taken by a consumer that dies unacknowledged
    /// is given to whoever is next. Dropping the stream cancels the consumer
    /// and closes its channel, which returns every unsettled message.
    ///
    /// Two processes under one name take from that one queue and so split the
    /// work, which is why [`Options::group`] is refused: the name is already
    /// the group.
    async fn consume(&self, subject: &str, consumer: &str, opts: &Options) -> Result<Deliveries> {
        self.check_subject(subject)?;
        if consumer.is_empty() {
            return Err(StreamError::Unsupported(
                "a durable consumer needs a name, because the name is the queue that survives \
                 a restart"
                    .into(),
            ));
        }
        if opts.group.is_some() {
            return Err(StreamError::Unsupported(format!(
                "the consumer name {consumer:?} is already the queue on RabbitMQ, and consumers \
                 of one queue already share it; drop the group option"
            )));
        }
        let amqp = self
            .attach(subject, Attach::Durable(&safe_name(consumer)), opts)
            .await?;
        tracing::info!(subject, consumer, stream = %self.stream.id, "rabbitmq: consuming");

        let registry = self.shared.config.registry.clone();
        let metrics = self.shared.config.metrics.clone();
        let subject = subject.to_owned();
        let consumer = consumer.to_owned();
        let deliveries = amqp.filter_map(move |delivery| {
            let registry = registry.clone();
            let metrics = metrics.clone();
            let subject = subject.clone();
            let consumer = consumer.clone();
            async move {
                let d = match delivery {
                    Ok(d) => d,
                    Err(e) => {
                        tracing::warn!(%subject, %consumer, error = %e, "rabbitmq: consumer failed");
                        return None;
                    }
                };
                let attempt = attempt(&d);
                let AmqpDelivery {
                    routing_key,
                    data,
                    acker,
                    ..
                } = d;
                match provider::unpack(&registry, routing_key.as_str(), data) {
                    Ok(message) => {
                        metrics.delivered(&subject, &consumer);
                        let acker = AmqpAck {
                            acker,
                            metrics,
                            subject,
                            consumer,
                        };
                        Some(Delivery::new(message, attempt, Box::new(acker)))
                    }
                    Err(e) => {
                        // Nothing will ever decode this, so no handler will ever
                        // acknowledge it. Reject it without requeueing, the one
                        // case where discarding is right: requeueing would hand
                        // the same undecodable bytes to the next consumer forever.
                        tracing::warn!(%subject, error = %e, "rabbitmq: rejecting a malformed message");
                        let _ = acker.reject(BasicRejectOptions { requeue: false }).await;
                        None
                    }
                }
            }
        });
        Ok(deliveries.boxed())
    }
}

#[async_trait::async_trait]
impl Batch for RabbitManager {
    /// Sends every payload, then waits for every confirmation, as runtime-go
    /// does: a batch costs one round trip rather than one per message.
    async fn publish_batch(
        &self,
        subject: &str,
        payloads: Vec<Payload>,
        opts: &Options,
    ) -> Result<Vec<String>> {
        provider::check_batch(opts)?;
        self.check_publish(subject, opts)?;
        let ids: Vec<String> = payloads.iter().map(|_| provider::new_id()).collect();
        let mut pending = Vec::with_capacity(ids.len());
        for (payload, id) in payloads.iter().zip(&ids) {
            pending.push(self.send(subject, id, payload, opts).await);
        }
        let mut failures = Vec::new();
        for (index, (sent, id)) in pending.into_iter().zip(&ids).enumerate() {
            let outcome = match sent {
                Ok(confirm) => confirmed(subject, id, confirm).await,
                Err(e) => Err(e),
            };
            if let Err(e) = outcome {
                failures.push((index, e));
            }
        }
        provider::batch_result(subject, ids, failures)
    }
}

impl Manager for RabbitManager {
    fn durable(&self) -> Option<&dyn Durable> {
        Some(self)
    }

    fn batch(&self) -> Option<&dyn Batch> {
        Some(self)
    }
}

/// How many times a delivery has been tried.
///
/// Quorum queues count redeliveries in an `x-delivery-count` header; classic
/// queues carry only a redelivered flag, which says a message has been seen
/// before but not how often. The flag is worth reporting as "at least the
/// second" rather than discarding.
fn attempt(d: &AmqpDelivery) -> u32 {
    let count = d
        .properties
        .headers()
        .as_ref()
        .and_then(|headers| headers.inner().get("x-delivery-count"))
        .and_then(delivery_count);
    match count {
        Some(n) => u32::try_from(n.saturating_add(1)).unwrap_or(u32::MAX),
        None if d.redelivered => 2,
        None => 1,
    }
}

/// Reads an integer header value of any AMQP width.
fn delivery_count(value: &AMQPValue) -> Option<u64> {
    match *value {
        AMQPValue::ShortShortInt(n) => u64::try_from(n).ok(),
        AMQPValue::ShortShortUInt(n) => Some(n.into()),
        AMQPValue::ShortInt(n) => u64::try_from(n).ok(),
        AMQPValue::ShortUInt(n) => Some(n.into()),
        AMQPValue::LongInt(n) => u64::try_from(n).ok(),
        AMQPValue::LongUInt(n) => Some(n.into()),
        AMQPValue::LongLongInt(n) => u64::try_from(n).ok(),
        _ => None,
    }
}

/// Settles one delivery. RabbitMQ has a true negative acknowledgement, so nak
/// requeues immediately rather than waiting out a visibility timeout.
struct AmqpAck {
    acker: lapin::Acker,
    metrics: Metrics,
    subject: String,
    consumer: String,
}

impl AmqpAck {
    /// Turns lapin's "nothing was sent" into an error: the acker had been used,
    /// or its channel closed and the broker has already requeued the message.
    fn settled(&self, sent: lapin::Result<bool>, what: &str, outcome: &'static str) -> Result<()> {
        let context = || format!("rabbitmq: cannot {what} on {:?}", self.subject);
        match sent {
            Ok(true) => {
                self.metrics.settled(&self.subject, &self.consumer, outcome);
                Ok(())
            }
            Ok(false) => Err(StreamError::Internal(format!(
                "{}: the channel is closed, so the broker has already returned the message",
                context()
            ))),
            Err(e) => Err(StreamError::internal(&context(), e)),
        }
    }
}

#[async_trait::async_trait]
impl Acker for AmqpAck {
    async fn ack(self: Box<Self>) -> Result<()> {
        let sent = self.acker.ack(BasicAckOptions::default()).await;
        self.settled(sent, "acknowledge", "ack")
    }

    async fn nak(self: Box<Self>) -> Result<()> {
        // Requeue, so it goes back for another consumer rather than being
        // discarded.
        let sent = self
            .acker
            .nack(BasicNackOptions {
                requeue: true,
                ..Default::default()
            })
            .await;
        self.settled(sent, "return the message", "nak")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use lapin::types::ShortString;

    use super::*;

    fn delivery(redelivered: bool, count: Option<AMQPValue>) -> AmqpDelivery {
        let mut d = AmqpDelivery::mock(1, "x".into(), "k".into(), redelivered, Vec::new());
        if let Some(count) = count {
            let headers = BTreeMap::from([(ShortString::from("x-delivery-count"), count)]);
            d.properties = BasicProperties::default().with_headers(FieldTable::from(headers));
        }
        d
    }

    #[test]
    fn attempt_reads_the_quorum_count_then_the_flag() {
        assert_eq!(attempt(&delivery(false, None)), 1);
        assert_eq!(attempt(&delivery(true, None)), 2);
        assert_eq!(attempt(&delivery(true, Some(AMQPValue::LongLongInt(3)))), 4);
        assert_eq!(attempt(&delivery(false, Some(AMQPValue::LongInt(0)))), 1);
        assert_eq!(
            attempt(&delivery(true, Some(AMQPValue::LongString("x".into())))),
            2
        );
    }

    #[test]
    fn qos_falls_back_and_clamps() {
        let default = Options::default();
        assert_eq!(qos(&default, 32), 32);
        assert_eq!(qos(&default.clone().with_prefetch(0), 32), 32);
        assert_eq!(qos(&default.clone().with_prefetch(5), 32), 5);
        assert_eq!(qos(&default.with_prefetch(1 << 20), 32), u16::MAX);
    }
}
