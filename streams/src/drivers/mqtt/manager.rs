//! The bound half of the MQTT provider: publishing, subscribing, and durable
//! consumers on one declared stream.

use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use rumqttc::{
    Outgoing,
    v5::{
        AsyncClient, Event, EventLoop, Incoming,
        mqttbytes::v5::{Publish, SubscribeReasonCode},
    },
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    Acker, Batch, Deliveries, Delivery, Durable, Manager, Options, Payload, Publisher, Result,
    Stream, StreamError, Subscriber, Subscription,
    provider::{self, Metrics},
};

use super::{
    Inner, LINGER, Session, dial, safe_name,
    topic::{declares, has_wildcard},
};

/// Publishes to and subscribes from one stream.
pub(crate) struct MqttManager {
    inner: Arc<Inner>,
    stream: Stream,
}

impl MqttManager {
    pub(crate) fn new(inner: Arc<Inner>, stream: Stream) -> Self {
        Self { inner, stream }
    }

    /// Rejects a subject the stream does not declare.
    ///
    /// A declared subject is an MQTT filter, so a stream declaring `user/+`
    /// accepts `user/created`, matched on MQTT's own wildcards.
    fn check_subject(&self, subject: &str) -> Result<()> {
        if declares(&self.stream.subjects, subject) {
            return Ok(());
        }
        tracing::error!(
            subject,
            stream = %self.stream.id,
            declared = ?self.stream.subjects,
            "mqtt: subject is not declared by this stream"
        );
        Err(provider::unknown_subject(
            &self.stream.id,
            subject,
            &self.stream.subjects,
        ))
    }

    fn topic(&self, subject: &str) -> String {
        self.inner.topic(&self.stream.id, subject)
    }

    /// Opens a connection under `client_id`, subscribes to `subject`, and
    /// returns it ready to route.
    async fn attach(
        &self,
        subject: &str,
        group: Option<&str>,
        client_id: &str,
        session: Session,
    ) -> Result<Attached> {
        let topic = self.topic(subject);
        let filter = match group {
            // A shared subscription: the broker gives each message to one
            // member rather than to all of them.
            Some(group) => format!("$share/{group}/{topic}"),
            None => topic,
        };
        let (client, mut events, resumed) = dial(&self.inner.options, client_id, session).await?;
        let mut attached = Attached {
            client,
            events: None,
            early: Vec::new(),
            filter,
            prefix: self.topic(""),
            subject: Arc::from(subject),
            exact: !has_wildcard(subject),
            durable: matches!(session, Session::Durable { .. }),
        };

        // A resumed session comes back with its subscriptions restored, but
        // subscribe anyway. The client id names a consumer and a subject, not
        // a stream, so a session kept from an earlier stream under the same
        // consumer and subject is subscribed to that stream's topic, not this
        // one, and skipping the SUBSCRIBE would leave this consumer deaf.
        // Subscribing again to a filter the session already holds replaces it
        // in place and keeps the queued messages; whatever the broker
        // redelivers before the SUBACK is held in `early`, not raced.
        if resumed {
            tracing::debug!(
                subject,
                client_id,
                "mqtt: resumed a session; subscribing again"
            );
        }

        let failed = |error: &dyn std::fmt::Display| {
            StreamError::Internal(format!("mqtt: cannot subscribe to {subject:?}: {error}"))
        };
        attached
            .client
            .subscribe(attached.filter.clone(), self.inner.config.qos)
            .await
            .map_err(|e| failed(&e))?;
        // The subscription is active once the broker acknowledges it, and the
        // contract promises that before returning. Anything delivered in the
        // meantime is held, not dropped.
        loop {
            match events.poll().await.map_err(|e| failed(&e))? {
                Event::Incoming(Incoming::SubAck(ack)) => {
                    match ack.return_codes.first() {
                        Some(SubscribeReasonCode::Success(_)) => break,
                        // The broker's reason code says why, and is the
                        // difference between a refused filter and a quota.
                        code => {
                            let _ = attached.client.try_disconnect();
                            return Err(failed(&format_args!("broker reason {code:?}")));
                        }
                    }
                }
                Event::Incoming(Incoming::Publish(publish)) => attached.early.push(publish),
                _ => {}
            }
        }
        attached.events = Some(events);
        Ok(attached)
    }
}

/// A subscribed connection, not yet routing.
struct Attached {
    client: AsyncClient,
    events: Option<EventLoop>,
    /// Publishes that arrived before the SUBACK.
    early: Vec<Publish>,
    filter: String,
    /// The topic prefix of this stream, stripped to recover a message's subject.
    prefix: String,
    /// The subject subscribed to, used when a topic cannot be read back.
    subject: Arc<str>,
    /// The subject has no wildcard, so every publish arrives on it and the
    /// one shared copy above is every message's subject.
    exact: bool,
    durable: bool,
}

impl Attached {
    /// The subject a publish arrived on: its topic below the stream's prefix,
    /// so a subscription to `user/+` reports `user/created`. A subscription
    /// to one exact subject shares that subject with every message rather
    /// than allocating it again for each.
    fn subject_of(&self, publish: &Publish) -> Arc<str> {
        if self.exact {
            return self.subject.clone();
        }
        std::str::from_utf8(&publish.topic)
            .ok()
            .and_then(|topic| topic.strip_prefix(self.prefix.as_str()))
            .map_or_else(|| self.subject.clone(), Arc::from)
    }

    /// Routes this connection's publishes into `tx`, converted by `convert`,
    /// until the receiver is dropped or the connection is lost.
    ///
    /// A lost connection ends the stream rather than redialing behind the
    /// caller's back: a clean session redialed has lost its subscription and
    /// whatever was published meanwhile, and a stream that ends says so where
    /// a silent reconnect would not. A durable consumer subscribes again and
    /// resumes its session.
    fn route<T, F>(mut self, tx: mpsc::Sender<T>, mut convert: F)
    where
        T: Send + 'static,
        F: FnMut(&AsyncClient, Arc<str>, Publish) -> Option<T> + Send + 'static,
    {
        let mut events = self
            .events
            .take()
            .expect("attached connections carry their loop");
        tokio::spawn(async move {
            let early = std::mem::take(&mut self.early);
            'routing: {
                for publish in early {
                    let subject = self.subject_of(&publish);
                    if let Some(item) = convert(&self.client, subject, publish)
                        && tx.send(item).await.is_err()
                    {
                        break 'routing;
                    }
                }
                loop {
                    tokio::select! {
                        _ = tx.closed() => break,
                        event = events.poll() => match event {
                            Ok(Event::Incoming(Incoming::Publish(publish))) => {
                                let subject = self.subject_of(&publish);
                                if let Some(item) = convert(&self.client, subject, publish)
                                    && tx.send(item).await.is_err()
                                {
                                    break;
                                }
                            }
                            Ok(Event::Incoming(Incoming::Disconnect(disconnect))) => {
                                tracing::warn!(subject = %self.subject, reason = ?disconnect.reason_code,
                                    "mqtt: broker ended the subscription");
                                return;
                            }
                            Ok(_) => {}
                            Err(error) => {
                                tracing::warn!(subject = %self.subject, %error,
                                    "mqtt: subscription connection lost; ending the stream");
                                return;
                            }
                        }
                    }
                }
            }
            // The receiver is gone. A clean subscription unsubscribes so the
            // broker stops routing to it at once; a durable one only
            // disconnects, because its subscription outliving the connection is
            // what makes it durable.
            if !self.durable {
                let _ = self.client.unsubscribe(self.filter.clone()).await;
            }
            let _ = self.client.disconnect().await;
            let _ = tokio::time::timeout(LINGER, async {
                loop {
                    match events.poll().await {
                        Ok(Event::Outgoing(Outgoing::Disconnect)) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
            })
            .await;
        });
    }
}

#[async_trait::async_trait]
impl Publisher for MqttManager {
    async fn publish(&self, subject: &str, payload: Payload, opts: &Options) -> Result<String> {
        let topic = self.publish_topic(subject, opts)?;
        let id = provider::publish_id(opts);
        let body = Bytes::from(provider::pack(&id, &payload));
        let len = body.len();
        if let Err(error) = self
            .inner
            .publisher
            .link()
            .publish(&topic, self.inner.config.qos, body)
            .await
        {
            tracing::error!(subject, id, %error, "mqtt: could not publish");
            return Err(StreamError::internal(
                &format!("mqtt: cannot publish on {subject:?}"),
                error,
            ));
        }
        tracing::debug!(subject, id, bytes = len, "mqtt: published");
        Ok(id)
    }
}

impl MqttManager {
    /// The topic a publish on `subject` goes to, or why nothing can be
    /// published there with `opts`.
    fn publish_topic(&self, subject: &str, opts: &Options) -> Result<String> {
        self.check_subject(subject)?;
        if has_wildcard(subject) {
            // A stream may declare a filter, but a message has to land
            // somewhere specific. The broker would reject this; saying so here
            // names the call.
            return Err(StreamError::Unsupported(format!(
                "cannot publish to the wildcard subject {subject:?}, only subscribe to it"
            )));
        }
        if opts.delay().is_some() {
            return Err(StreamError::Unsupported(
                "MQTT delivers on publish and cannot schedule".into(),
            ));
        }
        let topic = self.topic(subject);
        if has_wildcard(&topic) {
            return Err(StreamError::Unsupported(format!(
                "the topic {topic:?} carries a wildcard from the stream id or prefix, \
                 and nothing can be published on it"
            )));
        }
        Ok(topic)
    }
}

#[async_trait::async_trait]
impl Subscriber for MqttManager {
    /// Subscribes over a clean session of its own, which keeps nothing: a
    /// message published while nobody is attached is gone. Reach for
    /// [`Durable::consume`] when that matters.
    ///
    /// [`Options::group`] subscribes through a shared subscription, so the
    /// members split the subject rather than each receiving all of it.
    async fn subscribe(&self, subject: &str, opts: &Options) -> Result<Subscription> {
        self.check_subject(subject)?;
        let client_id = format!("runtime-rs-sub-{}", provider::new_id());
        let attached = self
            .attach(subject, opts.group.as_deref(), &client_id, Session::Clean)
            .await?;

        let (tx, rx) = mpsc::channel(provider::prefetch(opts).max(1));
        let registry = self.inner.config.registry.clone();
        attached.route(tx, move |_, subject, publish| {
            match provider::unpack_shared(&registry, &subject, publish.payload) {
                Ok(message) => Some(message),
                Err(error) => {
                    // One bad message is not a reason to end a healthy
                    // subscription. A clean session acknowledges on receipt, so
                    // it is not coming back either.
                    tracing::warn!(%subject, %error, "mqtt: dropping a malformed message");
                    None
                }
            }
        });
        Ok(ReceiverStream::new(rx).boxed())
    }
}

#[async_trait::async_trait]
impl Durable for MqttManager {
    /// Delivers messages under a named consumer.
    ///
    /// The name is an MQTT session: the broker keeps that client's
    /// subscription and its unacknowledged messages while it is away, so a
    /// consumer that dies and comes back is handed what it missed and what it
    /// never finished.
    ///
    /// Two processes cannot share one name: MQTT gives a client id to one
    /// session, and a second connection under it takes the first one's place.
    /// Pass [`Options::group`] to split work instead. That subscribes through a
    /// shared subscription, where the broker distributes messages among the
    /// members, and each member keeps a session of its own.
    ///
    /// # Acknowledging is a packet on the wire
    ///
    /// [`Delivery::ack`] queues a PUBACK, and dropping the stream in the same
    /// breath can close the connection before it is written. The message is
    /// then still outstanding and the broker delivers it again: at-least-once
    /// as promised, but a duplicate a consumer that shut down cleanly would not
    /// have seen. Keep the stream a moment after the last acknowledgement.
    ///
    /// # There is no attempt count
    ///
    /// MQTT flags a repeat delivery but does not say how many times it has
    /// tried, so [`Delivery::attempt`] is zero here, the contract's answer
    /// where a provider cannot count. [`Delivery::nak`] sends nothing, since
    /// MQTT has no negative acknowledgement: the message stays outstanding and
    /// comes back when the session reconnects.
    async fn consume(&self, subject: &str, consumer: &str, opts: &Options) -> Result<Deliveries> {
        self.check_subject(subject)?;
        if consumer.is_empty() {
            return Err(StreamError::Unsupported(
                "a durable consumer needs a name, because the name is the session that survives a restart"
                    .into(),
            ));
        }

        // Without a group the name is the session, and it is one consumer.
        // With one, the group is what the broker shares the subject among, so
        // each member needs an id of its own or the second would take the
        // first's session away.
        let mut client_id = consumer_id(consumer, subject);
        if opts.group.is_some() {
            client_id = format!("{client_id}-{}", provider::new_id());
        }
        let expiry = u32::try_from(self.inner.config.session_expiry.as_secs()).unwrap_or(u32::MAX);
        let prefetch = u16::try_from(provider::prefetch(opts)).unwrap_or(u16::MAX);
        let attached = self
            .attach(
                subject,
                opts.group.as_deref(),
                &client_id,
                Session::Durable { expiry, prefetch },
            )
            .await?;
        tracing::info!(subject, consumer, group = ?opts.group, client_id, "mqtt: consuming");

        let (tx, rx) = mpsc::channel(provider::prefetch(opts).max(1));
        let registry = self.inner.config.registry.clone();
        let metrics = self.inner.config.metrics.clone();
        let declared = subject.to_owned();
        let consumer = consumer.to_owned();
        attached.route(tx, move |client, subject, publish| {
            match provider::unpack_shared(&registry, &subject, publish.payload.clone()) {
                Ok(message) => {
                    metrics.delivered(&declared, &consumer);
                    Some(Delivery::new(
                        message,
                        0,
                        Box::new(MqttAcker {
                            client: client.clone(),
                            publish,
                            metrics: metrics.clone(),
                            subject: declared.clone(),
                            consumer: consumer.clone(),
                        }),
                    ))
                }
                Err(error) => {
                    // It must be acknowledged, or the session would be handed
                    // it again forever.
                    tracing::warn!(%subject, %error, "mqtt: acknowledging a malformed message");
                    let _ = client.try_ack(&publish);
                    None
                }
            }
        });
        Ok(ReceiverStream::new(rx).boxed())
    }
}

#[async_trait::async_trait]
impl Batch for MqttManager {
    /// Publishes every payload at once and waits for all of them. MQTT has no
    /// batch primitive and at QoS 1 each message is acknowledged on its own,
    /// but nothing makes the second wait for the first's PUBACK: the whole
    /// batch is put in flight on the publishing connection, in order, up to
    /// the broker's Receive Maximum, and the call returns when every
    /// acknowledgement is in. A refused entry fails alone, reported in
    /// [`StreamError::PartialBatch`] by its index, as the contract asks.
    async fn publish_batch(
        &self,
        subject: &str,
        payloads: Vec<Payload>,
        opts: &Options,
    ) -> Result<Vec<String>> {
        // An undeclared subject, a wildcard, or a delay fails every entry the
        // same way; say so once, as publish does, rather than as a batch of
        // identical failures.
        provider::check_batch(opts)?;
        let topic = self.publish_topic(subject, opts)?;
        let ids: Vec<String> = payloads.iter().map(|_| provider::new_id()).collect();
        let bodies = payloads
            .iter()
            .zip(&ids)
            .map(|(payload, id)| Bytes::from(provider::pack(id, payload)))
            .collect();
        let outcomes = self
            .inner
            .publisher
            .link()
            .publish_all(&topic, self.inner.config.qos, bodies)
            .await;
        let failures: Vec<(usize, StreamError)> = outcomes
            .into_iter()
            .enumerate()
            .filter_map(|(index, outcome)| outcome.err().map(|error| (index, error)))
            .collect();
        if let Some((_, error)) = failures.first() {
            tracing::error!(subject, failed = failures.len(), total = ids.len(), %error,
                "mqtt: could not publish every message of a batch");
        } else {
            tracing::debug!(subject, count = ids.len(), "mqtt: published a batch");
        }
        provider::batch_result(subject, ids, failures)
    }
}

impl Manager for MqttManager {
    fn durable(&self) -> Option<&dyn Durable> {
        Some(self)
    }

    fn batch(&self) -> Option<&dyn Batch> {
        Some(self)
    }
}

/// Settles one delivery with a PUBACK.
struct MqttAcker {
    client: AsyncClient,
    publish: Publish,
    metrics: Metrics,
    subject: String,
    consumer: String,
}

#[async_trait::async_trait]
impl Acker for MqttAcker {
    async fn ack(self: Box<Self>) -> Result<()> {
        self.client.ack(&self.publish).await.map_err(|error| {
            StreamError::internal(
                &format!("mqtt: cannot acknowledge on {:?}", self.subject),
                error,
            )
        })?;
        self.metrics.settled(&self.subject, &self.consumer, "ack");
        Ok(())
    }

    /// Leaves the message unacknowledged: the broker keeps it against this
    /// session and delivers it again when the consumer reconnects. MQTT has no
    /// negative acknowledgement, so there is nothing more truthful to do.
    async fn nak(self: Box<Self>) -> Result<()> {
        self.metrics.settled(&self.subject, &self.consumer, "nak");
        Ok(())
    }
}

/// The client id a named consumer of one subject connects under.
///
/// Derived from both, deterministically. Stable, because the client id *is*
/// the session: a fresh one on every restart is a stranger the broker kept
/// nothing for. Per subject, because MQTT restores a session's subscriptions
/// wholesale, and one session per subject keeps each consumer's backlog to the
/// one subject it reads.
///
/// The subject is hashed (32-bit FNV-1a, as runtime-go does, so both runtimes
/// resume the same session) rather than appended: brokers cap client id
/// length, and a subject is arbitrarily long.
pub(crate) fn consumer_id(consumer: &str, subject: &str) -> String {
    let hash = subject.bytes().fold(0x811c_9dc5_u32, |hash, byte| {
        (hash ^ u32::from(byte)).wrapping_mul(0x0100_0193)
    });
    format!("{}-{hash:08x}", safe_name(consumer))
}

#[cfg(test)]
mod tests {
    use super::*;

    // The client id is the session, so it has to be the same on every attach
    // or a restart is a stranger the broker kept nothing for.
    #[test]
    fn consumer_id_is_stable_and_per_subject() {
        assert_eq!(
            consumer_id("billing", "user/created"),
            consumer_id("billing", "user/created")
        );
        assert_ne!(
            consumer_id("billing", "user/created"),
            consumer_id("billing", "user/deleted")
        );
        assert_ne!(
            consumer_id("billing", "user/created"),
            consumer_id("shipping", "user/created")
        );
    }

    #[test]
    fn consumer_id_matches_go() {
        // fnv.New32a over "" is the offset basis; over "a" it is 0xe40c292c.
        assert_eq!(consumer_id("c", ""), "c-811c9dc5");
        assert_eq!(consumer_id("a/b", "a"), "a-b-e40c292c");
    }
}
