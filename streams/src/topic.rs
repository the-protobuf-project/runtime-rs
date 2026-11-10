//! Typed topics: one subject of a stream, its message type, and the defaults
//! it publishes and receives with.
//!
//! It is [`Typed`](crate::Typed)'s richer sibling, and the runtime
//! protoc-gen-streams generates against. Where a Typed view takes the subject
//! and options at every call, a [`Topic`] is fixed to one subject by a
//! [`TopicSpec`]: a `static` holding the subject, the default consumer name,
//! static headers, headers and a partition key read from each message, expiry,
//! retain, prefetch and a [`BatchPolicy`]. It hands out the three ways to use
//! the subject:
//!
//! | Handle | Calls | Guarantee |
//! | --- | --- | --- |
//! | [`Publisher`] | `publish`, `publish_with`, `publish_batch` | the broker accepted it |
//! | [`Subscriber`] | `subscribe`, `subscribe_with` | at most once |
//! | [`Consumer`] | `consume`, `consume_from` | at least once, with ack and nak |
//!
//! plus a [`BatchSink`] that buffers values and publishes them by the policy.
//! [`bind`] declares the stream a set of topics lives on, widening an existing
//! declaration rather than replacing it, and checks the capabilities the
//! caller [`require`]s.
//!
//! Nothing here depends on what a provider can do: a durable consumer on a
//! provider without one fails with [`StreamError::Unsupported`] at the call,
//! unless the stream was bound requiring [`Capability::Durable`] and so failed
//! at [`bind`] instead.
//!
//! # The default codec
//!
//! Every type here takes its codec as a parameter, `C`. With the `protobuf`
//! feature it defaults to `Protobuf`, so `Topic<Order>` carries a prost
//! message; without it there is no default and the codec is always written,
//! `Topic<Order, Json>`. The default is never anything else: were it `Json`
//! without the feature, enabling `protobuf` anywhere in a build would quietly
//! change what `Topic<Order>` means, and code that compiled would stop.
//!
//! ```
//! # #[cfg(feature = "protobuf")] {
//! use std::time::Duration;
//! use streams::topic::{BatchPolicy, TopicSpec};
//!
//! #[derive(Clone, PartialEq, prost::Message)]
//! struct Order {
//!     #[prost(string, tag = "1")]
//!     order_id: String,
//! }
//!
//! fn order_id(order: &Order) -> Option<String> {
//!     Some(order.order_id.clone())
//! }
//!
//! static PLACED: TopicSpec<Order> = TopicSpec {
//!     subject: "order.placed",
//!     consumer: "billing",
//!     group: None,
//!     headers: &[("source", "checkout")],
//!     header_fields: &[],
//!     partition_key: Some(order_id),
//!     expiry: None,
//!     retain: false,
//!     prefetch: None,
//!     batch: BatchPolicy { max_size: 100, max_delay: Duration::from_millis(10) },
//! };
//! # }
//! ```

use std::{fmt, sync::Arc, time::Duration};

use futures::{StreamExt, stream::BoxStream};
use tokio::time::Instant;
use tpp_telemetry::{
    FutureExt, KeyValue, Observer, Operation, Outcome, SpanKind, Start, Telemetry, extract,
    opentelemetry::propagation::Extractor,
};

#[cfg(feature = "protobuf")]
use crate::Protobuf;
use crate::{
    Capability, Codec, Deliveries, Manager, Message, Options, Payload, Position, Result, Stream,
    StreamError, Streams, as_batch, as_durable, as_positioned, require,
};

/// Declares `stream` on `provider` unless it already exists, binds it, and
/// checks that the bound manager has every capability in `capabilities`.
///
/// A stream that exists but lacks some of `stream`'s subjects, because the
/// proto gained a method since it was created, is updated to declare them as
/// well. Subjects are only ever added: one this build no longer uses may still
/// have publishers and subscribers in a build that does. Nothing else of the
/// existing declaration changes, so headers or a description set by an
/// operator survive a redeploy.
///
/// It lives here rather than on [`Streams`] because it is a policy, not a
/// primitive: whether a running service may widen a shared declaration is the
/// caller's decision, and a caller that may not uses [`Streams::bind`].
pub async fn bind(
    provider: &dyn Streams,
    stream: Stream,
    capabilities: &[Capability],
) -> Result<Arc<dyn Manager>> {
    let id = match provider.get(&stream.id).await {
        Ok(existing) => {
            let missing: Vec<String> = stream
                .subjects
                .iter()
                .filter(|subject| !existing.subjects.contains(subject))
                .cloned()
                .collect();
            if !missing.is_empty() {
                let mut wider = existing.clone();
                wider.subjects.extend(missing);
                provider.update(&existing.id, wider).await?;
            }
            existing.id
        }
        Err(StreamError::NotFound(_)) => provider.create(stream).await?.id,
        Err(error) => return Err(error),
    };
    let manager = provider.bind(&id).await?;
    require(&*manager, capabilities)?;
    Ok(manager)
}

/// When a [`BatchSink`] flushes: whichever limit is reached first.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchPolicy {
    /// Flush once this many values are buffered. Zero is treated as one.
    pub max_size: usize,
    /// Flush once the oldest buffered value has waited this long.
    pub max_delay: Duration,
}

/// Reads a header value or partition key out of a message; `None` when a
/// message on the way to the field is unset, and the header or key is then
/// left off rather than sent empty.
pub type FieldFn<T> = fn(&T) -> Option<String>;

/// The static half of a topic: its subject and the defaults every publish and
/// receive starts from.
///
/// Every field is a constant or a function pointer, so a spec is built in a
/// `static` and a [`Topic`] borrows it for `'static` rather than cloning
/// headers on every call. protoc-gen-streams writes one per topic from
/// `streams.yaml`.
#[derive(Debug)]
pub struct TopicSpec<T: 'static> {
    /// The subject the topic publishes and subscribes on.
    pub subject: &'static str,
    /// The durable consumer name [`Consumer::name`] suggests.
    pub consumer: &'static str,
    /// The subscription group every subscriber and consumer joins by default.
    pub group: Option<&'static str>,
    /// Headers on every publish, under the stream's own.
    pub headers: &'static [(&'static str, &'static str)],
    /// Headers read from each published message, over the static ones.
    pub header_fields: &'static [(&'static str, FieldFn<T>)],
    /// The partition key, read from each published message.
    pub partition_key: Option<FieldFn<T>>,
    /// Drops a message not delivered within this time; see [`Options::expires`].
    pub expiry: Option<Duration>,
    /// Keeps each message as the subject's last value; see [`Options::retain`].
    pub retain: bool,
    /// Messages a provider may hold ahead of the reader; see [`Options::prefetch`].
    pub prefetch: Option<usize>,
    /// The flush policy of [`BatchSink::new`].
    pub batch: BatchPolicy,
}

/// One subject of a bound stream, typed to the message `T` it carries and
/// encoded with codec `C`.
///
/// Cheap to clone and to make: a manager handle, a codec and a `&'static`
/// spec, so making one per call, as generated accessors do, costs nothing.
pub struct Topic<
    T: 'static,
    #[cfg(feature = "protobuf")] C = Protobuf,
    #[cfg(not(feature = "protobuf"))] C,
> {
    manager: Arc<dyn Manager>,
    codec: C,
    spec: &'static TopicSpec<T>,
    telemetry: Option<Arc<TopicTelemetry>>,
}

impl<T, C: Clone> Clone for Topic<T, C> {
    /// Written by hand so cloning needs only `C: Clone`, not `T: Clone`.
    fn clone(&self) -> Self {
        Self {
            manager: self.manager.clone(),
            codec: self.codec.clone(),
            spec: self.spec,
            telemetry: self.telemetry.clone(),
        }
    }
}

impl<T, C> Topic<T, C>
where
    T: Send + Sync + 'static,
    C: Codec<T> + Clone + Send + Sync + 'static,
{
    /// A topic of the stream `manager` is bound to.
    ///
    /// The subject is not checked here: a stream that does not declare it
    /// fails the first publish or subscribe with
    /// [`StreamError::UnknownSubject`], as a raw call would.
    pub fn new(manager: Arc<dyn Manager>, codec: C, spec: &'static TopicSpec<T>) -> Self {
        Self {
            manager,
            codec,
            spec,
            telemetry: None,
        }
    }

    /// The topic reporting through `telemetry`, as `backend`, the steps a
    /// provider cannot see: `streams.send` around each publish, with
    /// `streams.encode` and the provider's own `streams.publish` (when it is
    /// [`instrument`](crate::instrument())ed) inside, and `streams.decode` for each
    /// message received, the child of the provider's receive or deliver span,
    /// or of the publisher's when the provider is not instrumented.
    ///
    /// Each is a share of `runtime.operation.count` and
    /// `runtime.operation.duration`; telemetry without a signal changes nothing.
    pub fn with_telemetry(mut self, telemetry: &Telemetry, backend: &'static str) -> Self {
        if telemetry.is_enabled() {
            self.telemetry = Some(Arc::new(TopicTelemetry {
                observer: telemetry.observer("streams"),
                backend,
            }));
        }
        self
    }

    /// The subject it publishes and subscribes on.
    pub fn subject(&self) -> &'static str {
        self.spec.subject
    }

    /// Its subject and defaults.
    pub fn spec(&self) -> &'static TopicSpec<T> {
        self.spec
    }

    /// The manager it publishes and receives through.
    pub fn manager(&self) -> &Arc<dyn Manager> {
        &self.manager
    }

    /// Publishes messages: done when the broker accepted them.
    pub fn publisher(&self) -> Publisher<T, C> {
        Publisher {
            topic: self.clone(),
        }
    }

    /// Receives messages at most once, from now on.
    pub fn subscriber(&self) -> Subscriber<T, C> {
        Subscriber {
            topic: self.clone(),
        }
    }

    /// Receives messages at least once, under a named consumer whose position
    /// the broker keeps.
    pub fn consumer(&self) -> Consumer<T, C> {
        Consumer {
            topic: self.clone(),
        }
    }

    /// The options a publish of `value` starts from, with `over` laid on top.
    fn publish_options(&self, value: &T, over: Options) -> Options {
        let spec = self.spec;
        let mut opts = Options {
            partition_key: spec.partition_key.and_then(|key| key(value)),
            expires: spec.expiry,
            retain: spec.retain,
            ..Options::default()
        };
        opts.headers.extend(spec.headers.iter().copied());
        for (header, field) in spec.header_fields {
            if let Some(v) = field(value) {
                opts.headers.insert(*header, v);
            }
        }
        overlay(opts, over)
    }

    /// The options a subscription or consumer starts from, with `over` on top.
    fn receive_options(&self, over: Options) -> Options {
        let base = Options {
            group: self.spec.group.map(str::to_owned),
            prefetch: self.spec.prefetch,
            ..Options::default()
        };
        overlay(base, over)
    }
}

/// Lays `over` on `base`: every setting `over` makes wins, and its headers are
/// merged over `base`'s.
///
/// `retain` can only be turned on: a `false` is indistinguishable from not
/// saying, and a topic declared retained should not lose it to a default.
fn overlay(base: Options, over: Options) -> Options {
    Options {
        ttl: over.ttl.or(base.ttl),
        id: over.id.or(base.id),
        group: over.group.or(base.group),
        prefetch: over.prefetch.or(base.prefetch),
        partition_key: over.partition_key.or(base.partition_key),
        headers: base.headers.merged(&over.headers),
        expires: over.expires.or(base.expires),
        retain: over.retain || base.retain,
    }
}

/// Publishes `T` on a topic.
pub struct Publisher<
    T: 'static,
    #[cfg(feature = "protobuf")] C = Protobuf,
    #[cfg(not(feature = "protobuf"))] C,
> {
    topic: Topic<T, C>,
}

impl<T, C: Clone> Clone for Publisher<T, C> {
    /// Written by hand so cloning needs only `C: Clone`.
    fn clone(&self) -> Self {
        Self {
            topic: self.topic.clone(),
        }
    }
}

impl<T, C> Publisher<T, C>
where
    T: Send + Sync + 'static,
    C: Codec<T> + Clone + Send + Sync + 'static,
{
    /// Publishes `value` with the topic's defaults and returns its id.
    pub async fn publish(&self, value: &T) -> Result<String> {
        self.publish_with(value, Options::default()).await
    }

    /// Publishes `value` with `opts` laid over the topic's defaults, so a key
    /// or header set here wins for this message.
    pub async fn publish_with(&self, value: &T, opts: Options) -> Result<String> {
        let opts = self.topic.publish_options(value, opts);
        let topic = &self.topic;
        let Some(tel) = &topic.telemetry else {
            let payload = Payload::encode(&topic.codec, value)?;
            return topic
                .manager
                .publish(topic.spec.subject, payload, &opts)
                .await;
        };
        let send = tel.start(
            &topic.codec,
            topic.spec.subject,
            "send",
            SpanKind::Producer,
            None,
        );
        let payload = tel.encode(
            Some(send.context()),
            &topic.codec,
            topic.spec.subject,
            std::slice::from_ref(value),
        );
        let result = match payload {
            Ok(mut payloads) => {
                let payload = payloads.pop().expect("one value, one payload");
                topic
                    .manager
                    .publish(topic.spec.subject, payload, &opts)
                    .with_context(send.context())
                    .await
            }
            Err(error) => Err(error),
        };
        TopicTelemetry::finish(send, &result);
        result
    }

    /// Publishes `values` in order and returns their ids, one per value.
    ///
    /// It uses the provider's [`Batch`](crate::Batch) where it has one and
    /// publishes one at a time where it does not, so the call works everywhere
    /// and is fast where the broker allows. Consecutive values that share a
    /// partition key and headers go out as one batch, since a batch carries one
    /// set of options; values that alternate keys cost one batch each.
    pub async fn publish_batch(&self, values: &[T]) -> Result<Vec<String>> {
        let mut ids = Vec::with_capacity(values.len());
        let mut start = 0;
        while start < values.len() {
            let opts = self
                .topic
                .publish_options(&values[start], Options::default());
            let mut end = start + 1;
            while end < values.len()
                && self.topic.publish_options(&values[end], Options::default()) == opts
            {
                end += 1;
            }
            ids.extend(self.publish_run(&values[start..end], &opts).await?);
            start = end;
        }
        Ok(ids)
    }

    /// Publishes values that share one set of options.
    async fn publish_run(&self, values: &[T], opts: &Options) -> Result<Vec<String>> {
        let Some(tel) = &self.topic.telemetry else {
            return self.publish_encoded(values, opts).await;
        };
        let topic = &self.topic;
        let mut send = tel.start(
            &topic.codec,
            topic.spec.subject,
            "send",
            SpanKind::Producer,
            None,
        );
        send.set_attribute(KeyValue::new(
            "messaging.batch.message_count",
            values.len() as i64,
        ));
        let result = self
            .publish_encoded(values, opts)
            .with_context(send.context())
            .await;
        if let Ok(ids) = &result {
            send.written_rows(ids.len());
        }
        TopicTelemetry::finish(send, &result);
        result
    }

    /// Encodes values, as one encode operation under the send in the current
    /// context when the topic has telemetry, and publishes them as one run.
    async fn publish_encoded(&self, values: &[T], opts: &Options) -> Result<Vec<String>> {
        let subject = self.topic.spec.subject;
        let payloads = match &self.topic.telemetry {
            Some(tel) => tel.encode(None, &self.topic.codec, subject, values)?,
            None => values
                .iter()
                .map(|value| Payload::encode(&self.topic.codec, value))
                .collect::<Result<Vec<_>>>()?,
        };
        match as_batch(&*self.topic.manager) {
            Ok(batch) => batch.publish_batch(subject, payloads, opts).await,
            Err(StreamError::Unsupported(_)) => {
                let mut ids = Vec::with_capacity(payloads.len());
                for payload in payloads {
                    ids.push(self.topic.manager.publish(subject, payload, opts).await?);
                }
                Ok(ids)
            }
            Err(error) => Err(error),
        }
    }
}

/// Receives `T` from a topic, at most once.
pub struct Subscriber<
    T: 'static,
    #[cfg(feature = "protobuf")] C = Protobuf,
    #[cfg(not(feature = "protobuf"))] C,
> {
    topic: Topic<T, C>,
}

impl<T, C: Clone> Clone for Subscriber<T, C> {
    /// Written by hand so cloning needs only `C: Clone`.
    fn clone(&self) -> Self {
        Self {
            topic: self.topic.clone(),
        }
    }
}

impl<T, C> Subscriber<T, C>
where
    T: Send + Sync + 'static,
    C: Codec<T> + Clone + Send + Sync + 'static,
{
    /// Subscribes with the topic's defaults.
    pub async fn subscribe(&self) -> Result<BoxStream<'static, T>> {
        self.subscribe_with(Options::default()).await
    }

    /// Subscribes with `opts` laid over the topic's defaults.
    ///
    /// A message that does not decode is skipped with a warning rather than
    /// ending the subscription: one bad publisher should not stop delivery of
    /// everyone else's messages.
    pub async fn subscribe_with(&self, opts: Options) -> Result<BoxStream<'static, T>> {
        let opts = self.topic.receive_options(opts);
        let messages = self
            .topic
            .manager
            .subscribe(self.topic.spec.subject, &opts)
            .await?;
        let codec = self.topic.codec.clone();
        let telemetry = self.topic.telemetry.clone();
        Ok(messages
            .filter_map(move |message| {
                let decoded = match TopicTelemetry::decode(telemetry.as_deref(), &message, &codec) {
                    Ok(value) => Some(value),
                    Err(error) => {
                        tracing::warn!(
                            subject = message.subject(),
                            id = message.id(),
                            %error,
                            "skipping a message that does not decode"
                        );
                        None
                    }
                };
                std::future::ready(decoded)
            })
            .boxed())
    }
}

/// Receives `T` from a topic at least once, under a named consumer.
pub struct Consumer<
    T: 'static,
    #[cfg(feature = "protobuf")] C = Protobuf,
    #[cfg(not(feature = "protobuf"))] C,
> {
    topic: Topic<T, C>,
}

impl<T, C: Clone> Clone for Consumer<T, C> {
    /// Written by hand so cloning needs only `C: Clone`.
    fn clone(&self) -> Self {
        Self {
            topic: self.topic.clone(),
        }
    }
}

impl<T, C> Consumer<T, C>
where
    T: Send + Sync + 'static,
    C: Codec<T> + Clone + Send + Sync + 'static,
{
    /// The consumer name the topic's spec chose, to pass to
    /// [`Consumer::consume`] when there is no reason to pick another.
    pub fn name(&self) -> &'static str {
        self.topic.spec.consumer
    }

    /// Delivers messages under consumer `name`, from where it left off.
    ///
    /// Fails with [`StreamError::Unsupported`] where the provider keeps no
    /// consumer positions.
    pub async fn consume(&self, name: &str) -> Result<BoxStream<'static, Delivery<T>>> {
        let opts = self.topic.receive_options(Options::default());
        let durable = as_durable(&*self.topic.manager)?;
        let deliveries = durable
            .consume(self.topic.spec.subject, name, &opts)
            .await?;
        Ok(self.decode(deliveries))
    }

    /// Delivers messages under consumer `name`, starting at `at`.
    ///
    /// Fails with [`StreamError::Unsupported`] where the provider keeps no log
    /// to read from.
    pub async fn consume_from(
        &self,
        name: &str,
        at: Position,
    ) -> Result<BoxStream<'static, Delivery<T>>> {
        let opts = self.topic.receive_options(Options::default());
        let positioned = as_positioned(&*self.topic.manager)?;
        let deliveries = positioned
            .consume_from(self.topic.spec.subject, name, at, &opts)
            .await?;
        Ok(self.decode(deliveries))
    }

    /// Decodes each delivery.
    ///
    /// One that does not decode never will, so it is acknowledged, with a
    /// warning, rather than redelivered forever: naking it would loop, and
    /// leaving it unsettled would hold a prefetch slot until the broker gave
    /// up on the consumer, then do the same to the next one.
    fn decode(&self, deliveries: Deliveries) -> BoxStream<'static, Delivery<T>> {
        let codec = self.topic.codec.clone();
        let telemetry = self.topic.telemetry.clone();
        deliveries
            .filter_map(move |delivery| {
                let codec = codec.clone();
                let telemetry = telemetry.clone();
                async move {
                    let decoded: Result<T> =
                        TopicTelemetry::decode(telemetry.as_deref(), delivery.message(), &codec);
                    match decoded {
                        Ok(value) => Some(Delivery { value, delivery }),
                        Err(error) => {
                            tracing::warn!(
                                subject = delivery.subject(),
                                id = delivery.id(),
                                %error,
                                "acknowledging a message that does not decode"
                            );
                            if let Err(error) = delivery.ack().await {
                                tracing::warn!(%error, "could not acknowledge the message");
                            }
                            None
                        }
                    }
                }
            })
            .boxed()
    }
}

/// A decoded message lent by a [`Consumer`], to settle once handled.
///
/// Dropping it unsettled leaves it to the broker's redelivery, as with the
/// [`Delivery`](crate::Delivery) it wraps.
pub struct Delivery<T> {
    value: T,
    delivery: crate::Delivery,
}

impl<T> Delivery<T> {
    /// The decoded message.
    pub fn message(&self) -> &T {
        &self.value
    }

    /// The message as it arrived: id, subject, codec, headers.
    pub fn raw(&self) -> &Message {
        self.delivery.message()
    }

    /// How many times it has been delivered, starting at 1; 0 where the
    /// provider cannot count.
    pub fn attempt(&self) -> u32 {
        self.delivery.attempt()
    }

    /// Reports it handled, so it is not delivered again.
    pub async fn ack(self) -> Result<()> {
        self.delivery.ack().await
    }

    /// Returns it for redelivery.
    pub async fn nak(self) -> Result<()> {
        self.delivery.nak().await
    }

    /// Returns it for redelivery no sooner than `delay` from now, where the
    /// broker can hold it back; see [`Delivery::nak_after`](crate::Delivery::nak_after).
    pub async fn nak_after(self, delay: Duration) -> Result<()> {
        self.delivery.nak_after(delay).await
    }

    /// The decoded message and the delivery that settles it, for handing the
    /// two to different owners.
    pub fn into_parts(self) -> (T, crate::Delivery) {
        (self.value, self.delivery)
    }
}

impl<T: fmt::Debug> fmt::Debug for Delivery<T> {
    /// The decoded message and the raw delivery, which leaves out its acker.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Delivery")
            .field("value", &self.value)
            .field("delivery", &self.delivery)
            .finish()
    }
}

/// Buffers values for a topic and publishes them in batches, by its
/// [`BatchPolicy`]: the convenience protoc-gen-streams gives a
/// client-streaming method.
///
/// Values still buffered when it is dropped are not published; end with
/// [`BatchSink::finish`], or hand the whole stream to [`BatchSink::send_all`].
#[must_use = "buffered values are only published by flush, finish or send_all"]
pub struct BatchSink<
    T: 'static,
    #[cfg(feature = "protobuf")] C = Protobuf,
    #[cfg(not(feature = "protobuf"))] C,
> {
    publisher: Publisher<T, C>,
    policy: BatchPolicy,
    buffer: Vec<T>,
    oldest: Option<Instant>,
    ids: Vec<String>,
}

impl<T, C> BatchSink<T, C>
where
    T: Send + Sync + 'static,
    C: Codec<T> + Clone + Send + Sync + 'static,
{
    /// A sink over `publisher`, flushing by its topic's policy.
    pub fn new(publisher: Publisher<T, C>) -> Self {
        let policy = publisher.topic.spec.batch;
        Self::with_policy(publisher, policy)
    }

    /// A sink over `publisher`, flushing by `policy`.
    pub fn with_policy(publisher: Publisher<T, C>, policy: BatchPolicy) -> Self {
        Self {
            publisher,
            policy,
            buffer: Vec::new(),
            oldest: None,
            ids: Vec::new(),
        }
    }

    /// Buffers `value`, publishing the buffer when it is full or its oldest
    /// value has waited long enough.
    ///
    /// The delay is only checked here, when a value arrives: a sink fed by
    /// hand that goes quiet holds its buffer until the next `send` or
    /// [`BatchSink::flush`]. [`BatchSink::send_all`] flushes on time while
    /// waiting.
    pub async fn send(&mut self, value: T) -> Result<()> {
        self.oldest.get_or_insert_with(Instant::now);
        self.buffer.push(value);
        if self.buffer.len() >= self.policy.max_size.max(1) || self.overdue() {
            self.flush().await?;
        }
        Ok(())
    }

    /// Publishes everything buffered.
    pub async fn flush(&mut self) -> Result<()> {
        self.oldest = None;
        if self.buffer.is_empty() {
            return Ok(());
        }
        let values = std::mem::take(&mut self.buffer);
        let ids = self.publisher.publish_batch(&values).await?;
        self.ids.extend(ids);
        Ok(())
    }

    /// Publishes what is left and returns the id of every value sent, in the
    /// order they were sent.
    pub async fn finish(mut self) -> Result<Vec<String>> {
        self.flush().await?;
        Ok(self.ids)
    }

    /// Publishes every value of `values` in batches, flushing on size and on
    /// delay even while the stream is idle, and returns every id.
    pub async fn send_all<S>(mut self, values: S) -> Result<Vec<String>>
    where
        S: futures::Stream<Item = T>,
    {
        let mut values = std::pin::pin!(values);
        loop {
            let next = match self.oldest {
                Some(oldest) => {
                    let left = self.policy.max_delay.saturating_sub(oldest.elapsed());
                    match tokio::time::timeout(left, values.next()).await {
                        Ok(next) => next,
                        Err(_) => {
                            self.flush().await?;
                            continue;
                        }
                    }
                }
                None => values.next().await,
            };
            match next {
                Some(value) => self.send(value).await?,
                None => return self.finish().await,
            }
        }
    }

    /// Whether the oldest buffered value has waited out the policy's delay.
    fn overdue(&self) -> bool {
        self.oldest
            .is_some_and(|oldest| oldest.elapsed() >= self.policy.max_delay)
    }
}

/// What a topic reports through; see [`Topic::with_telemetry`].
struct TopicTelemetry {
    observer: Observer,
    backend: &'static str,
}

impl TopicTelemetry {
    /// Starts one operation on `subject` under `parent`, or the current context.
    fn start<T, C: Codec<T>>(
        &self,
        codec: &C,
        subject: &'static str,
        operation: &'static str,
        kind: SpanKind,
        parent: Option<tpp_telemetry::Context>,
    ) -> Operation {
        self.observer.start_with(
            self.backend,
            operation,
            kind,
            Start {
                parent,
                attributes: vec![
                    KeyValue::new("messaging.destination.name", subject),
                    KeyValue::new("streams.codec", codec.name()),
                ],
                ..Start::default()
            },
        )
    }

    /// Encodes `values` as one encode operation.
    fn encode<T, C: Codec<T>>(
        &self,
        parent: Option<tpp_telemetry::Context>,
        codec: &C,
        subject: &'static str,
        values: &[T],
    ) -> Result<Vec<Payload>> {
        let mut op = self.start(codec, subject, "encode", SpanKind::Internal, parent);
        let result = values
            .iter()
            .map(|value| Payload::encode(codec, value))
            .collect::<Result<Vec<_>>>();
        if let Ok(payloads) = &result {
            op.written_rows(payloads.len());
        }
        Self::finish(op, &result);
        result
    }

    /// Decodes `message` as one decode operation when there is telemetry: the
    /// child of the provider's receive span, or of the publisher's when the
    /// provider is not instrumented.
    fn decode<T, C: Codec<T>>(telemetry: Option<&Self>, message: &Message, codec: &C) -> Result<T> {
        let Some(tel) = telemetry else {
            return message.decode(codec);
        };
        let parent = if message.traced() {
            message.context()
        } else {
            extract(&HeaderExtractor(message.headers()))
        };
        let mut op = tel.observer.start_with(
            tel.backend,
            "decode",
            SpanKind::Internal,
            Start {
                parent: Some(parent),
                attributes: vec![
                    KeyValue::new("messaging.destination.name", message.subject().to_owned()),
                    KeyValue::new("streams.codec", message.codec().to_owned()),
                ],
                ..Start::default()
            },
        );
        let result = message.decode(codec);
        if result.is_ok() {
            op.read_rows(1);
        }
        Self::finish(op, &result);
        result
    }

    fn finish<V>(op: Operation, result: &Result<V>) {
        op.finish(match result {
            Ok(_) => Outcome::Ok,
            Err(StreamError::Codec(_)) => Outcome::Invalid,
            Err(StreamError::Unsupported(_)) => Outcome::Unsupported,
            Err(_) => Outcome::Error,
        });
    }
}

struct HeaderExtractor<'a>(&'a crate::Headers);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.iter().map(|(k, _)| k).collect()
    }
}
