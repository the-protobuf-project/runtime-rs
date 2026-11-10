//! Every operation of any provider, traced, measured and logged.
//!
//! [`instrument`] wraps a provider so each call is a tpp-telemetry operation of
//! component `streams`, and the managers it binds keep every capability the
//! provider's have: [`as_durable`](crate::as_durable),
//! [`as_positioned`](crate::as_positioned) and [`as_batch`](crate::as_batch)
//! find them through the wrapper, instrumented. It is runtime-go's
//! `streams.Instrument`, span for span and header for header.

use std::{sync::Arc, time::SystemTime};

use chrono::{DateTime, SecondsFormat, Utc};
use futures::StreamExt;
use tpp_telemetry::{
    Observer, Operation, Outcome, SpanKind, Start, Telemetry, extract_into, inject,
    opentelemetry::{
        Context, KeyValue,
        metrics::Histogram,
        propagation::{Extractor, Injector},
    },
};

use crate::{
    Acker, Batch, Deliveries, Delivery, Durable, Headers, Manager, Message, Options, Payload,
    Position, Positioned, Publisher, Result, Stream, StreamError, Streams, Subscriber,
    Subscription,
};

/// Carries the time a message was handed to the broker, RFC 3339 in UTC. A
/// receiver subtracts it from its own clock for `streams.transit.duration`;
/// across machines that includes the skew between their clocks.
pub const SENT_AT_HEADER: &str = "sent-at";

/// Wraps `provider` so every operation reports through `telemetry`, as
/// `backend` ("nats", "kafka", "rabbitmq"); returns it unchanged when
/// `telemetry` carries no signal.
///
/// | Span | Kind | Covers |
/// | --- | --- | --- |
/// | `streams.publish`, `streams.publish_batch` | producer | a publish, to the broker's acknowledgement |
/// | `streams.subscribe`, `streams.consume` | client | opening a subscription or a durable consumer |
/// | `streams.receive` | consumer | one message, receipt to hand-off |
/// | `streams.deliver` | consumer | one delivery, receipt to ack or nak |
/// | `streams.create`, `get`, `update`, `delete`, `list`, `bind` | client | the provider's lifecycle |
///
/// A publish stamps the message's headers with its trace context (W3C
/// `traceparent`, `tracestate`) and [`SENT_AT_HEADER`]; the receiving side
/// continues that trace, in this program or another, in Rust or Go, and
/// [`Message::context`] carries its span to the caller. A delivery's span stays
/// open until it is settled, so it covers the handling, and an unsettled
/// delivery that is dropped reports itself cancelled.
pub fn instrument(
    provider: Arc<dyn Streams>,
    telemetry: &Telemetry,
    backend: &'static str,
) -> Arc<dyn Streams> {
    if !telemetry.is_enabled() {
        return provider;
    }
    let observer = telemetry.observer("streams");
    let transit = observer.meter().map(|m| {
        m.f64_histogram("streams.transit.duration")
            .with_description("Time from a message's sent-at header to its receipt")
            .with_unit("s")
            .build()
    });
    Arc::new(Instrumented {
        next: provider,
        ins: Arc::new(Instruments {
            observer,
            backend,
            transit,
        }),
    })
}

/// What every wrapper of one provider reports through.
struct Instruments {
    observer: Observer,
    backend: &'static str,
    transit: Option<Histogram<f64>>,
}

impl Instruments {
    fn start(
        &self,
        operation: &'static str,
        kind: SpanKind,
        attributes: Vec<KeyValue>,
    ) -> Operation {
        let mut attributes = attributes;
        attributes.push(KeyValue::new("messaging.system", self.backend));
        self.observer.start_with(
            self.backend,
            operation,
            kind,
            Start {
                attributes,
                ..Start::default()
            },
        )
    }

    /// `opts` with the trace of `op` and the send time added to its headers.
    fn stamp(&self, op: &Operation, opts: &Options) -> Options {
        let mut opts = opts.clone();
        opts.headers.insert(
            SENT_AT_HEADER,
            Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true),
        );
        inject(&op.context(), &mut HeaderInjector(&mut opts.headers));
        opts
    }

    /// Starts the span of `message` arriving through `operation`, the child of
    /// the publish its headers carry, and records its transit.
    fn received(
        &self,
        operation: &'static str,
        message: &Message,
        extra: Vec<KeyValue>,
    ) -> Operation {
        let now = SystemTime::now();
        let mut attributes = vec![
            KeyValue::new("messaging.system", self.backend),
            KeyValue::new("messaging.destination.name", message.subject().to_owned()),
            KeyValue::new("messaging.message.id", message.id().to_owned()),
            KeyValue::new("messaging.message.body.size", message.data().len() as i64),
            KeyValue::new("streams.codec", message.codec().to_owned()),
        ];
        attributes.extend(extra);
        if let Some(sent) = message
            .headers()
            .get(SENT_AT_HEADER)
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        {
            let transit = Utc::now().signed_duration_since(sent.with_timezone(&Utc));
            if let Ok(transit) = transit.to_std() {
                let seconds = transit.as_secs_f64();
                if let Some(h) = &self.transit {
                    h.record(
                        seconds,
                        &[
                            KeyValue::new("subject", message.subject().to_owned()),
                            KeyValue::new("backend", self.backend),
                        ],
                    );
                }
                attributes.push(KeyValue::new("streams.transit.duration", seconds));
            }
        }
        let parent = extract_into(Context::new(), &HeaderExtractor(message.headers()));
        self.observer.start_with(
            self.backend,
            operation,
            SpanKind::Consumer,
            Start {
                parent: Some(parent),
                time: Some(now),
                attributes,
                ..Start::default()
            },
        )
    }
}

/// The outcome of a streams result: a missing stream is an expected answer.
fn outcome<T>(result: &Result<T>) -> Outcome {
    match result {
        Ok(_) => Outcome::Ok,
        Err(StreamError::NotFound(_)) => Outcome::NotFound,
        Err(StreamError::Unsupported(_)) => Outcome::Unsupported,
        Err(StreamError::UnknownSubject { .. } | StreamError::InvalidHeader(_)) => Outcome::Invalid,
        Err(_) => Outcome::Error,
    }
}

struct HeaderInjector<'a>(&'a mut Headers);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key, value);
    }
}

struct HeaderExtractor<'a>(&'a Headers);

impl Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.iter().map(|(k, _)| k).collect()
    }
}

// --- the provider --------------------------------------------------------------

struct Instrumented {
    next: Arc<dyn Streams>,
    ins: Arc<Instruments>,
}

impl Instrumented {
    fn admin(&self, operation: &'static str, id: &str) -> Operation {
        self.ins.start(
            operation,
            SpanKind::Client,
            vec![KeyValue::new("streams.stream", id.to_owned())],
        )
    }
}

#[async_trait::async_trait]
impl Streams for Instrumented {
    async fn create(&self, stream: Stream) -> Result<Stream> {
        let op = self.admin("create", &stream.id);
        let result = self.next.create(stream).await;
        op.finish(outcome(&result));
        result
    }

    async fn get(&self, id: &str) -> Result<Stream> {
        let op = self.admin("get", id);
        let result = self.next.get(id).await;
        op.finish(outcome(&result));
        result
    }

    async fn bind(&self, id: &str) -> Result<Arc<dyn Manager>> {
        let op = self.admin("bind", id);
        let result = self.next.bind(id).await;
        op.finish(outcome(&result));
        let next = result?;
        Ok(Arc::new(InstrumentedManager {
            halves: Halves {
                next: next.clone(),
                ins: self.ins.clone(),
            },
            next,
            ins: self.ins.clone(),
        }))
    }

    async fn update(&self, id: &str, stream: Stream) -> Result<Stream> {
        let op = self.admin("update", id);
        let result = self.next.update(id, stream).await;
        op.finish(outcome(&result));
        result
    }

    async fn delete(&self, id: &str) -> Result<()> {
        let op = self.admin("delete", id);
        let result = self.next.delete(id).await;
        op.finish(outcome(&result));
        result
    }

    async fn list(&self) -> Result<Vec<Stream>> {
        let mut op = self.admin("list", "");
        let result = self.next.list().await;
        if let Ok(streams) = &result {
            op.read_rows(streams.len());
        }
        op.finish(outcome(&result));
        result
    }

    async fn close(&self) -> Result<()> {
        self.next.close().await
    }
}

// --- a bound manager -----------------------------------------------------------

struct InstrumentedManager {
    next: Arc<dyn Manager>,
    ins: Arc<Instruments>,
    halves: Halves,
}

/// The instrumented durable, positioned and batch halves, which a manager
/// hands out only when the one it wraps has them.
struct Halves {
    next: Arc<dyn Manager>,
    ins: Arc<Instruments>,
}

#[async_trait::async_trait]
impl Publisher for InstrumentedManager {
    async fn publish(&self, subject: &str, payload: Payload, opts: &Options) -> Result<String> {
        let mut op = self.ins.start(
            "publish",
            SpanKind::Producer,
            vec![KeyValue::new(
                "messaging.destination.name",
                subject.to_owned(),
            )],
        );
        let opts = self.ins.stamp(&op, opts);
        let result = self.next.publish(subject, payload, &opts).await;
        if let Ok(id) = &result {
            op.set_attribute(KeyValue::new("messaging.message.id", id.clone()));
            op.written_rows(1);
        }
        op.finish(outcome(&result));
        result
    }
}

#[async_trait::async_trait]
impl Subscriber for InstrumentedManager {
    async fn subscribe(&self, subject: &str, opts: &Options) -> Result<Subscription> {
        let op = self.ins.start(
            "subscribe",
            SpanKind::Client,
            vec![KeyValue::new(
                "messaging.destination.name",
                subject.to_owned(),
            )],
        );
        let result = self.next.subscribe(subject, opts).await;
        op.finish(outcome(&result));
        let group: Vec<KeyValue> = opts
            .group
            .iter()
            .map(|g| KeyValue::new("messaging.consumer.group.name", g.clone()))
            .collect();
        let ins = self.ins.clone();
        Ok(result?
            .map(move |message| {
                let mut op = ins.received("receive", &message, group.clone());
                op.read_rows(1);
                let message = message.with_context(op.context());
                op.finish(Outcome::Ok);
                message
            })
            .boxed())
    }
}

impl Manager for InstrumentedManager {
    fn durable(&self) -> Option<&dyn Durable> {
        self.next.durable().map(|_| &self.halves as &dyn Durable)
    }

    fn positioned(&self) -> Option<&dyn Positioned> {
        self.next
            .positioned()
            .map(|_| &self.halves as &dyn Positioned)
    }

    fn batch(&self) -> Option<&dyn Batch> {
        self.next.batch().map(|_| &self.halves as &dyn Batch)
    }
}

#[async_trait::async_trait]
impl Batch for Halves {
    async fn publish_batch(
        &self,
        subject: &str,
        payloads: Vec<Payload>,
        opts: &Options,
    ) -> Result<Vec<String>> {
        let batch = self.next.batch().ok_or_else(|| unsupported("batch"))?;
        let mut op = self.ins.start(
            "publish_batch",
            SpanKind::Producer,
            vec![
                KeyValue::new("messaging.destination.name", subject.to_owned()),
                KeyValue::new("messaging.batch.message_count", payloads.len() as i64),
            ],
        );
        let opts = self.ins.stamp(&op, opts);
        let result = batch.publish_batch(subject, payloads, &opts).await;
        if let Ok(ids) = &result {
            op.written_rows(ids.len());
        }
        op.finish(outcome(&result));
        result
    }
}

#[async_trait::async_trait]
impl Durable for Halves {
    async fn consume(&self, subject: &str, consumer: &str, opts: &Options) -> Result<Deliveries> {
        let durable = self.next.durable().ok_or_else(|| unsupported("durable"))?;
        let op = self
            .ins
            .start("consume", SpanKind::Client, consumed(subject, consumer));
        let result = durable.consume(subject, consumer, opts).await;
        op.finish(outcome(&result));
        Ok(deliveries(self.ins.clone(), consumer, result?))
    }
}

#[async_trait::async_trait]
impl Positioned for Halves {
    async fn consume_from(
        &self,
        subject: &str,
        consumer: &str,
        at: Position,
        opts: &Options,
    ) -> Result<Deliveries> {
        let positioned = self
            .next
            .positioned()
            .ok_or_else(|| unsupported("positioned"))?;
        let op = self
            .ins
            .start("consume", SpanKind::Client, consumed(subject, consumer));
        let result = positioned.consume_from(subject, consumer, at, opts).await;
        op.finish(outcome(&result));
        Ok(deliveries(self.ins.clone(), consumer, result?))
    }
}

fn consumed(subject: &str, consumer: &str) -> Vec<KeyValue> {
    vec![
        KeyValue::new("messaging.destination.name", subject.to_owned()),
        KeyValue::new("messaging.consumer.group.name", consumer.to_owned()),
    ]
}

fn unsupported(capability: &str) -> StreamError {
    StreamError::Unsupported(format!("the wrapped provider lost its {capability} half"))
}

/// Starts each delivery's span and hands it on with an acker that finishes the
/// span when the delivery is settled.
fn deliveries(ins: Arc<Instruments>, consumer: &str, deliveries: Deliveries) -> Deliveries {
    let consumer = consumer.to_owned();
    deliveries
        .map(move |delivery| {
            let (message, attempt, acker) = delivery.into_parts();
            let mut op = ins.received(
                "deliver",
                &message,
                vec![
                    KeyValue::new("messaging.consumer.group.name", consumer.clone()),
                    KeyValue::new("streams.delivery.attempt", i64::from(attempt)),
                ],
            );
            op.read_rows(1);
            let message = message.with_context(op.context());
            Delivery::new(message, attempt, Box::new(Settling { next: acker, op }))
        })
        .boxed()
}

/// An acker that finishes its delivery's span with how it was settled.
struct Settling {
    next: Box<dyn Acker>,
    op: Operation,
}

impl Settling {
    fn settle(op: Operation, how: &'static str, result: &Result<()>) {
        op.set_attribute(KeyValue::new("streams.settled", how));
        op.finish(outcome(result));
    }
}

#[async_trait::async_trait]
impl Acker for Settling {
    async fn ack(self: Box<Self>) -> Result<()> {
        let Settling { next, op } = *self;
        let result = next.ack().await;
        Self::settle(op, "ack", &result);
        result
    }

    async fn nak(self: Box<Self>) -> Result<()> {
        let Settling { next, op } = *self;
        let result = next.nak().await;
        Self::settle(op, "nak", &result);
        result
    }

    async fn nak_after(self: Box<Self>, delay: std::time::Duration) -> Result<()> {
        let Settling { next, op } = *self;
        let result = next.nak_after(delay).await;
        Self::settle(op, "nak_after", &result);
        result
    }

    fn delays_nak(&self) -> bool {
        self.next.delays_nak()
    }

    fn max_nak_delay(&self) -> Option<std::time::Duration> {
        self.next.max_nak_delay()
    }
}
