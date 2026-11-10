//! `streams::instrument` over an in-memory provider: the provider's spans, the
//! capabilities it keeps, the trace crossing the "broker", and a typed topic on
//! top. It mirrors runtime-go's streams/topic/instrument_test.go.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures::{StreamExt, stream::BoxStream};
use streams::{
    Acker, Batch, BatchPolicy, Deliveries, Delivery, Durable, Json, Manager, Message, Options,
    Payload, Publisher, Result, SENT_AT_HEADER, Stream, StreamError, Streams, Subscriber,
    Subscription, Telemetry, Topic, TopicSpec, as_batch, as_durable,
};
use tokio::sync::broadcast;
use tpp_telemetry::{SpanKind, testing::Recorder};

#[derive(Clone, PartialEq, Debug, serde::Serialize, serde::Deserialize)]
struct Order {
    order_id: String,
}

static PLACED: TopicSpec<Order> = TopicSpec {
    subject: "order.placed",
    consumer: "billing",
    group: None,
    headers: &[],
    header_fields: &[],
    partition_key: None,
    expiry: None,
    retain: false,
    prefetch: None,
    batch: BatchPolicy {
        max_size: 10,
        max_delay: Duration::from_millis(10),
    },
};

fn declaration() -> Stream {
    Stream::named("orders")
        .with_id("orders")
        .subjects(["order.placed"])
}

// --- the in-memory provider ---------------------------------------------------

#[derive(Default)]
struct Memory {
    streams: Mutex<HashMap<String, Stream>>,
    buses: Mutex<HashMap<String, Arc<Bus>>>,
    durable: bool,
    batch: bool,
}

struct Bus {
    me: Weak<Bus>,
    tx: broadcast::Sender<Message>,
    published: Mutex<Vec<Options>>,
    acks: AtomicUsize,
    durable: bool,
    batch: bool,
}

#[async_trait::async_trait]
impl Streams for Memory {
    async fn create(&self, stream: Stream) -> Result<Stream> {
        self.streams
            .lock()
            .unwrap()
            .insert(stream.id.clone(), stream.clone());
        Ok(stream)
    }

    async fn get(&self, id: &str) -> Result<Stream> {
        self.streams
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| StreamError::NotFound(id.into()))
    }

    async fn bind(&self, id: &str) -> Result<Arc<dyn Manager>> {
        self.get(id).await?;
        let bus = self
            .buses
            .lock()
            .unwrap()
            .entry(id.into())
            .or_insert_with(|| {
                Arc::new_cyclic(|me| Bus {
                    me: me.clone(),
                    tx: broadcast::channel(64).0,
                    published: Mutex::default(),
                    acks: AtomicUsize::new(0),
                    durable: self.durable,
                    batch: self.batch,
                })
            })
            .clone();
        Ok(bus)
    }

    async fn update(&self, id: &str, stream: Stream) -> Result<Stream> {
        self.streams
            .lock()
            .unwrap()
            .insert(id.into(), stream.clone());
        Ok(stream)
    }

    async fn delete(&self, id: &str) -> Result<()> {
        self.streams.lock().unwrap().remove(id);
        Ok(())
    }

    async fn list(&self) -> Result<Vec<Stream>> {
        Ok(self.streams.lock().unwrap().values().cloned().collect())
    }
}

impl Bus {
    fn receive(&self) -> BoxStream<'static, Message> {
        futures::stream::unfold(self.tx.subscribe(), |mut rx| async move {
            rx.recv().await.ok().map(|m| (m, rx))
        })
        .boxed()
    }
}

#[async_trait::async_trait]
impl Publisher for Bus {
    async fn publish(&self, subject: &str, payload: Payload, opts: &Options) -> Result<String> {
        let mut published = self.published.lock().unwrap();
        let id = format!("m{}", published.len());
        published.push(opts.clone());
        let message = Message::new(&id, subject, payload.codec(), payload.data().clone())
            .with_headers(opts.headers.clone());
        let _ = self.tx.send(message);
        Ok(id)
    }
}

#[async_trait::async_trait]
impl Subscriber for Bus {
    async fn subscribe(&self, _subject: &str, _opts: &Options) -> Result<Subscription> {
        Ok(self.receive())
    }
}

#[async_trait::async_trait]
impl Batch for Bus {
    async fn publish_batch(
        &self,
        subject: &str,
        payloads: Vec<Payload>,
        opts: &Options,
    ) -> Result<Vec<String>> {
        let mut ids = Vec::new();
        for payload in payloads {
            ids.push(self.publish(subject, payload, opts).await?);
        }
        Ok(ids)
    }
}

struct Count(Arc<Bus>);

#[async_trait::async_trait]
impl Acker for Count {
    async fn ack(self: Box<Self>) -> Result<()> {
        self.0.acks.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn nak(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

#[async_trait::async_trait]
impl Durable for Bus {
    async fn consume(
        &self,
        _subject: &str,
        _consumer: &str,
        _opts: &Options,
    ) -> Result<Deliveries> {
        let bus = self.me.upgrade().expect("a bound bus is alive");
        Ok(self
            .receive()
            .map(move |m| Delivery::new(m, 1, Box::new(Count(bus.clone()))))
            .boxed())
    }
}

impl Manager for Bus {
    fn durable(&self) -> Option<&dyn Durable> {
        if self.durable { Some(self) } else { None }
    }

    fn batch(&self) -> Option<&dyn Batch> {
        if self.batch { Some(self) } else { None }
    }
}

async fn bound(provider: &Arc<dyn Streams>) -> Arc<dyn Manager> {
    provider.create(declaration()).await.unwrap();
    provider.bind("orders").await.unwrap()
}

fn instrumented(memory: Memory) -> (Arc<dyn Streams>, Recorder) {
    let rec = Recorder::new();
    let provider: Arc<dyn Streams> = Arc::new(memory);
    (streams::instrument(provider, &rec.signals(), "memory"), rec)
}

fn payload(id: &str) -> Payload {
    Payload::encode(
        &Json,
        &Order {
            order_id: id.into(),
        },
    )
    .unwrap()
}

// --- tests ---------------------------------------------------------------------

#[test]
fn telemetry_without_a_signal_leaves_the_provider_alone() {
    let provider: Arc<dyn Streams> = Arc::new(Memory::default());
    let same = streams::instrument(provider.clone(), &Telemetry::default(), "memory");
    assert!(Arc::ptr_eq(&provider, &same));
}

#[tokio::test]
async fn an_instrumented_publish_is_the_parent_of_its_receive() {
    let (provider, rec) = instrumented(Memory::default());
    let mgr = bound(&provider).await;
    let mut messages = mgr
        .subscribe("order.placed", &Options::default())
        .await
        .unwrap();
    mgr.publish("order.placed", payload("o-1"), &Options::default())
        .await
        .unwrap();
    let message = messages.next().await.unwrap();

    assert!(message.traced());
    assert!(message.headers().get("traceparent").is_some());
    assert!(message.headers().get(SENT_AT_HEADER).is_some());

    let publish = rec.span("streams.publish").expect("publish span");
    let receive = rec.span("streams.receive").expect("receive span");
    assert_eq!(publish.kind, SpanKind::Producer);
    assert_eq!(receive.kind, SpanKind::Consumer);
    assert_eq!(receive.parent_span_id, publish.span_id);
    assert_eq!(receive.trace_id, publish.trace_id);
    assert!(rec.span("streams.bind").is_some() && rec.span("streams.subscribe").is_some());

    let outcomes = rec.outcomes();
    assert_eq!(outcomes["publish"], "ok");
    assert_eq!(outcomes["receive"], "ok");
    assert!(
        rec.measurements()
            .iter()
            .any(|m| m.name == "streams.transit.duration")
    );
}

#[tokio::test]
async fn an_instrumented_provider_keeps_its_capabilities() {
    let (provider, rec) = instrumented(Memory {
        durable: true,
        batch: true,
        ..Memory::default()
    });
    let mgr = bound(&provider).await;
    assert!(as_durable(&*mgr).is_ok());
    let batch = as_batch(&*mgr).unwrap();
    batch
        .publish_batch(
            "order.placed",
            vec![payload("a"), payload("b")],
            &Options::default(),
        )
        .await
        .unwrap();
    let span = rec
        .span("streams.publish_batch")
        .expect("publish_batch span");
    assert_eq!(span.attributes["messaging.batch.message_count"], "2");

    let (plain, _) = instrumented(Memory::default());
    let plain = bound(&plain).await;
    assert!(matches!(
        as_durable(&*plain),
        Err(StreamError::Unsupported(_))
    ));
}

#[tokio::test]
async fn a_delivery_span_ends_when_it_is_settled() {
    let (provider, rec) = instrumented(Memory {
        durable: true,
        ..Memory::default()
    });
    let mgr = bound(&provider).await;
    let mut deliveries = as_durable(&*mgr)
        .unwrap()
        .consume("order.placed", "billing", &Options::default())
        .await
        .unwrap();
    mgr.publish("order.placed", payload("o-1"), &Options::default())
        .await
        .unwrap();
    let delivery = deliveries.next().await.unwrap();
    assert!(
        rec.span("streams.deliver").is_none(),
        "deliver ended before the ack"
    );

    delivery.ack().await.unwrap();
    let deliver = rec.span("streams.deliver").expect("deliver span");
    assert_eq!(deliver.attributes["streams.settled"], "ack");
    assert_eq!(
        deliver.parent_span_id,
        rec.span("streams.publish").unwrap().span_id
    );
}

#[tokio::test]
async fn a_topic_over_an_instrumented_provider_traces_each_step_once() {
    let (provider, rec) = instrumented(Memory {
        durable: true,
        ..Memory::default()
    });
    let mgr = bound(&provider).await;
    let orders: Topic<Order, Json> =
        Topic::new(mgr, Json, &PLACED).with_telemetry(&rec.signals(), "memory");

    let mut deliveries = orders.consumer().consume("billing").await.unwrap();
    orders
        .publisher()
        .publish(&Order {
            order_id: "o-1".into(),
        })
        .await
        .unwrap();
    let delivery = deliveries.next().await.unwrap();
    assert_eq!(delivery.message().order_id, "o-1");
    delivery.ack().await.unwrap();

    let send = rec.span("streams.send").expect("send");
    let encode = rec.span("streams.encode").expect("encode");
    let publish = rec.span("streams.publish").expect("publish");
    let deliver = rec.span("streams.deliver").expect("deliver");
    let decode = rec.span("streams.decode").expect("decode");
    assert_eq!(encode.parent_span_id, send.span_id);
    assert_eq!(publish.parent_span_id, send.span_id);
    assert_eq!(deliver.parent_span_id, publish.span_id);
    assert_eq!(decode.parent_span_id, deliver.span_id);
}

#[tokio::test]
async fn a_missing_stream_is_not_a_failure() {
    let (provider, rec) = instrumented(Memory::default());
    assert!(matches!(
        provider.get("nope").await,
        Err(StreamError::NotFound(_))
    ));
    let get = rec.span("streams.get").expect("get span");
    assert!(!get.failed);
    assert_eq!(rec.outcomes()["get"], "not_found");
}
