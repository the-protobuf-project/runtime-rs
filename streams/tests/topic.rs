//! Typed topics over an in-memory provider: bind, publish, subscribe, consume,
//! batch, and the codec and option plumbing in between.
//!
//! The provider is a broadcast bus per stream that records every publish's
//! options, so a test can see what a topic's spec put on the wire without a
//! broker. Its capabilities are switches, so each test picks the provider it
//! needs: durable or not, native batch or not.

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
    Acker, Batch, BatchPolicy, BatchSink, Capability, Deliveries, Delivery, Durable, Json, Manager,
    Message, Options, Payload, Position, Publisher, Result, Stream, StreamError, Streams,
    Subscriber, Subscription, Topic, TopicSpec, as_positioned, topic,
};
use tokio::sync::broadcast;

// --- messages ---------------------------------------------------------------

/// A prost message that is also serde, so one type covers both codecs.
#[derive(Clone, PartialEq, prost::Message, serde::Serialize, serde::Deserialize)]
struct Order {
    #[prost(string, tag = "1")]
    order_id: String,
    #[prost(message, optional, tag = "2")]
    customer: Option<Customer>,
    #[prost(int32, tag = "3")]
    quantity: i32,
}

#[derive(Clone, PartialEq, prost::Message, serde::Serialize, serde::Deserialize)]
struct Customer {
    #[prost(string, tag = "1")]
    tenant: String,
}

fn order(id: &str, tenant: &str) -> Order {
    Order {
        order_id: id.into(),
        customer: Some(Customer {
            tenant: tenant.into(),
        }),
        quantity: 1,
    }
}

// --- specs, as protoc-gen-streams writes them --------------------------------

fn order_id(m: &Order) -> Option<String> {
    Some(m.order_id.clone())
}

fn tenant(m: &Order) -> Option<String> {
    Some(m.customer.as_ref()?.tenant.clone())
}

fn quantity(m: &Order) -> Option<String> {
    Some(m.quantity.to_string())
}

const BATCH: BatchPolicy = BatchPolicy {
    max_size: 100,
    max_delay: Duration::from_millis(10),
};

static PLACED: TopicSpec<Order> = TopicSpec {
    subject: "order.placed",
    consumer: "billing",
    group: Some("checkout"),
    headers: &[("source", "checkout")],
    header_fields: &[("tenant", tenant as topic::FieldFn<_>)],
    partition_key: Some(order_id),
    expiry: Some(Duration::from_secs(30)),
    retain: false,
    prefetch: Some(8),
    batch: BATCH,
};

static SHIPPED: TopicSpec<Order> = TopicSpec {
    subject: "order.shipped",
    consumer: "shipping",
    group: None,
    headers: &[],
    header_fields: &[("quantity", quantity as topic::FieldFn<_>)],
    partition_key: Some(tenant),
    expiry: None,
    retain: false,
    prefetch: None,
    batch: BATCH,
};

fn declaration() -> Stream {
    Stream::named("orders")
        .with_id("orders")
        .subjects(["order.placed", "order.shipped"])
        .header("team", "commerce")
}

// --- the in-memory provider ---------------------------------------------------

/// Streams in memory, with one broadcast bus per bound stream.
#[derive(Default)]
struct Memory {
    streams: Mutex<HashMap<String, Stream>>,
    buses: Mutex<HashMap<String, Arc<Bus>>>,
    updates: AtomicUsize,
    durable: bool,
    batch: bool,
}

impl Memory {
    fn durable() -> Self {
        Self {
            durable: true,
            ..Self::default()
        }
    }

    fn batching() -> Self {
        Self {
            batch: true,
            ..Self::default()
        }
    }

    fn bus(&self, id: &str) -> Arc<Bus> {
        self.buses.lock().unwrap()[id].clone()
    }
}

/// One bound stream: its declaration, a bus, and a record of every publish.
struct Bus {
    /// Itself, for the ackers a durable consumer hands out.
    me: Weak<Bus>,
    stream: Stream,
    tx: broadcast::Sender<Message>,
    published: Mutex<Vec<Options>>,
    batches: Mutex<Vec<usize>>,
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
        let stream = self.get(id).await?;
        let bus = self
            .buses
            .lock()
            .unwrap()
            .entry(id.into())
            .or_insert_with(|| {
                Arc::new_cyclic(|me| Bus {
                    me: me.clone(),
                    stream,
                    tx: broadcast::channel(64).0,
                    published: Mutex::default(),
                    batches: Mutex::default(),
                    acks: AtomicUsize::new(0),
                    durable: self.durable,
                    batch: self.batch,
                })
            })
            .clone();
        Ok(bus)
    }

    async fn update(&self, id: &str, stream: Stream) -> Result<Stream> {
        self.updates.fetch_add(1, Ordering::SeqCst);
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
    fn check(&self, subject: &str) -> Result<()> {
        if self.stream.subjects.iter().any(|s| s == subject) {
            return Ok(());
        }
        Err(StreamError::UnknownSubject {
            stream: self.stream.id.clone(),
            subject: subject.into(),
            declared: self.stream.subjects.clone(),
        })
    }

    fn receive(&self, subject: &str) -> BoxStream<'static, Message> {
        let subject = subject.to_owned();
        futures::stream::unfold(self.tx.subscribe(), |mut rx| async move {
            loop {
                match rx.recv().await {
                    Ok(m) => return Some((m, rx)),
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        })
        .filter(move |m| std::future::ready(m.subject() == subject))
        .boxed()
    }

    fn published(&self) -> Vec<Options> {
        self.published.lock().unwrap().clone()
    }

    fn batches(&self) -> Vec<usize> {
        self.batches.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl Publisher for Bus {
    async fn publish(&self, subject: &str, payload: Payload, opts: &Options) -> Result<String> {
        self.check(subject)?;
        let mut published = self.published.lock().unwrap();
        let id = format!("m{}", published.len());
        published.push(opts.clone());
        let message = Message::new(&id, subject, payload.codec(), payload.data().clone())
            .with_headers(self.stream.headers.merged(&opts.headers));
        let _ = self.tx.send(message);
        Ok(id)
    }
}

#[async_trait::async_trait]
impl Subscriber for Bus {
    async fn subscribe(&self, subject: &str, _opts: &Options) -> Result<Subscription> {
        self.check(subject)?;
        Ok(self.receive(subject))
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
        self.batches.lock().unwrap().push(payloads.len());
        let mut ids = Vec::with_capacity(payloads.len());
        for payload in payloads {
            ids.push(self.publish(subject, payload, opts).await?);
        }
        Ok(ids)
    }
}

/// Counts acks on its bus; naks settle nothing.
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
    /// Every delivery is its first; its acker counts on this bus.
    async fn consume(&self, subject: &str, _consumer: &str, _opts: &Options) -> Result<Deliveries> {
        self.check(subject)?;
        let bus = self.me.upgrade().expect("a bound bus is alive");
        Ok(self
            .receive(subject)
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

// --- round trips ---------------------------------------------------------------

#[tokio::test]
async fn publish_carries_the_specs_key_and_headers() {
    let provider = Memory::default();
    let manager = topic::bind(&provider, declaration(), &[]).await.unwrap();
    let placed: Topic<Order> = Topic::new(manager, streams::Protobuf, &PLACED);
    assert_eq!(placed.subject(), "order.placed");

    let mut live = placed.subscriber().subscribe().await.unwrap();
    let sent = order("o-1", "acme");
    placed.publisher().publish(&sent).await.unwrap();
    assert_eq!(live.next().await.unwrap(), sent);

    let opts = &provider.bus("orders").published()[0];
    assert_eq!(opts.partition_key.as_deref(), Some("o-1"));
    assert_eq!(opts.headers.get("source"), Some("checkout"));
    assert_eq!(opts.headers.get("tenant"), Some("acme"));
    assert_eq!(opts.expires, Some(Duration::from_secs(30)));

    // A publish's own options win over the spec's.
    let over = Options::default()
        .with_partition_key("pinned")
        .header("source", "admin");
    placed.publisher().publish_with(&sent, over).await.unwrap();
    let opts = &provider.bus("orders").published()[1];
    assert_eq!(opts.partition_key.as_deref(), Some("pinned"));
    assert_eq!(opts.headers.get("source"), Some("admin"));
    assert_eq!(opts.headers.get("tenant"), Some("acme"));
}

#[tokio::test]
async fn a_field_on_an_unset_message_leaves_its_header_off() {
    let provider = Memory::default();
    let manager = topic::bind(&provider, declaration(), &[]).await.unwrap();
    let placed: Topic<Order> = Topic::new(manager, streams::Protobuf, &PLACED);
    let anonymous = Order {
        order_id: "o-2".into(),
        ..Order::default()
    };
    placed.publisher().publish(&anonymous).await.unwrap();
    let opts = &provider.bus("orders").published()[0];
    assert!(!opts.headers.contains_key("tenant"));
    assert_eq!(opts.headers.get("source"), Some("checkout"));
}

#[tokio::test]
async fn bind_fails_without_a_required_capability() {
    let err = topic::bind(&Memory::default(), declaration(), &[Capability::Durable])
        .await
        .err()
        .expect("a plain provider has no durable consumers");
    assert!(matches!(err, StreamError::Unsupported(_)), "{err}");
}

#[tokio::test]
async fn json_consumer_delivers_at_least_once() {
    let provider = Memory::durable();
    let manager = topic::bind(&provider, declaration(), &[Capability::Durable])
        .await
        .unwrap();
    let placed: Topic<Order, Json> = Topic::new(manager, Json, &PLACED);
    let consumer = placed.consumer();
    assert_eq!(consumer.name(), "billing");
    let mut deliveries = consumer.consume(consumer.name()).await.unwrap();

    let sent = order("o-3", "acme");
    placed.publisher().publish(&sent).await.unwrap();
    let delivery = deliveries.next().await.unwrap();
    assert_eq!(delivery.raw().codec(), "json");
    assert!(
        std::str::from_utf8(delivery.raw().data())
            .unwrap()
            .contains("\"o-3\"")
    );
    assert_eq!(delivery.raw().headers().get("team"), Some("commerce"));
    assert_eq!(delivery.raw().headers().get("tenant"), Some("acme"));
    assert_eq!(delivery.attempt(), 1);
    assert_eq!(delivery.message(), &sent);
    delivery.ack().await.unwrap();
    assert_eq!(provider.bus("orders").acks.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn consume_is_unsupported_on_a_plain_provider() {
    let provider = Memory::default();
    let manager = topic::bind(&provider, declaration(), &[]).await.unwrap();
    let placed: Topic<Order> = Topic::new(manager.clone(), streams::Protobuf, &PLACED);

    let err = placed.consumer().consume("x").await.err().unwrap();
    assert!(matches!(err, StreamError::Unsupported(_)), "{err}");
    let err = placed
        .consumer()
        .consume_from("x", Position::Earliest)
        .await
        .err()
        .unwrap();
    assert_eq!(
        err.to_string(),
        as_positioned(&*manager).err().unwrap().to_string()
    );
}

#[tokio::test]
async fn batch_sink_falls_back_to_sequential_publishes() {
    let provider = Memory::default();
    let manager = topic::bind(&provider, declaration(), &[]).await.unwrap();
    let shipped: Topic<Order> = Topic::new(manager, streams::Protobuf, &SHIPPED);
    let mut live = shipped.subscriber().subscribe().await.unwrap();

    let value = |n: i32| Order {
        quantity: n,
        ..order(
            &format!("o-{n}"),
            if n % 2 == 0 { "acme" } else { "globex" },
        )
    };
    let values = futures::stream::iter((0..5).map(value));
    let ids = BatchSink::new(shipped.publisher())
        .send_all(values)
        .await
        .unwrap();
    assert_eq!(ids, ["m0", "m1", "m2", "m3", "m4"]);
    for n in 0..5 {
        assert_eq!(live.next().await.unwrap(), value(n));
    }

    let bus = provider.bus("orders");
    assert!(bus.batches().is_empty(), "the provider has no native batch");
    let published = bus.published();
    assert_eq!(published[3].headers.get("quantity"), Some("3"));
    assert_eq!(published[3].partition_key.as_deref(), Some("globex"));
}

#[tokio::test]
async fn undecodable_messages_are_skipped_and_acknowledged() {
    let provider = Memory::durable();
    let manager = topic::bind(&provider, declaration(), &[]).await.unwrap();
    let placed: Topic<Order> = Topic::new(manager.clone(), streams::Protobuf, &PLACED);
    let mut live = placed.subscriber().subscribe().await.unwrap();
    let mut deliveries = placed.consumer().consume("billing").await.unwrap();

    // Not a protobuf Order: field 1 claims more bytes than there are.
    let junk = Payload::from_parts("protobuf", vec![0x0a, 0xff]);
    manager
        .publish("order.placed", junk, &Options::default())
        .await
        .unwrap();
    let sent = order("o-4", "acme");
    placed.publisher().publish(&sent).await.unwrap();

    assert_eq!(live.next().await.unwrap(), sent);
    let delivery = deliveries.next().await.unwrap();
    assert_eq!(delivery.message(), &sent);
    assert_eq!(
        provider.bus("orders").acks.load(Ordering::SeqCst),
        1,
        "the junk was acknowledged so it is not redelivered forever"
    );
    let (value, raw) = delivery.into_parts();
    assert_eq!(value, sent);
    assert_eq!(raw.id(), "m1");
}

// --- bind ----------------------------------------------------------------------

#[tokio::test]
async fn bind_creates_a_stream_that_does_not_exist() {
    let provider = Memory::default();
    topic::bind(&provider, declaration(), &[]).await.unwrap();
    let declared = provider.get("orders").await.unwrap();
    assert_eq!(declared.subjects, ["order.placed", "order.shipped"]);
    assert_eq!(declared.headers.get("team"), Some("commerce"));
    assert_eq!(provider.updates.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn bind_adds_missing_subjects_and_removes_none() {
    let provider = Memory::default();
    provider
        .create(
            Stream::named("orders")
                .with_id("orders")
                .subjects(["order.cancelled", "order.placed"])
                .header("owner", "ops"),
        )
        .await
        .unwrap();

    topic::bind(&provider, declaration(), &[]).await.unwrap();
    let declared = provider.get("orders").await.unwrap();
    assert_eq!(
        declared.subjects,
        ["order.cancelled", "order.placed", "order.shipped"]
    );
    assert_eq!(declared.headers.get("owner"), Some("ops"));
    assert_eq!(provider.updates.load(Ordering::SeqCst), 1);

    // A second bind finds nothing missing and leaves the stream alone.
    topic::bind(&provider, declaration(), &[]).await.unwrap();
    assert_eq!(provider.updates.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn bind_checks_every_required_capability() {
    let provider = Memory::durable();
    topic::bind(&provider, declaration(), &[Capability::Durable])
        .await
        .unwrap();
    let err = topic::bind(
        &provider,
        declaration(),
        &[Capability::Durable, Capability::Batch],
    )
    .await
    .err()
    .expect("the provider has no native batch");
    assert_eq!(
        err.to_string(),
        streams::as_batch(&*provider.bind("orders").await.unwrap())
            .err()
            .unwrap()
            .to_string()
    );
    streams::require(
        &*provider.bind("orders").await.unwrap(),
        &[Capability::Durable],
    )
    .unwrap();
}

// --- batching ------------------------------------------------------------------

async fn shipped(provider: &Memory) -> Topic<Order> {
    let manager = topic::bind(provider, declaration(), &[]).await.unwrap();
    Topic::new(manager, streams::Protobuf, &SHIPPED)
}

#[tokio::test]
async fn batch_sink_flushes_on_size() {
    let provider = Memory::batching();
    let policy = BatchPolicy {
        max_size: 2,
        max_delay: Duration::from_secs(3600),
    };
    let mut sink = BatchSink::with_policy(shipped(&provider).await.publisher(), policy);
    for n in 0..5 {
        sink.send(order(&format!("o-{n}"), "acme")).await.unwrap();
    }
    assert_eq!(provider.bus("orders").batches(), [2, 2]);
    let ids = sink.finish().await.unwrap();
    assert_eq!(ids, ["m0", "m1", "m2", "m3", "m4"]);
    assert_eq!(provider.bus("orders").batches(), [2, 2, 1]);
}

#[tokio::test(start_paused = true)]
async fn batch_sink_flushes_on_delay() {
    let provider = Memory::batching();
    let policy = BatchPolicy {
        max_size: 100,
        max_delay: Duration::from_millis(10),
    };
    let mut sink = BatchSink::with_policy(shipped(&provider).await.publisher(), policy);
    sink.send(order("o-0", "acme")).await.unwrap();
    assert!(provider.bus("orders").batches().is_empty());
    tokio::time::advance(Duration::from_millis(11)).await;
    sink.send(order("o-1", "acme")).await.unwrap();
    assert_eq!(provider.bus("orders").batches(), [2]);

    // send_all flushes while its source is idle, not only when it yields.
    let bus = provider.bus("orders");
    let values =
        futures::stream::iter([order("o-2", "acme")]).chain(futures::stream::once(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert_eq!(bus.batches(), [2, 1], "o-2 went out while the source slept");
            order("o-3", "acme")
        }));
    let sink = BatchSink::with_policy(shipped(&provider).await.publisher(), policy);
    let ids = sink.send_all(values).await.unwrap();
    assert_eq!(ids, ["m2", "m3"]);
    assert_eq!(provider.bus("orders").batches(), [2, 1, 1]);
}

#[tokio::test]
async fn publish_batch_uses_the_native_batch_one_run_per_key() {
    let provider = Memory::batching();
    let publisher = shipped(&provider).await.publisher();
    // SHIPPED keys by tenant and stamps the quantity, so equal quantities and
    // tenants share options and alternating tenants split the batch.
    let ids = publisher
        .publish_batch(&[
            order("a", "acme"),
            order("b", "acme"),
            order("c", "globex"),
            order("d", "acme"),
        ])
        .await
        .unwrap();
    assert_eq!(ids, ["m0", "m1", "m2", "m3"]);
    assert_eq!(provider.bus("orders").batches(), [2, 1, 1]);
    let keys: Vec<_> = provider
        .bus("orders")
        .published()
        .into_iter()
        .map(|opts| opts.partition_key)
        .collect();
    assert_eq!(
        keys,
        [
            Some("acme".into()),
            Some("acme".into()),
            Some("globex".into()),
            Some("acme".into())
        ]
    );
}
