//! The RabbitMQ provider against a real broker, ported from runtime-go's
//! `streams/rabbitmq/rabbitmq_test.go`.
//!
//! RabbitMQ has no embeddable server, so these need one running:
//!
//! ```text
//! docker compose -f streams/docker/compose.rabbitmq.yaml up -d --wait
//! cargo test -p tpp-streams --features rabbitmq,live-tests --test rabbitmq_live
//! ```
//!
//! `RABBITMQ_TEST_URL` points them elsewhere. Each test declares its streams
//! under a prefix of its own, so tests never see each other's exchanges or
//! queues.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures::StreamExt;
use lapin::{Connection, ConnectionProperties};
use serde::{Deserialize, Serialize};
use streams::{
    Delivery, Manager, Options, Payload, Stream, StreamError, Streams, as_batch, as_durable,
    as_positioned,
    drivers::rabbitmq::{RabbitConfig, RabbitProvider},
};

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
struct Event {
    user: String,
    action: String,
}

const SUBJECT: &str = "user.created";
/// How long a test waits for a delivery before calling it lost.
const WAIT: Duration = Duration::from_secs(10);

fn url() -> String {
    std::env::var("RABBITMQ_TEST_URL")
        .unwrap_or_else(|_| "amqp://guest:guest@127.0.0.1:15682/%2f".into())
}

/// Keeps one test's exchanges and queues away from another's.
fn prefix() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("t{nanos}-{}", SEQ.fetch_add(1, Ordering::Relaxed))
}

fn event(user: &str, action: &str) -> Payload {
    Payload::json(&Event {
        user: user.into(),
        action: action.into(),
    })
    .unwrap()
}

/// A provider on a fresh connection the test owns, as an application would.
async fn provider() -> (Arc<Connection>, RabbitProvider) {
    provider_under(&prefix()).await
}

/// [`provider`] under a prefix the caller chose, to find its names on the broker.
async fn provider_under(prefix: &str) -> (Arc<Connection>, RabbitProvider) {
    let conn = Connection::connect(&url(), ConnectionProperties::default())
        .await
        .unwrap_or_else(|e| {
            panic!(
                "no RabbitMQ at {}: {e}; see docker/compose.rabbitmq.yaml",
                url()
            )
        });
    let conn = Arc::new(conn);
    let provider = RabbitProvider::new(conn.clone(), RabbitConfig::default().with_prefix(prefix))
        .await
        .unwrap();
    (conn, provider)
}

async fn declare(subjects: &[&str]) -> (RabbitProvider, Arc<dyn Manager>, Arc<Connection>) {
    let (conn, provider) = provider().await;
    let stream = provider
        .create(Stream {
            name: "test".into(),
            subjects: subjects.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        })
        .await
        .unwrap();
    let manager = provider.bind(&stream.id).await.unwrap();
    (provider, manager, conn)
}

async fn next<S: futures::Stream + Unpin>(stream: &mut S) -> S::Item {
    tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("nothing was delivered in time")
        .expect("the stream ended before a message arrived")
}

#[tokio::test]
async fn lifecycle() {
    let (_conn, s) = provider().await;

    let created = s
        .create(Stream {
            name: "users".into(),
            description: "before".into(),
            subjects: vec![SUBJECT.into()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(!created.id.is_empty(), "create did not assign an id");
    assert!(created.active);

    s.update(
        &created.id,
        Stream {
            name: "users".into(),
            description: "after".into(),
            subjects: vec![SUBJECT.into()],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(s.get(&created.id).await.unwrap().description, "after");
    assert_eq!(s.list().await.unwrap().len(), 1);

    s.delete(&created.id).await.unwrap();
    assert!(matches!(
        s.get(&created.id).await,
        Err(StreamError::NotFound(_))
    ));
    assert!(matches!(
        s.delete(&created.id).await,
        Err(StreamError::NotFound(_))
    ));
    s.close().await.unwrap();
    // The connection was the test's and is still open.
}

#[tokio::test]
async fn publish_and_subscribe() {
    let (_p, m, _c) = declare(&[SUBJECT]).await;
    let mut sub = m.subscribe(SUBJECT, &Options::default()).await.unwrap();

    let id = m
        .publish(SUBJECT, event("ada", "created"), &Options::default())
        .await
        .unwrap();
    let msg = next(&mut sub).await;
    assert_eq!(msg.id(), id);
    assert_eq!(msg.subject(), SUBJECT);
    assert_eq!(msg.json::<Event>().unwrap().user, "ada");
}

#[tokio::test]
async fn publish_keeps_a_chosen_id() {
    let (_p, m, _c) = declare(&[SUBJECT]).await;
    let mut sub = m.subscribe(SUBJECT, &Options::default()).await.unwrap();
    let id = m
        .publish(
            SUBJECT,
            event("ada", "x"),
            &Options::default().with_id("mine-1"),
        )
        .await
        .unwrap();
    assert_eq!(id, "mine-1");
    assert_eq!(next(&mut sub).await.id(), "mine-1");
}

/// A durable queue but not a log.
#[tokio::test]
async fn durable_and_batch_but_not_positioned() {
    let (_p, m, _c) = declare(&[SUBJECT]).await;
    assert!(as_durable(m.as_ref()).is_ok());
    assert!(as_batch(m.as_ref()).is_ok());
    assert!(matches!(
        as_positioned(m.as_ref()),
        Err(StreamError::Unsupported(_))
    ));
}

#[tokio::test]
async fn consume_delivers_and_acknowledges() {
    let (_p, m, _c) = declare(&[SUBJECT]).await;
    let durable = as_durable(m.as_ref()).unwrap();
    let mut deliveries = durable
        .consume(SUBJECT, "billing", &Options::default())
        .await
        .unwrap();

    m.publish(SUBJECT, event("grace", "created"), &Options::default())
        .await
        .unwrap();
    let got = next(&mut deliveries).await;
    assert_eq!(got.attempt(), 1);
    assert_eq!(got.json::<Event>().unwrap().user, "grace");
    got.ack().await.unwrap();
}

/// RabbitMQ has a true negative acknowledgement, so nak returns a message
/// immediately rather than after a timeout.
#[tokio::test]
async fn nak_returns_the_message_immediately() {
    let (_p, m, _c) = declare(&[SUBJECT]).await;
    let durable = as_durable(m.as_ref()).unwrap();
    let mut deliveries = durable
        .consume(SUBJECT, "billing", &Options::default())
        .await
        .unwrap();
    m.publish(SUBJECT, event("ada", ""), &Options::default())
        .await
        .unwrap();

    let first = next(&mut deliveries).await;
    let first_id = first.id().to_owned();
    first.nak().await.unwrap();

    let second: Delivery = tokio::time::timeout(Duration::from_secs(5), deliveries.next())
        .await
        .expect("a naked message was not handed straight back")
        .unwrap();
    assert_eq!(second.id(), first_id);
    assert!(second.attempt() >= 2, "attempt {}", second.attempt());
    second.ack().await.unwrap();
}

/// The queue is the broker's, so it holds messages while the consumer is away.
#[tokio::test]
async fn a_queue_keeps_messages_while_the_consumer_is_away() {
    let (_p, m, _c) = declare(&[SUBJECT]).await;
    let durable = as_durable(m.as_ref()).unwrap();

    // Attach once so the queue and its binding exist.
    let mut first = durable
        .consume(SUBJECT, "billing", &Options::default())
        .await
        .unwrap();
    m.publish(SUBJECT, event("first", ""), &Options::default())
        .await
        .unwrap();
    next(&mut first).await.ack().await.unwrap();
    drop(first);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Published with nobody consuming. The queue holds it.
    m.publish(SUBJECT, event("second", ""), &Options::default())
        .await
        .unwrap();

    let mut again = durable
        .consume(SUBJECT, "billing", &Options::default())
        .await
        .unwrap();
    let resumed = next(&mut again).await;
    assert_eq!(resumed.json::<Event>().unwrap().user, "second");
    resumed.ack().await.unwrap();
}

/// Dropping an unsettled delivery's stream returns the message to the queue.
#[tokio::test]
async fn dropping_the_consumer_returns_unsettled_messages() {
    let (_p, m, _c) = declare(&[SUBJECT]).await;
    let durable = as_durable(m.as_ref()).unwrap();

    let mut first = durable
        .consume(SUBJECT, "billing", &Options::default())
        .await
        .unwrap();
    m.publish(SUBJECT, event("ada", ""), &Options::default())
        .await
        .unwrap();
    let unsettled = next(&mut first).await;
    let id = unsettled.id().to_owned();
    drop(unsettled);
    drop(first);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut again = durable
        .consume(SUBJECT, "billing", &Options::default())
        .await
        .unwrap();
    let back = next(&mut again).await;
    assert_eq!(back.id(), id);
    assert!(back.attempt() >= 2, "attempt {}", back.attempt());
    back.ack().await.unwrap();
}

/// Two consumers under one name take from one queue, so the work is split
/// rather than doubled.
#[tokio::test]
async fn one_name_splits_the_work() {
    let (_p, m, _c) = declare(&[SUBJECT]).await;
    let durable = as_durable(m.as_ref()).unwrap();
    let first = durable
        .consume(SUBJECT, "shared", &Options::default())
        .await
        .unwrap();
    let second = durable
        .consume(SUBJECT, "shared", &Options::default())
        .await
        .unwrap();

    const TOTAL: usize = 6;
    for i in 0..TOTAL {
        m.publish(SUBJECT, event("u", &i.to_string()), &Options::default())
            .await
            .unwrap();
    }

    let mut both = futures::stream::select(first, second);
    let mut seen = std::collections::BTreeSet::new();
    while seen.len() < TOTAL {
        let d = next(&mut both).await;
        seen.insert(d.id().to_owned());
        d.ack().await.unwrap();
    }
    // Each message went to exactly one of them: nothing else is waiting.
    assert!(
        tokio::time::timeout(Duration::from_millis(500), both.next())
            .await
            .is_err()
    );
}

/// A stream may declare a binding pattern; a message still needs a concrete
/// key.
#[tokio::test]
async fn wildcard_subjects() {
    let (_p, m, _c) = declare(&["user.*", "audit.#"]).await;
    let mut sub = m.subscribe("user.*", &Options::default()).await.unwrap();
    let mut audit = m.subscribe("audit.#", &Options::default()).await.unwrap();

    m.publish("user.created", event("ada", ""), &Options::default())
        .await
        .unwrap();
    assert_eq!(next(&mut sub).await.subject(), "user.created");

    // AMQP's '#' takes zero words, so the bare root is covered too.
    m.publish("audit", event("ada", ""), &Options::default())
        .await
        .unwrap();
    assert_eq!(next(&mut audit).await.subject(), "audit");

    assert!(matches!(
        m.publish("user.*", event("", ""), &Options::default())
            .await,
        Err(StreamError::UnknownSubject { .. })
    ));
}

#[tokio::test]
async fn dropping_a_subscription_releases_its_queue() {
    let (_p, m, _c) = declare(&[SUBJECT]).await;
    let sub = m.subscribe(SUBJECT, &Options::default()).await.unwrap();
    drop(sub);
    tokio::time::sleep(Duration::from_millis(500)).await;
    // With the auto-delete queue gone the exchange routes to nobody, and a
    // publish is still accepted: AMQP drops unroutable messages by default.
    m.publish(SUBJECT, event("ada", ""), &Options::default())
        .await
        .unwrap();
}

#[tokio::test]
async fn batch_publishes_in_order() {
    let (_p, m, _c) = declare(&[SUBJECT]).await;
    let mut sub = m.subscribe(SUBJECT, &Options::default()).await.unwrap();
    let ids = as_batch(m.as_ref())
        .unwrap()
        .publish_batch(
            SUBJECT,
            vec![event("a", ""), event("b", ""), event("c", "")],
            &Options::default(),
        )
        .await
        .unwrap();
    assert_eq!(ids.len(), 3);
    for id in &ids {
        assert_eq!(next(&mut sub).await.id(), id);
    }
    assert!(matches!(
        as_batch(m.as_ref())
            .unwrap()
            .publish_batch(
                SUBJECT,
                vec![event("a", "")],
                &Options::default().with_id("x")
            )
            .await,
        Err(StreamError::Unsupported(_))
    ));
}

#[tokio::test]
async fn refusals() {
    let (_p, m, _c) = declare(&[SUBJECT]).await;
    let durable = as_durable(m.as_ref()).unwrap();

    assert!(matches!(
        m.publish("typo", event("", ""), &Options::default()).await,
        Err(StreamError::UnknownSubject { .. })
    ));
    assert!(matches!(
        m.subscribe("typo", &Options::default()).await,
        Err(StreamError::UnknownSubject { .. })
    ));
    assert!(matches!(
        m.publish(
            SUBJECT,
            event("", ""),
            &Options::default().with_ttl(Duration::from_secs(1))
        )
        .await,
        Err(StreamError::Unsupported(_))
    ));
    assert!(matches!(
        m.subscribe(SUBJECT, &Options::default().with_group("g"))
            .await,
        Err(StreamError::Unsupported(_))
    ));
    assert!(matches!(
        durable
            .consume(SUBJECT, "billing", &Options::default().with_group("other"))
            .await,
        Err(StreamError::Unsupported(_))
    ));
    assert!(matches!(
        durable.consume(SUBJECT, "", &Options::default()).await,
        Err(StreamError::Unsupported(_))
    ));
}

// Headers, expiry, retain and delayed nak.

/// The plaintext broker's container, for its own tools.
const CONTAINER: &str = "tpp-streams-rabbitmq";

/// A stream declaring [`SUBJECT`] with `headers` as its defaults, under a
/// prefix of its own, and the name of a durable consumer's queue on it.
struct Headed {
    provider: RabbitProvider,
    manager: Arc<dyn Manager>,
    conn: Arc<Connection>,
    exchange: String,
}

impl Headed {
    async fn new(headers: &[(&str, &str)]) -> Self {
        let prefix = prefix();
        let (conn, provider) = provider_under(&prefix).await;
        let stream = provider
            .create(
                Stream::named("headers")
                    .subjects([SUBJECT])
                    .headers(headers.iter().copied()),
            )
            .await
            .unwrap();
        let manager = provider.bind(&stream.id).await.unwrap();
        Self {
            provider,
            manager,
            conn,
            exchange: format!("{prefix}.{}", stream.id),
        }
    }

    /// The durable queue `consumer` reads [`SUBJECT`] from.
    fn queue(&self, consumer: &str) -> String {
        format!("{}.{consumer}.user-created", self.exchange)
    }

    /// Declares `consumer`'s queue and detaches, so publishes wait in it.
    async fn park(&self, consumer: &str) {
        let durable = as_durable(self.manager.as_ref()).unwrap();
        drop(
            durable
                .consume(SUBJECT, consumer, &Options::default())
                .await
                .unwrap(),
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// `docker exec` into the plaintext broker, failing loudly.
fn in_broker(args: &[&str]) -> Vec<u8> {
    let out = std::process::Command::new("docker")
        .args(["exec", CONTAINER])
        .args(args)
        .output()
        .expect("docker is not on PATH");
    assert!(
        out.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    out.stdout
}

/// The broker's own view of `queue`'s messages, through the management HTTP
/// API inside the container, leaving them in the queue.
fn peek(queue: &str) -> Vec<serde_json::Value> {
    let url = format!(
        "http://guest:guest@127.0.0.1:15672/api/queues/%2f/{}/get",
        queue.replace(':', "%3A")
    );
    let body = r#"{"count":10,"ackmode":"ack_requeue_true","encoding":"base64"}"#;
    let out = in_broker(&[
        "wget",
        "-q",
        "-O-",
        "--header",
        "content-type: application/json",
        "--post-data",
        body,
        &url,
    ]);
    serde_json::from_slice(&out).unwrap()
}

/// How many messages the broker holds in `queue`, delivered or not.
fn depth(queue: &str) -> usize {
    let out = in_broker(&["rabbitmqctl", "-q", "list_queues", "name", "messages"]);
    String::from_utf8_lossy(&out)
        .lines()
        .find_map(|line| {
            let mut words = line.split_whitespace();
            (words.next() == Some(queue)).then(|| words.next().unwrap().parse().unwrap())
        })
        .unwrap_or_else(|| panic!("no queue {queue}"))
}

fn headers_of(m: &streams::Message) -> Vec<(String, String)> {
    m.headers()
        .iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect()
}

fn pairs(p: &[(&str, &str)]) -> Vec<(String, String)> {
    p.iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// Stream defaults arrive and a publish's own win, on single publishes over
/// every channel of the pool and on batches.
#[tokio::test]
async fn headers_travel_in_the_amqp_properties() {
    let h = Headed::new(&[("lang", "rust"), ("site", "plant-3")]).await;
    let m = &h.manager;
    let mut sub = m.subscribe(SUBJECT, &Options::default()).await.unwrap();

    m.publish(SUBJECT, event("ada", ""), &Options::default())
        .await
        .unwrap();
    let got = next(&mut sub).await;
    assert_eq!(
        headers_of(&got),
        pairs(&[("lang", "rust"), ("site", "plant-3")])
    );
    assert_eq!(got.json::<Event>().unwrap().user, "ada");

    // More publishes than channels, so every channel carries them.
    for _ in 0..8 {
        m.publish(
            SUBJECT,
            event("grace", ""),
            &Options::default()
                .header("site", "plant-4")
                .header("traceparent", "00-ab"),
        )
        .await
        .unwrap();
        assert_eq!(
            headers_of(&next(&mut sub).await),
            pairs(&[
                ("lang", "rust"),
                ("site", "plant-4"),
                ("traceparent", "00-ab")
            ])
        );
    }

    let ids = as_batch(m.as_ref())
        .unwrap()
        .publish_batch(
            SUBJECT,
            vec![event("a", ""), event("b", "")],
            &Options::default().header("batch", "1"),
        )
        .await
        .unwrap();
    for id in &ids {
        let got = next(&mut sub).await;
        assert_eq!(got.id(), id);
        assert_eq!(
            headers_of(&got),
            pairs(&[("batch", "1"), ("lang", "rust"), ("site", "plant-3")])
        );
    }
}

/// Another tool reading the broker sees the headers as an AMQP table and the
/// expiry as the `expiration` property; the body is runtime-go's version-1
/// frame with no header in it, and a header-less publish sends no table.
#[tokio::test]
async fn headers_are_native_amqp_properties() {
    let h = Headed::new(&[("lang", "rust")]).await;
    h.park("audit").await;
    let queue = h.queue("audit");

    let payload = event("ada", "created");
    let id = h
        .manager
        .publish(
            SUBJECT,
            payload.clone(),
            &Options::default()
                .header("traceparent", "00-ab")
                .with_expiry(Duration::from_secs(60)),
        )
        .await
        .unwrap();
    let seen = peek(&queue);
    assert_eq!(seen.len(), 1, "{seen:?}");
    let props = &seen[0]["properties"];
    assert_eq!(props["headers"]["lang"], "rust", "{props}");
    assert_eq!(props["headers"]["traceparent"], "00-ab", "{props}");
    assert_eq!(props["expiration"], "60000", "{props}");
    assert_eq!(props["message_id"], id.as_str(), "{props}");
    eprintln!("management API sees: {props}");

    let ch = h.conn.create_channel().await.unwrap();
    let raw = ch
        .basic_get(
            queue.as_str().into(),
            lapin::options::BasicGetOptions { no_ack: true },
        )
        .await
        .unwrap()
        .expect("the queue did not hold the message");
    assert_eq!(raw.data, streams::provider::pack(&id, &payload));

    // Without headers or expiry the properties are runtime-go's exactly.
    let plain = Headed::new(&[]).await;
    plain.park("audit").await;
    plain
        .manager
        .publish(SUBJECT, event("x", ""), &Options::default())
        .await
        .unwrap();
    let seen = peek(&plain.queue("audit"));
    let props = seen[0]["properties"].as_object().unwrap();
    assert!(!props.contains_key("headers"), "{props:?}");
    assert!(!props.contains_key("expiration"), "{props:?}");
}

/// A frame a runtime-go peer publishes, with no headers table, reads back with
/// no headers; one carrying the broker's own bookkeeping and a non-string
/// value keeps only the string headers.
#[tokio::test]
async fn foreign_messages_read_back() {
    let h = Headed::new(&[]).await;
    let durable = as_durable(h.manager.as_ref()).unwrap();
    let mut d = durable
        .consume(SUBJECT, "go", &Options::default())
        .await
        .unwrap();
    let ch = h.conn.create_channel().await.unwrap();
    let send = |id: &str, headers: Option<lapin::types::FieldTable>| {
        let mut props = lapin::BasicProperties::default()
            .with_message_id(id.into())
            .with_delivery_mode(2)
            .with_content_type("application/json".into());
        if let Some(t) = headers {
            props = props.with_headers(t);
        }
        let body = streams::provider::pack(id, &event("go", ""));
        let ch = ch.clone();
        let exchange = h.exchange.clone();
        async move {
            ch.basic_publish(
                exchange.as_str().into(),
                SUBJECT.into(),
                lapin::options::BasicPublishOptions::default(),
                &body,
                props,
            )
            .await
            .unwrap()
            .await
            .unwrap();
        }
    };
    send("go-1", None).await;
    let got = next(&mut d).await;
    assert_eq!(got.id(), "go-1");
    assert!(got.headers().is_empty());
    assert_eq!(got.json::<Event>().unwrap().user, "go");
    got.ack().await.unwrap();

    use lapin::types::{AMQPValue, FieldTable};
    let mut table = FieldTable::default();
    table.insert("lang".into(), AMQPValue::LongString("go".into()));
    table.insert("count".into(), AMQPValue::LongInt(3));
    table.insert("x-origin".into(), AMQPValue::LongString("shovel".into()));
    send("go-2", Some(table)).await;
    let got = next(&mut d).await;
    assert_eq!(headers_of(&got), pairs(&[("lang", "go")]));
    got.ack().await.unwrap();
}

/// Headers come back with a redelivery after a nak and after the consumer
/// dropped the message unsettled.
#[tokio::test]
async fn headers_survive_redelivery() {
    let h = Headed::new(&[("lang", "rust")]).await;
    let durable = as_durable(h.manager.as_ref()).unwrap();
    let mut d = durable
        .consume(SUBJECT, "billing", &Options::default())
        .await
        .unwrap();
    h.manager
        .publish(SUBJECT, event("ada", ""), &Options::default().header("try", "1"))
        .await
        .unwrap();
    let want = pairs(&[("lang", "rust"), ("try", "1")]);
    let first = next(&mut d).await;
    assert_eq!(headers_of(&first), want);
    first.nak().await.unwrap();
    let again = next(&mut d).await;
    assert!(again.attempt() >= 2);
    assert_eq!(headers_of(&again), want);
    drop(again);
    drop(d);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut d = durable
        .consume(SUBJECT, "billing", &Options::default())
        .await
        .unwrap();
    let back = next(&mut d).await;
    assert_eq!(headers_of(&back), want);
    back.ack().await.unwrap();
}

/// Invalid and RabbitMQ-reserved headers are refused at create, update and
/// publish, before anything reaches the broker.
#[tokio::test]
async fn bad_headers_are_refused() {
    let (_conn, p) = provider().await;
    let bad = [
        ("x-death", "v"),
        ("x-delivery-count", "1"),
        ("CC", "user.deleted"),
        ("BCC", "user.deleted"),
        ("has space", "v"),
        ("lang", "two\nlines"),
    ];
    for (key, value) in bad {
        let stream = Stream::named("bad").subjects([SUBJECT]).header(key, value);
        assert!(
            matches!(p.create(stream).await, Err(StreamError::InvalidHeader(_))),
            "create accepted {key:?}"
        );
    }
    let long = "k".repeat(256);
    assert!(matches!(
        p.create(Stream::named("bad").subjects([SUBJECT]).header(&long, "v"))
            .await,
        Err(StreamError::InvalidHeader(_))
    ));
    assert!(p.list().await.unwrap().is_empty(), "a refused create stored a stream");

    let good = p
        .create(Stream::named("ok").subjects([SUBJECT]).header("lang", "rust"))
        .await
        .unwrap();
    assert!(matches!(
        p.update(&good.id, Stream::named("ok").subjects([SUBJECT]).header("x-a", "b"))
            .await,
        Err(StreamError::InvalidHeader(_))
    ));
    assert_eq!(p.get(&good.id).await.unwrap().headers.get("lang"), Some("rust"));

    let m = p.bind(&good.id).await.unwrap();
    let batch = as_batch(m.as_ref()).unwrap();
    for (key, value) in bad.into_iter().chain([(long.as_str(), "v")]) {
        let opts = Options::default().header(key, value);
        assert!(
            matches!(
                m.publish(SUBJECT, event("", ""), &opts).await,
                Err(StreamError::InvalidHeader(_))
            ),
            "publish accepted {key:?}"
        );
        assert!(
            matches!(
                batch.publish_batch(SUBJECT, vec![event("", "")], &opts).await,
                Err(StreamError::InvalidHeader(_))
            ),
            "batch accepted {key:?}"
        );
    }
}

/// An expired message is dropped by the broker, not delivered; one that has
/// not expired is delivered with its headers.
#[tokio::test]
async fn expired_messages_are_not_delivered() {
    let h = Headed::new(&[]).await;
    h.park("late").await;
    h.manager
        .publish(
            SUBJECT,
            event("stale", ""),
            &Options::default().with_expiry(Duration::from_millis(200)),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    let fresh = h
        .manager
        .publish(
            SUBJECT,
            event("fresh", ""),
            &Options::default().with_expiry(Duration::from_secs(60)),
        )
        .await
        .unwrap();
    let durable = as_durable(h.manager.as_ref()).unwrap();
    let mut d = durable
        .consume(SUBJECT, "late", &Options::default())
        .await
        .unwrap();
    let got = next(&mut d).await;
    assert_eq!(got.id(), fresh);
    got.ack().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(500), d.next())
            .await
            .is_err(),
        "an expired message was delivered"
    );

    // An expiry RabbitMQ cannot hold is refused by name, and zero means none.
    let too_long = Duration::from_millis(u64::from(u32::MAX) + 1);
    assert!(matches!(
        h.manager
            .publish(SUBJECT, event("", ""), &Options::default().with_expiry(too_long))
            .await,
        Err(StreamError::Unsupported(_))
    ));
    let none = h
        .manager
        .publish(
            SUBJECT,
            event("zero", ""),
            &Options::default().with_expiry(Duration::ZERO),
        )
        .await
        .unwrap();
    assert_eq!(next(&mut d).await.id(), none);
}

/// RabbitMQ expires a message only once it reaches the head of its queue: a
/// short-lived message behind a long-lived one stays in the queue past its
/// expiry, and is dropped, not delivered, once it gets there.
#[tokio::test]
async fn expiry_waits_for_the_head_of_the_queue() {
    let h = Headed::new(&[]).await;
    h.park("slow").await;
    let queue = h.queue("slow");
    let long = h
        .manager
        .publish(
            SUBJECT,
            event("long", ""),
            &Options::default().with_expiry(Duration::from_secs(60)),
        )
        .await
        .unwrap();
    h.manager
        .publish(
            SUBJECT,
            event("short", ""),
            &Options::default().with_expiry(Duration::from_millis(200)),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(depth(&queue), 2, "the expired message is still held behind the head");

    let durable = as_durable(h.manager.as_ref()).unwrap();
    let mut d = durable
        .consume(SUBJECT, "slow", &Options::default())
        .await
        .unwrap();
    let got = next(&mut d).await;
    assert_eq!(got.id(), long);
    got.ack().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(500), d.next())
            .await
            .is_err(),
        "the expired message was delivered once it reached the head"
    );
    assert_eq!(depth(&queue), 0);
}

/// Retain and a delayed nak are refused by name; a refused delayed nak still
/// hands the message straight back rather than stranding it.
#[tokio::test]
async fn retain_and_delayed_nak_are_refused() {
    let h = Headed::new(&[]).await;
    let error = h
        .manager
        .publish(SUBJECT, event("", ""), &Options::default().with_retain())
        .await
        .unwrap_err();
    assert!(matches!(error, StreamError::Unsupported(_)), "{error}");
    assert!(error.to_string().contains("last value"), "{error}");
    assert!(matches!(
        as_batch(h.manager.as_ref())
            .unwrap()
            .publish_batch(SUBJECT, vec![event("", "")], &Options::default().with_retain())
            .await,
        Err(StreamError::Unsupported(_))
    ));

    let durable = as_durable(h.manager.as_ref()).unwrap();
    let mut d = durable
        .consume(SUBJECT, "retry", &Options::default())
        .await
        .unwrap();
    let id = h
        .manager
        .publish(SUBJECT, event("ada", ""), &Options::default())
        .await
        .unwrap();
    let first = next(&mut d).await;
    assert!(!first.can_delay_redelivery());
    let error = first.nak_after(Duration::from_secs(30)).await.unwrap_err();
    assert!(matches!(error, StreamError::Unsupported(_)), "{error}");
    assert!(error.to_string().contains("requeued"), "{error}");
    let again = tokio::time::timeout(Duration::from_secs(5), d.next())
        .await
        .expect("the refused delayed nak stranded the message")
        .unwrap();
    assert_eq!(again.id(), id);
    again.ack().await.unwrap();
    drop(h.provider);
}
