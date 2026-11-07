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
    let conn = Connection::connect(&url(), ConnectionProperties::default())
        .await
        .unwrap_or_else(|e| {
            panic!(
                "no RabbitMQ at {}: {e}; see docker/compose.rabbitmq.yaml",
                url()
            )
        });
    let conn = Arc::new(conn);
    let provider = RabbitProvider::new(conn.clone(), RabbitConfig::default().with_prefix(prefix()))
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
