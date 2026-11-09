//! The Redis provider against a real server, porting runtime-go's
//! `redis_test.go`, `durable_test.go`, and `flow_test.go`.
//!
//! Start one with `docker compose -f docker/compose.redis.yaml up -d --wait`,
//! then `cargo test --features redis,live-tests --test redis_live`. Point it
//! elsewhere with `REDIS_TEST_ADDR` (default `127.0.0.1:16380`). Every test
//! runs under its own key prefix in database 3, so tests share a server
//! without reading each other's streams, and nothing is flushed.
//!
//! Scheduled delivery needs `--notify-keyspace-events Ex`; the compose file
//! sets it, and [`client`] also asks for it with CONFIG SET in case the server
//! was started some other way.

use std::{collections::HashSet, sync::Arc, time::Duration};

use futures::StreamExt;
use streams::{
    Deliveries, Delivery, Headers, Manager, Message, Options, Payload, Position, Stream,
    StreamError, Streams, Subscription, as_batch, as_durable, as_positioned,
    drivers::redis::{RedisConfig, RedisProvider, redis},
};

const SUBJECT: &str = "user.created";
const WITHIN: Duration = Duration::from_secs(5);

/// Short enough to see a redelivery inside a test and long enough that a
/// consumer which is merely slow does not have its message taken.
const QUICK_RECLAIM: Duration = Duration::from_millis(300);

#[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize)]
struct Event {
    user: String,
    action: String,
}

fn event(user: &str, action: &str) -> Payload {
    Payload::json(&Event {
        user: user.into(),
        action: action.into(),
    })
    .unwrap()
}

fn addr() -> String {
    std::env::var("REDIS_TEST_ADDR").unwrap_or_else(|_| "127.0.0.1:16380".into())
}

/// A client on database 3, failing loudly when no server answers: with
/// `live-tests` on, a missing server is a failure, not a skip.
async fn client() -> redis::Client {
    let client = redis::Client::open(format!("redis://{}/3", addr())).unwrap();
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .unwrap_or_else(|error| {
            panic!(
                "no Redis at {} ({error}); start docker/compose.redis.yaml",
                addr()
            )
        });
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("notify-keyspace-events")
        .arg("Ex")
        .query_async(&mut conn)
        .await
        .unwrap();
    client
}

fn config() -> RedisConfig {
    RedisConfig {
        prefix: format!("t{}", streams::provider::new_id()),
        reclaim_after: QUICK_RECLAIM,
        ..RedisConfig::default()
    }
}

async fn immediate() -> RedisProvider {
    RedisProvider::new(client().await, config())
}

async fn scheduled() -> RedisProvider {
    RedisProvider::scheduled(client().await, config())
}

async fn durable_with(config: RedisConfig) -> RedisProvider {
    RedisProvider::durable(client().await, config)
}

async fn durable() -> RedisProvider {
    durable_with(config()).await
}

/// Creates a stream and binds a manager to it.
async fn bind(provider: &RedisProvider, subjects: &[&str]) -> (Stream, Arc<dyn Manager>) {
    let stream = provider
        .create(Stream {
            name: "test".into(),
            subjects: subjects.iter().map(|s| (*s).to_owned()).collect(),
            ..Stream::default()
        })
        .await
        .unwrap();
    let manager = provider.bind(&stream.id).await.unwrap();
    (stream, manager)
}

async fn recv(messages: &mut Subscription, within: Duration) -> Message {
    tokio::time::timeout(within, messages.next())
        .await
        .expect("no message arrived in time")
        .expect("the subscription ended before a message arrived")
}

async fn recv_delivery(deliveries: &mut Deliveries, within: Duration) -> Delivery {
    tokio::time::timeout(within, deliveries.next())
        .await
        .expect("no delivery arrived in time")
        .expect("the consumer ended before a delivery arrived")
}

/// Asserts nothing arrives on `stream` for `quiet`.
async fn silent<S: futures::Stream + Unpin>(stream: &mut S, quiet: Duration)
where
    S::Item: std::fmt::Debug,
{
    if let Ok(Some(item)) = tokio::time::timeout(quiet, stream.next()).await {
        panic!("expected nothing, received {item:?}");
    }
}

async fn command<T: redis::FromRedisValue>(cmd: &redis::Cmd) -> T {
    let mut conn = client()
        .await
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    cmd.query_async(&mut conn).await.unwrap()
}

/// Waits until `check` holds, for state the server updates asynchronously.
async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + WITHIN;
    while tokio::time::Instant::now() < deadline {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{what} did not happen in time");
}

// --- lifecycle -------------------------------------------------------------

#[tokio::test]
async fn stream_lifecycle() {
    let provider = immediate().await;
    let created = provider
        .create(Stream {
            name: "users".into(),
            description: "before".into(),
            subjects: vec![SUBJECT.into()],
            ..Stream::default()
        })
        .await
        .unwrap();
    assert!(!created.id.is_empty());
    assert!(created.active);

    let updated = provider
        .update(
            &created.id,
            Stream {
                name: "users".into(),
                description: "after".into(),
                subjects: vec![SUBJECT.into()],
                ..Stream::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.id, created.id);

    // Update must leave exactly one metadata entry, not accumulate them.
    assert_eq!(
        provider.get(&created.id).await.unwrap().description,
        "after"
    );
    let list = provider.list().await.unwrap();
    assert_eq!(list.len(), 1);

    provider.delete(&created.id).await.unwrap();
    assert!(matches!(
        provider.get(&created.id).await,
        Err(StreamError::NotFound(_))
    ));
}

#[tokio::test]
async fn deleting_a_missing_stream_reports_not_found() {
    let provider = immediate().await;
    assert!(matches!(
        provider.delete("absent").await,
        Err(StreamError::NotFound(_))
    ));
}

#[tokio::test]
async fn binding_a_missing_stream_fails() {
    let provider = immediate().await;
    assert!(matches!(
        provider.bind("absent").await,
        Err(StreamError::NotFound(_))
    ));
}

#[tokio::test]
async fn modes_keep_separate_namespaces() {
    let client = client().await;
    let config = config();
    let plain = RedisProvider::new(client.clone(), config.clone());
    let sched = RedisProvider::scheduled(client.clone(), config.clone());
    let log = RedisProvider::durable(client, config);
    for provider in [&plain, &sched, &log] {
        bind(provider, &["s"]).await;
    }
    for provider in [&plain, &sched, &log] {
        assert_eq!(provider.list().await.unwrap().len(), 1);
    }
}

// --- immediate -------------------------------------------------------------

#[tokio::test]
async fn publish_and_subscribe() {
    let (_, m) = bind(&immediate().await, &[SUBJECT]).await;
    let mut messages = m.subscribe(SUBJECT, &Options::default()).await.unwrap();
    let id = m
        .publish(SUBJECT, event("alice", "created"), &Options::default())
        .await
        .unwrap();

    let message = recv(&mut messages, WITHIN).await;
    assert_eq!(message.id(), id);
    assert_eq!(message.subject(), SUBJECT);
    assert_eq!(
        message.json::<Event>().unwrap(),
        Event {
            user: "alice".into(),
            action: "created".into()
        }
    );
}

#[tokio::test]
async fn publish_delivers_exactly_once() {
    let (_, m) = bind(&immediate().await, &[SUBJECT]).await;
    let mut messages = m.subscribe(SUBJECT, &Options::default()).await.unwrap();
    m.publish(SUBJECT, event("a", ""), &Options::default())
        .await
        .unwrap();
    recv(&mut messages, WITHIN).await;
    silent(&mut messages, Duration::from_millis(500)).await;
}

#[tokio::test]
async fn undeclared_subjects_are_rejected() {
    let (_, m) = bind(&immediate().await, &[SUBJECT]).await;
    let publish = m.publish("typo", event("", ""), &Options::default()).await;
    assert!(matches!(publish, Err(StreamError::UnknownSubject { .. })));
    let subscribe = m.subscribe("typo", &Options::default()).await;
    assert!(matches!(subscribe, Err(StreamError::UnknownSubject { .. })));
}

#[tokio::test]
async fn subjects_are_isolated() {
    let (_, m) = bind(&immediate().await, &["a", "b"]).await;
    let mut on_a = m.subscribe("a", &Options::default()).await.unwrap();
    m.publish("b", event("for b", ""), &Options::default())
        .await
        .unwrap();
    silent(&mut on_a, Duration::from_millis(300)).await;
}

#[tokio::test]
async fn an_immediate_stream_rejects_a_ttl() {
    let (_, m) = bind(&immediate().await, &[SUBJECT]).await;
    let opts = Options::default().with_ttl(Duration::from_secs(1));
    let error = m.publish(SUBJECT, event("", ""), &opts).await.unwrap_err();
    assert!(matches!(error, StreamError::Unsupported(_)), "{error}");
}

#[tokio::test]
async fn a_group_is_refused_rather_than_fanned_out() {
    let (_, m) = bind(&immediate().await, &[SUBJECT]).await;
    let opts = Options::default().with_group("workers");
    assert!(matches!(
        m.subscribe(SUBJECT, &opts).await,
        Err(StreamError::Unsupported(_))
    ));
}

/// Dropping the subscription is how it ends, and it has to release the
/// server-side subscription with it.
#[tokio::test]
async fn dropping_a_subscription_unsubscribes() {
    let (stream, m) = bind(&immediate().await, &[SUBJECT]).await;
    let channel = config_channel(&m, &stream).await;
    let subscribers = || async {
        let reply: (String, u64) = command(redis::cmd("PUBSUB").arg("NUMSUB").arg(&channel)).await;
        reply.1
    };

    let messages = m.subscribe(SUBJECT, &Options::default()).await.unwrap();
    assert_eq!(subscribers().await, 1);
    m.publish(SUBJECT, event("ignored", ""), &Options::default())
        .await
        .unwrap();
    drop(messages);
    eventually("the server-side unsubscribe", || async {
        subscribers().await == 0
    })
    .await;
}

/// The channel name for `stream`'s subject, found by asking the server which
/// channels have subscribers, so the test does not restate the key layout.
async fn config_channel(m: &Arc<dyn Manager>, stream: &Stream) -> String {
    let probe = m.subscribe(SUBJECT, &Options::default()).await.unwrap();
    let channels: Vec<String> = command(
        redis::cmd("PUBSUB")
            .arg("CHANNELS")
            .arg(format!("*{}*", stream.id)),
    )
    .await;
    drop(probe);
    assert_eq!(channels.len(), 1, "{channels:?}");
    let channel = channels.into_iter().next().unwrap();
    assert!(
        channel.ends_with(&format!("stream:ch:{}:{SUBJECT}", stream.id)),
        "{channel}"
    );
    eventually("the probe to unsubscribe", || async {
        let reply: (String, u64) = command(redis::cmd("PUBSUB").arg("NUMSUB").arg(&channel)).await;
        reply.1 == 0
    })
    .await;
    channel
}

#[tokio::test]
async fn the_typed_view_decodes_deliveries() {
    let (_, m) = bind(&immediate().await, &[SUBJECT]).await;
    let events = streams::typed::<Event>(m);
    let mut received = events
        .subscribe(SUBJECT, &Options::default())
        .await
        .unwrap();
    let grace = Event {
        user: "grace".into(),
        action: "created".into(),
    };
    events
        .publish(SUBJECT, &grace, &Options::default())
        .await
        .unwrap();
    let got = tokio::time::timeout(WITHIN, received.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, grace);
}

#[tokio::test]
async fn an_immediate_batch_is_one_pipeline() {
    let (_, m) = bind(&immediate().await, &[SUBJECT]).await;
    let mut messages = m.subscribe(SUBJECT, &Options::default()).await.unwrap();
    let batch = as_batch(m.as_ref()).unwrap();
    let ids = batch
        .publish_batch(
            SUBJECT,
            vec![event("a", ""), event("b", ""), event("c", "")],
            &Options::default(),
        )
        .await
        .unwrap();
    assert_eq!(ids.len(), 3);
    for id in &ids {
        assert_eq!(recv(&mut messages, WITHIN).await.id(), id);
    }
    let refused = batch
        .publish_batch(
            SUBJECT,
            vec![event("a", "")],
            &Options::default().with_id("x"),
        )
        .await;
    assert!(matches!(refused, Err(StreamError::Unsupported(_))));
}

#[tokio::test]
async fn pubsub_has_no_durable_half() {
    let (_, m) = bind(&immediate().await, &[SUBJECT]).await;
    assert!(matches!(
        as_durable(m.as_ref()),
        Err(StreamError::Unsupported(_))
    ));
    assert!(matches!(
        as_positioned(m.as_ref()),
        Err(StreamError::Unsupported(_))
    ));
}

// --- flow control ----------------------------------------------------------

/// Prefetch is a request, and asking for none has to still deliver.
#[tokio::test]
async fn prefetch_is_honoured() {
    let (_, m) = bind(&immediate().await, &[SUBJECT]).await;
    for prefetch in [0, 1, 128] {
        let opts = Options::default().with_prefetch(prefetch);
        let mut messages = m.subscribe(SUBJECT, &opts).await.unwrap();
        m.publish(SUBJECT, event("ada", ""), &Options::default())
            .await
            .unwrap();
        recv(&mut messages, WITHIN).await;
    }
}

#[tokio::test]
async fn delivery_survives_a_late_reader() {
    let (_, m) = bind(&immediate().await, &[SUBJECT]).await;
    let mut messages = m
        .subscribe(SUBJECT, &Options::default().with_prefetch(8))
        .await
        .unwrap();
    m.publish(SUBJECT, event("early", ""), &Options::default())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        recv(&mut messages, WITHIN)
            .await
            .json::<Event>()
            .unwrap()
            .user,
        "early"
    );
}

// --- scheduled -------------------------------------------------------------

fn after(ms: u64) -> Options {
    Options::default().with_ttl(Duration::from_millis(ms))
}

#[tokio::test]
async fn scheduled_delivers_on_expiry() {
    let (_, m) = bind(&scheduled().await, &["reminder"]).await;
    let mut messages = m.subscribe("reminder", &Options::default()).await.unwrap();
    m.publish("reminder", event("alice", "take a pill"), &after(300))
        .await
        .unwrap();
    let message = recv(&mut messages, WITHIN).await;
    assert_eq!(message.json::<Event>().unwrap().action, "take a pill");
}

#[tokio::test]
async fn scheduled_requires_a_ttl() {
    let (_, m) = bind(&scheduled().await, &["reminder"]).await;
    let error = m
        .publish("reminder", event("", ""), &Options::default())
        .await
        .unwrap_err();
    assert!(matches!(error, StreamError::Unsupported(_)), "{error}");
    let zero = Options::default().with_ttl(Duration::ZERO);
    assert!(matches!(
        m.publish("reminder", event("", ""), &zero).await,
        Err(StreamError::Unsupported(_))
    ));
}

#[tokio::test]
async fn two_scheduled_messages_both_fire() {
    let (_, m) = bind(&scheduled().await, &["reminder"]).await;
    let mut messages = m.subscribe("reminder", &Options::default()).await.unwrap();
    for who in ["first", "second"] {
        m.publish("reminder", event(who, ""), &after(300))
            .await
            .unwrap();
    }
    let mut seen = HashSet::new();
    while seen.len() < 2 {
        seen.insert(
            recv(&mut messages, WITHIN)
                .await
                .json::<Event>()
                .unwrap()
                .user,
        );
    }
}

#[tokio::test]
async fn scheduled_subjects_are_honoured() {
    let (_, m) = bind(&scheduled().await, &["wanted", "unwanted"]).await;
    let mut messages = m.subscribe("wanted", &Options::default()).await.unwrap();
    m.publish("unwanted", event("nope", ""), &after(200))
        .await
        .unwrap();
    silent(&mut messages, Duration::from_millis(1500)).await;
}

/// One expiry delivers one message even with two subscribers racing for it.
#[tokio::test]
async fn one_expiry_is_claimed_once() {
    let (_, m) = bind(&scheduled().await, &["reminder"]).await;
    let mut a = m.subscribe("reminder", &Options::default()).await.unwrap();
    let mut b = m.subscribe("reminder", &Options::default()).await.unwrap();
    m.publish("reminder", event("once", ""), &after(200))
        .await
        .unwrap();
    let mut merged = futures::stream::select(&mut a, &mut b);
    tokio::time::timeout(WITHIN, merged.next())
        .await
        .unwrap()
        .unwrap();
    silent(&mut merged, Duration::from_millis(700)).await;
}

#[tokio::test]
async fn a_scheduled_batch_fires_whole() {
    let (_, m) = bind(&scheduled().await, &["reminder"]).await;
    let mut messages = m.subscribe("reminder", &Options::default()).await.unwrap();
    let ids = as_batch(m.as_ref())
        .unwrap()
        .publish_batch(
            "reminder",
            vec![event("a", ""), event("b", "")],
            &after(200),
        )
        .await
        .unwrap();
    let mut seen = HashSet::new();
    while seen.len() < 2 {
        seen.insert(recv(&mut messages, WITHIN).await.id().to_owned());
    }
    assert_eq!(seen, ids.into_iter().collect());
}

// --- durable ---------------------------------------------------------------

#[tokio::test]
async fn as_durable_succeeds_only_on_the_durable_provider() {
    let (_, m) = bind(&durable().await, &[SUBJECT]).await;
    assert!(as_durable(m.as_ref()).is_ok());
    assert!(as_positioned(m.as_ref()).is_ok());
    assert!(as_batch(m.as_ref()).is_ok());
}

#[tokio::test]
async fn consume_delivers_and_acknowledges() {
    let (_, m) = bind(&durable().await, &[SUBJECT]).await;
    let d = as_durable(m.as_ref()).unwrap();
    let mut deliveries = d
        .consume(SUBJECT, "workers", &Options::default())
        .await
        .unwrap();
    m.publish(SUBJECT, event("ada", "created"), &Options::default())
        .await
        .unwrap();

    let got = recv_delivery(&mut deliveries, WITHIN).await;
    assert_eq!(got.attempt(), 1);
    assert_eq!(got.json::<Event>().unwrap().user, "ada");
    got.ack().await.unwrap();
}

/// The whole reason to reach for Durable: a consumer that dies holding a
/// message must not be the last word on it.
#[tokio::test]
async fn an_unacknowledged_message_is_redelivered() {
    let (_, m) = bind(&durable().await, &[SUBJECT]).await;
    let d = as_durable(m.as_ref()).unwrap();

    let mut first = d
        .consume(SUBJECT, "workers", &Options::default())
        .await
        .unwrap();
    m.publish(SUBJECT, event("grace", ""), &Options::default())
        .await
        .unwrap();
    let taken = recv_delivery(&mut first, WITHIN).await;
    assert_eq!(taken.attempt(), 1);
    drop(taken);
    drop(first);

    let mut again = d
        .consume(SUBJECT, "workers", &Options::default())
        .await
        .unwrap();
    let got = recv_delivery(&mut again, Duration::from_secs(10)).await;
    assert!(got.attempt() >= 2, "attempt {}", got.attempt());
    assert_eq!(got.json::<Event>().unwrap().user, "grace");
}

/// A nak leaves the message pending, so it comes back with its attempt count
/// rising rather than as a fresh copy.
#[tokio::test]
async fn a_naked_message_comes_back() {
    let (_, m) = bind(&durable().await, &[SUBJECT]).await;
    let d = as_durable(m.as_ref()).unwrap();
    let mut deliveries = d
        .consume(SUBJECT, "workers", &Options::default())
        .await
        .unwrap();
    let id = m
        .publish(SUBJECT, event("retry", ""), &Options::default())
        .await
        .unwrap();

    let first = recv_delivery(&mut deliveries, WITHIN).await;
    first.nak().await.unwrap();
    let second = recv_delivery(&mut deliveries, Duration::from_secs(10)).await;
    assert_eq!(second.id(), id);
    assert!(second.attempt() >= 2);
    second.ack().await.unwrap();
}

#[tokio::test]
async fn an_acknowledged_message_is_not_redelivered() {
    let (_, m) = bind(&durable().await, &[SUBJECT]).await;
    let d = as_durable(m.as_ref()).unwrap();

    let mut first = d
        .consume(SUBJECT, "workers", &Options::default())
        .await
        .unwrap();
    m.publish(SUBJECT, event("ada", ""), &Options::default())
        .await
        .unwrap();
    recv_delivery(&mut first, WITHIN).await.ack().await.unwrap();
    drop(first);

    let mut again = d
        .consume(SUBJECT, "workers", &Options::default())
        .await
        .unwrap();
    silent(&mut again, Duration::from_secs(2)).await;
}

/// With `confirm_acks` off an ack returns before its XACK is answered, and
/// many go out as one multi-id XACK; every one still lands, so the group ends
/// with nothing pending and a fresh consumer under the name sees nothing again.
#[tokio::test]
async fn unconfirmed_acks_still_settle_every_message() {
    let config = RedisConfig {
        confirm_acks: false,
        ..config()
    };
    let prefix = config.prefix.clone();
    let provider = durable_with(config).await;
    let (stream, m) = bind(&provider, &[SUBJECT]).await;
    let d = as_durable(m.as_ref()).unwrap();
    let mut first = d
        .consume(SUBJECT, "workers", &Options::default().with_prefetch(256))
        .await
        .unwrap();

    const TOTAL: usize = 500;
    as_batch(m.as_ref())
        .unwrap()
        .publish_batch(
            SUBJECT,
            (0..TOTAL).map(|i| event("u", &i.to_string())).collect(),
            &Options::default(),
        )
        .await
        .unwrap();
    let mut seen = HashSet::new();
    while seen.len() < TOTAL {
        let delivery = recv_delivery(&mut first, WITHIN).await;
        assert_eq!(delivery.attempt(), 1);
        seen.insert(delivery.id().to_owned());
        delivery.ack().await.unwrap();
    }

    let key = format!("{prefix}:durable:s:{}:{SUBJECT}", stream.id);
    eventually("every ack to land", || {
        let key = key.clone();
        async move {
            let reply: redis::Value =
                command(redis::cmd("XPENDING").arg(&key).arg("workers")).await;
            matches!(&reply, redis::Value::Array(v) if v.first() == Some(&redis::Value::Int(0)))
        }
    })
    .await;
    drop(first);

    let mut again = d
        .consume(SUBJECT, "workers", &Options::default())
        .await
        .unwrap();
    silent(&mut again, Duration::from_secs(1)).await;
}

/// A named consumer's position outlives the process reading it.
#[tokio::test]
async fn a_consumer_resumes_where_the_name_left_off() {
    let (_, m) = bind(&durable().await, &[SUBJECT]).await;
    let d = as_durable(m.as_ref()).unwrap();

    let mut first = d
        .consume(SUBJECT, "workers", &Options::default())
        .await
        .unwrap();
    m.publish(SUBJECT, event("first", ""), &Options::default())
        .await
        .unwrap();
    recv_delivery(&mut first, WITHIN).await.ack().await.unwrap();
    drop(first);

    m.publish(SUBJECT, event("second", ""), &Options::default())
        .await
        .unwrap();
    let mut again = d
        .consume(SUBJECT, "workers", &Options::default())
        .await
        .unwrap();
    let resumed = recv_delivery(&mut again, Duration::from_secs(10)).await;
    assert_eq!(resumed.json::<Event>().unwrap().user, "second");
}

#[tokio::test]
async fn consume_from_earliest_replays_history() {
    let (_, m) = bind(&durable().await, &[SUBJECT]).await;
    m.publish(SUBJECT, event("before", ""), &Options::default())
        .await
        .unwrap();
    let p = as_positioned(m.as_ref()).unwrap();
    let mut deliveries = p
        .consume_from(SUBJECT, "history", Position::Earliest, &Options::default())
        .await
        .unwrap();
    let got = recv_delivery(&mut deliveries, WITHIN).await;
    assert_eq!(got.json::<Event>().unwrap().user, "before");
}

#[tokio::test]
async fn consume_from_new_skips_history() {
    let (_, m) = bind(&durable().await, &[SUBJECT]).await;
    m.publish(SUBJECT, event("before", ""), &Options::default())
        .await
        .unwrap();
    let p = as_positioned(m.as_ref()).unwrap();
    let mut deliveries = p
        .consume_from(SUBJECT, "tail", Position::New, &Options::default())
        .await
        .unwrap();
    silent(&mut deliveries, Duration::from_secs(2)).await;
}

/// Two processes under one name share the position and split the work.
#[tokio::test]
async fn one_name_splits_the_work() {
    // A long reclaim so the two cannot take each other's messages: this is
    // about the split, not redelivery.
    let provider = durable_with(RedisConfig {
        reclaim_after: Duration::from_secs(60),
        ..config()
    })
    .await;
    let (_, m) = bind(&provider, &[SUBJECT]).await;
    let d = as_durable(m.as_ref()).unwrap();
    let first = d
        .consume(SUBJECT, "shared", &Options::default())
        .await
        .unwrap();
    let second = d
        .consume(SUBJECT, "shared", &Options::default())
        .await
        .unwrap();

    const TOTAL: usize = 6;
    let ids = as_batch(m.as_ref())
        .unwrap()
        .publish_batch(
            SUBJECT,
            (0..TOTAL).map(|i| event("u", &i.to_string())).collect(),
            &Options::default(),
        )
        .await
        .unwrap();

    let mut merged: Deliveries = futures::stream::select(first, second).boxed();
    let mut seen = HashSet::new();
    while seen.len() < TOTAL {
        let delivery = recv_delivery(&mut merged, Duration::from_secs(15)).await;
        seen.insert(delivery.id().to_owned());
        delivery.ack().await.unwrap();
    }
    assert_eq!(seen, ids.into_iter().collect());
}

/// Dropping a consumer stops its loop and removes its identity from the group
/// once it holds nothing, so the roster does not grow with every restart.
#[tokio::test]
async fn dropping_a_consumer_leaves_the_group() {
    let (stream, m) = bind(&durable().await, &[SUBJECT]).await;
    let d = as_durable(m.as_ref()).unwrap();
    let deliveries = d
        .consume(SUBJECT, "workers", &Options::default())
        .await
        .unwrap();

    let key = {
        let keys: Vec<String> =
            command(redis::cmd("KEYS").arg(format!("*durable:s:{}:{SUBJECT}", stream.id))).await;
        assert_eq!(keys.len(), 1, "{keys:?}");
        keys.into_iter().next().unwrap()
    };
    let consumers = || {
        let key = key.clone();
        async move {
            let reply: Vec<redis::Value> = command(
                redis::cmd("XINFO")
                    .arg("CONSUMERS")
                    .arg(&key)
                    .arg("workers"),
            )
            .await;
            reply.len()
        }
    };
    eventually("the consumer to join", || async { consumers().await == 1 }).await;
    drop(deliveries);
    eventually("the consumer to leave", || async { consumers().await == 0 }).await;
}

#[tokio::test]
async fn durable_subscribe_tails_the_log() {
    let (_, m) = bind(&durable().await, &[SUBJECT]).await;
    m.publish(SUBJECT, event("history", ""), &Options::default())
        .await
        .unwrap();
    let mut messages = m.subscribe(SUBJECT, &Options::default()).await.unwrap();
    m.publish(SUBJECT, event("tail", ""), &Options::default())
        .await
        .unwrap();
    assert_eq!(
        recv(&mut messages, WITHIN)
            .await
            .json::<Event>()
            .unwrap()
            .user,
        "tail"
    );
}

#[tokio::test]
async fn durable_rejects_a_ttl() {
    let (_, m) = bind(&durable().await, &[SUBJECT]).await;
    let opts = Options::default().with_ttl(Duration::from_secs(1));
    assert!(matches!(
        m.publish(SUBJECT, event("", ""), &opts).await,
        Err(StreamError::Unsupported(_))
    ));
}

#[tokio::test]
async fn consume_refuses_what_it_cannot_mean() {
    let (_, m) = bind(&durable().await, &[SUBJECT]).await;
    let d = as_durable(m.as_ref()).unwrap();
    let group = Options::default().with_group("other");
    assert!(matches!(
        d.consume(SUBJECT, "workers", &group).await,
        Err(StreamError::Unsupported(_))
    ));
    assert!(matches!(
        d.consume(SUBJECT, "", &Options::default()).await,
        Err(StreamError::Unsupported(_))
    ));
    assert!(matches!(
        d.consume("typo", "workers", &Options::default()).await,
        Err(StreamError::UnknownSubject { .. })
    ));
}

/// The retention cap trims the log, approximately, as Go's WithMaxLen does.
#[tokio::test]
async fn max_len_caps_the_log() {
    let provider = durable_with(RedisConfig {
        max_len: Some(10),
        ..config()
    })
    .await;
    let (stream, m) = bind(&provider, &[SUBJECT]).await;
    let batch = as_batch(m.as_ref()).unwrap();
    for _ in 0..10 {
        batch
            .publish_batch(
                SUBJECT,
                (0..100).map(|i| event("u", &i.to_string())).collect(),
                &Options::default(),
            )
            .await
            .unwrap();
    }
    let keys: Vec<String> =
        command(redis::cmd("KEYS").arg(format!("*durable:s:{}:{SUBJECT}", stream.id))).await;
    let len: u64 = command(redis::cmd("XLEN").arg(&keys[0])).await;
    assert!(len < 1000, "the log kept {len} entries");
}

// --- headers, expiry, retain, delayed nak ------------------------------------

fn headers<const N: usize>(pairs: [(&str, &str); N]) -> Headers {
    pairs.into_iter().collect()
}

/// A stream on SUBJECT stamping `lang` and `site` on everything.
fn with_defaults() -> Stream {
    Stream::named("headers")
        .subject(SUBJECT)
        .header("lang", "rust")
        .header("site", "plant-3")
}

async fn bind_stream(provider: &RedisProvider, stream: Stream) -> (Stream, Arc<dyn Manager>) {
    let stream = provider.create(stream).await.unwrap();
    let manager = provider.bind(&stream.id).await.unwrap();
    (stream, manager)
}

/// What a publish that overrides `site` and adds `trace` should arrive with.
fn overridden() -> Headers {
    headers([("lang", "rust"), ("site", "plant-9"), ("trace", "t-1")])
}

fn override_opts() -> Options {
    Options::default()
        .header("site", "plant-9")
        .header("trace", "t-1")
}

#[tokio::test]
async fn pubsub_carries_stream_and_publish_headers() {
    let (_, m) = bind_stream(&immediate().await, with_defaults()).await;
    let mut messages = m.subscribe(SUBJECT, &Options::default()).await.unwrap();

    m.publish(SUBJECT, event("a", ""), &Options::default())
        .await
        .unwrap();
    let got = recv(&mut messages, WITHIN).await;
    assert_eq!(
        got.headers(),
        &headers([("lang", "rust"), ("site", "plant-3")])
    );
    assert_eq!(got.json::<Event>().unwrap().user, "a");

    m.publish(SUBJECT, event("b", ""), &override_opts())
        .await
        .unwrap();
    assert_eq!(recv(&mut messages, WITHIN).await.headers(), &overridden());

    let batch = as_batch(m.as_ref()).unwrap();
    batch
        .publish_batch(SUBJECT, vec![event("c", ""), event("d", "")], &override_opts())
        .await
        .unwrap();
    for _ in 0..2 {
        assert_eq!(recv(&mut messages, WITHIN).await.headers(), &overridden());
    }
}

/// A message without headers is the version-1 frame runtime-go writes and
/// reads, byte for byte; one with headers is version 2.
#[tokio::test]
async fn pubsub_frames_stay_version_1_without_headers() {
    let (stream, m) = bind(&immediate().await, &[SUBJECT]).await;
    let channel = config_channel(&m, &stream).await;
    let mut raw = client().await.get_async_pubsub().await.unwrap();
    raw.subscribe(&channel).await.unwrap();
    let mut raw = raw.into_on_message();

    let payload = event("go", "");
    let id = m
        .publish(SUBJECT, payload.clone(), &Options::default())
        .await
        .unwrap();
    let frame = tokio::time::timeout(WITHIN, raw.next())
        .await
        .unwrap()
        .unwrap()
        .get_payload_bytes()
        .to_vec();
    assert_eq!(frame[1], 1, "a header-less frame must stay version 1");
    assert_eq!(frame, streams::provider::pack(&id, &payload));

    m.publish(SUBJECT, payload, &Options::default().header("lang", "rust"))
        .await
        .unwrap();
    let frame = tokio::time::timeout(WITHIN, raw.next())
        .await
        .unwrap()
        .unwrap()
        .get_payload_bytes()
        .to_vec();
    assert_eq!(frame[1], 2);
}

#[tokio::test]
async fn scheduled_carries_headers() {
    let (_, m) = bind_stream(&scheduled().await, with_defaults()).await;
    let mut messages = m.subscribe(SUBJECT, &Options::default()).await.unwrap();
    m.publish(SUBJECT, event("later", ""), &override_opts().with_ttl(Duration::from_millis(200)))
        .await
        .unwrap();
    let got = recv(&mut messages, WITHIN).await;
    assert_eq!(got.headers(), &overridden());
    assert_eq!(got.json::<Event>().unwrap().user, "later");
}

/// Durable headers are fields of the entry, beside an unchanged version-1
/// `msg` frame, and every delivery reads them back, a redelivery included.
#[tokio::test]
async fn durable_headers_are_entry_fields() {
    let config = config();
    let prefix = config.prefix.clone();
    let (stream, m) = bind_stream(&durable_with(config).await, with_defaults()).await;
    let d = as_durable(m.as_ref()).unwrap();
    let mut tail = m.subscribe(SUBJECT, &Options::default()).await.unwrap();
    let mut deliveries = d
        .consume(SUBJECT, "workers", &Options::default())
        .await
        .unwrap();
    let id = m
        .publish(SUBJECT, event("d", ""), &override_opts())
        .await
        .unwrap();

    let key = format!("{prefix}:durable:s:{}:{SUBJECT}", stream.id);
    let entries: redis::streams::StreamRangeReply =
        command(redis::cmd("XRANGE").arg(&key).arg("-").arg("+")).await;
    let entry = &entries.ids[0];
    let mut fields: Vec<&str> = entry.map.keys().map(String::as_str).collect();
    fields.sort_unstable();
    assert_eq!(fields, ["h:lang", "h:site", "h:trace", "msg"]);
    assert_eq!(entry.get::<String>("h:site").as_deref(), Some("plant-9"));
    let frame: Vec<u8> = entry.get("msg").unwrap();
    assert_eq!(frame[1], 1, "the payload frame stays the one Go reads");

    assert_eq!(recv(&mut tail, WITHIN).await.headers(), &overridden());

    let first = recv_delivery(&mut deliveries, WITHIN).await;
    assert_eq!(first.id(), id);
    assert_eq!(first.headers(), &overridden());
    first.nak().await.unwrap();
    let again = recv_delivery(&mut deliveries, Duration::from_secs(10)).await;
    assert_eq!(again.id(), id);
    assert!(again.attempt() >= 2);
    assert_eq!(again.headers(), &overridden(), "a redelivery lost its headers");
    again.ack().await.unwrap();

    let batch = as_batch(m.as_ref()).unwrap();
    batch
        .publish_batch(SUBJECT, vec![event("e", "")], &Options::default())
        .await
        .unwrap();
    let next = recv_delivery(&mut deliveries, WITHIN).await;
    assert_eq!(
        next.headers(),
        &headers([("lang", "rust"), ("site", "plant-3")])
    );
    next.ack().await.unwrap();
}

/// Stream headers are stored with the metadata; a stream without them is
/// stored exactly as runtime-go stores it, with no `headers` field.
#[tokio::test]
async fn stream_headers_are_stored_and_omitted_when_empty() {
    for provider in [immediate().await, scheduled().await, durable().await] {
        let created = provider.create(with_defaults()).await.unwrap();
        let read = provider.get(&created.id).await.unwrap();
        assert_eq!(read.headers, headers([("lang", "rust"), ("site", "plant-3")]));
        let updated = provider
            .update(&created.id, with_defaults().header("team", "billing"))
            .await
            .unwrap();
        assert_eq!(provider.get(&created.id).await.unwrap(), updated);
        assert_eq!(updated.headers.get("team"), Some("billing"));
        provider.delete(&created.id).await.unwrap();
    }

    let config = config();
    let prefix = config.prefix.clone();
    let provider = durable_with(config).await;
    let (stream, _) = bind(&provider, &[SUBJECT]).await;
    let meta: redis::streams::StreamRangeReply = command(
        redis::cmd("XRANGE")
            .arg(format!("{prefix}:durable:meta:{}", stream.id))
            .arg("-")
            .arg("+"),
    )
    .await;
    let json: String = meta.ids[0].get("meta").unwrap();
    assert!(!json.contains("headers"), "{json}");
}

fn invalid<T: std::fmt::Debug>(result: streams::Result<T>) {
    assert!(
        matches!(result, Err(StreamError::InvalidHeader(_))),
        "expected InvalidHeader, got {result:?}"
    );
}

fn unsupported<T: std::fmt::Debug>(result: streams::Result<T>) {
    assert!(
        matches!(result, Err(StreamError::Unsupported(_))),
        "expected Unsupported, got {result:?}"
    );
}

/// The three modes, each with the publish options it otherwise accepts: a
/// scheduled stream needs a TTL.
async fn every_mode() -> Vec<(RedisProvider, Options)> {
    vec![
        (immediate().await, Options::default()),
        (scheduled().await, after(200)),
        (durable().await, Options::default()),
    ]
}

#[tokio::test]
async fn invalid_headers_are_refused_where_they_are_set() {
    for (provider, base) in every_mode().await {
        invalid(
            provider
                .create(Stream::named("x").subject(SUBJECT).header("a:b", "1"))
                .await,
        );
        invalid(
            provider
                .create(Stream::named("x").subject(SUBJECT).header("", "1"))
                .await,
        );
        let (stream, m) = bind(&provider, &[SUBJECT]).await;
        invalid(
            provider
                .update(&stream.id, Stream::named("x").header("bad key", "1"))
                .await,
        );
        let bad = base.clone().header("ok", "line\nbreak");
        invalid(m.publish(SUBJECT, event("", ""), &bad).await);
        invalid(
            as_batch(m.as_ref())
                .unwrap()
                .publish_batch(SUBJECT, vec![event("", "")], &bad)
                .await,
        );
        // Nothing reserved: a header may even be called `msg` or `meta`.
        m.publish(SUBJECT, event("", ""), &base.clone().header("msg", "fine"))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn expiry_and_retain_are_refused_in_every_mode() {
    for (provider, base) in every_mode().await {
        let (_, m) = bind(&provider, &[SUBJECT]).await;
        let batch = as_batch(m.as_ref()).unwrap();
        for opts in [
            base.clone().with_expiry(Duration::from_secs(5)),
            base.clone().with_retain(),
        ] {
            unsupported(m.publish(SUBJECT, event("", ""), &opts).await);
            unsupported(batch.publish_batch(SUBJECT, vec![event("", "")], &opts).await);
        }
        // A zero expiry is no expiry, as a zero TTL is no TTL.
        m.publish(
            SUBJECT,
            event("", ""),
            &base.clone().with_expiry(Duration::ZERO),
        )
        .await
        .unwrap();
    }
}

/// A delayed nak holds the redelivery back for the delay and no longer: well
/// before a plain nak's reclaim interval, never before the delay, and with the
/// attempt count of one redelivery.
#[tokio::test]
async fn a_delayed_nak_waits_for_its_delay() {
    let reclaim = Duration::from_secs(8);
    let delay = Duration::from_millis(1500);
    let provider = durable_with(RedisConfig {
        reclaim_after: reclaim,
        ..config()
    })
    .await;
    let (_, m) = bind_stream(&provider, with_defaults()).await;
    let mut deliveries = as_durable(m.as_ref())
        .unwrap()
        .consume(SUBJECT, "workers", &Options::default())
        .await
        .unwrap();
    let id = m
        .publish(SUBJECT, event("busy", ""), &Options::default())
        .await
        .unwrap();

    let first = recv_delivery(&mut deliveries, WITHIN).await;
    assert!(first.can_delay_redelivery());
    let naked_at = tokio::time::Instant::now();
    first.nak_after(delay).await.unwrap();

    silent(&mut deliveries, delay - Duration::from_millis(100)).await;
    let again = recv_delivery(&mut deliveries, WITHIN).await;
    let waited = naked_at.elapsed();
    assert_eq!(again.id(), id);
    assert!(waited >= delay, "redelivered after {waited:?}");
    assert!(
        waited < reclaim - Duration::from_secs(2),
        "redelivered after {waited:?}, no sooner than a plain nak"
    );
    assert_eq!(again.attempt(), 2);
    assert_eq!(
        again.headers(),
        &headers([("lang", "rust"), ("site", "plant-3")])
    );
    again.ack().await.unwrap();
}

#[tokio::test]
async fn a_delayed_nak_beyond_the_reclaim_interval_is_refused() {
    let (_, m) = bind(&durable().await, &[SUBJECT]).await;
    let mut deliveries = as_durable(m.as_ref())
        .unwrap()
        .consume(SUBJECT, "workers", &Options::default())
        .await
        .unwrap();
    m.publish(SUBJECT, event("", ""), &Options::default())
        .await
        .unwrap();
    let first = recv_delivery(&mut deliveries, WITHIN).await;
    unsupported(first.nak_after(QUICK_RECLAIM * 2).await);
    // It stays pending and comes back as a plain nak's would.
    let again = recv_delivery(&mut deliveries, WITHIN).await;
    assert!(again.attempt() >= 2);
    again.ack().await.unwrap();

    // With reclaiming off, nothing would redeliver: no delayed nak at all.
    let off = durable_with(RedisConfig {
        reclaim_after: Duration::ZERO,
        ..config()
    })
    .await;
    let (_, m) = bind(&off, &[SUBJECT]).await;
    let mut deliveries = as_durable(m.as_ref())
        .unwrap()
        .consume(SUBJECT, "workers", &Options::default())
        .await
        .unwrap();
    m.publish(SUBJECT, event("", ""), &Options::default())
        .await
        .unwrap();
    let first = recv_delivery(&mut deliveries, WITHIN).await;
    assert!(!first.can_delay_redelivery());
    unsupported(first.nak_after(Duration::from_millis(10)).await);
}
