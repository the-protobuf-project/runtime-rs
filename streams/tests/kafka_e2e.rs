//! The Kafka provider end to end, over mutual TLS, against a durable broker.
//!
//! Start one with
//! `docker compose -p tpp-streams-e2e-kafka -f docker/compose.kafka.secure.yaml up -d --wait`
//! (certificates from `docker/tls/generate.sh`), then
//! `cargo test -p tpp-streams --features kafka,tls,live-tests --test kafka_e2e`.
//!
//! `KAFKA_E2E_ADDR` overrides the broker (default `127.0.0.1:19093`) and
//! `STREAMS_E2E_TLS_DIR` the certificate directory (default `docker/tls/out`).
//! The restart test runs only with `STREAMS_E2E_RESTART=1`, because it restarts
//! the `tpp-streams-kafka-secure` container.

use std::{
    collections::HashSet,
    path::PathBuf,
    process::Command,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::{StreamExt, stream::BoxStream};
use streams::{
    Deliveries, Delivery, Manager, Message, Options, Payload, Position, Stream, StreamError,
    Streams, as_batch, as_durable, as_positioned,
    drivers::kafka::{
        KafkaConfig, KafkaProvider,
        rdkafka::{
            ClientConfig,
            consumer::{BaseConsumer, Consumer},
        },
    },
};

const SUBJECT: &str = "user.created";
const OTHER: &str = "user.deleted";
const CONTAINER: &str = "tpp-streams-kafka-secure";
const WITHIN: Duration = Duration::from_secs(30);

/// Every test holds this shared; the restart test holds it exclusively, so it
/// never takes the broker down under another test in the same binary.
static BROKER: tokio::sync::RwLock<()> = tokio::sync::RwLock::const_new(());

#[derive(Debug, Clone, PartialEq, serde::Deserialize, serde::Serialize)]
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
    std::env::var("KAFKA_E2E_ADDR").unwrap_or_else(|_| "127.0.0.1:19093".into())
}

fn certs() -> PathBuf {
    std::env::var("STREAMS_E2E_TLS_DIR").map_or_else(
        |_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("docker/tls/out"),
        PathBuf::from,
    )
}

fn cert(name: &str) -> String {
    let path = certs().join(name);
    assert!(
        path.exists(),
        "{} is missing; run docker/tls/generate.sh",
        path.display()
    );
    path.to_string_lossy().into_owned()
}

/// A client config for the secure listener: TLS, verifying the broker against
/// the test CA, presenting the client certificate when `mutual`.
fn tls_client(mutual: bool) -> ClientConfig {
    let mut client = ClientConfig::new();
    client
        .set("bootstrap.servers", addr())
        .set("security.protocol", "ssl")
        .set("ssl.ca.location", cert("ca.crt"))
        .set("ssl.endpoint.identification.algorithm", "https");
    if mutual {
        client
            .set("ssl.certificate.location", cert("client.crt"))
            .set("ssl.key.location", cert("client.key"));
    }
    client
}

fn fresh_prefix() -> String {
    format!("e2e-{}", streams::provider::new_id().to_lowercase())
}

async fn provider_at(prefix: &str, partitions: i32) -> KafkaProvider {
    KafkaProvider::connect(
        tls_client(true),
        KafkaConfig {
            prefix: prefix.into(),
            partitions,
            ..KafkaConfig::default()
        },
    )
    .await
    .expect("secure Kafka is not reachable; start docker/compose.kafka.secure.yaml")
}

async fn provider(partitions: i32) -> (KafkaProvider, String) {
    let prefix = fresh_prefix();
    (provider_at(&prefix, partitions).await, prefix)
}

async fn declare(provider: &KafkaProvider) -> (Stream, Arc<dyn Manager>) {
    let stream = provider
        .create(Stream {
            name: "e2e".into(),
            subjects: vec![SUBJECT.into()],
            ..Stream::default()
        })
        .await
        .unwrap();
    let manager = provider.bind(&stream.id).await.unwrap();
    (stream, manager)
}

async fn next(deliveries: &mut Deliveries) -> Delivery {
    tokio::time::timeout(WITHIN, deliveries.next())
        .await
        .expect("no delivery in time")
        .expect("deliveries ended before a message arrived")
}

async fn next_message(messages: &mut BoxStream<'static, Message>) -> Message {
    tokio::time::timeout(WITHIN, messages.next())
        .await
        .expect("nothing was delivered in time")
        .expect("the subscription ended")
}

/// Every topic name on the broker, read over TLS with the test's own client.
fn topics() -> Vec<String> {
    let consumer: BaseConsumer = tls_client(true).create().unwrap();
    consumer
        .fetch_metadata(None, Duration::from_secs(10))
        .unwrap()
        .topics()
        .iter()
        .map(|topic| topic.name().to_owned())
        .collect()
}

/// Every consumer group the broker knows.
fn groups() -> Vec<String> {
    let consumer: BaseConsumer = tls_client(true).create().unwrap();
    consumer
        .fetch_group_list(None, Duration::from_secs(10))
        .unwrap()
        .groups()
        .iter()
        .map(|group| group.name().to_owned())
        .collect()
}

/// Waits until no topic carries `stream`'s name under `prefix`. Topic deletion
/// is asynchronous on the broker, so a delete that returned may still be
/// finishing.
async fn assert_topics_gone(prefix: &str, stream: &str) {
    let marker = format!("{prefix}.{stream}.");
    let deadline = Instant::now() + WITHIN;
    loop {
        let left: Vec<String> = tokio::task::spawn_blocking(topics)
            .await
            .unwrap()
            .into_iter()
            .filter(|topic| topic.starts_with(&marker))
            .collect();
        if left.is_empty() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "topics left behind after delete: {left:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

// ---------------------------------------------------------------------------
// TLS is really in use
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn plaintext_is_refused_on_the_secure_port() {
    let _broker = BROKER.read().await;
    // The broker is up for a mutual-TLS client, so the refusal below is
    // the listener's doing and not an outage.
    provider(1).await;
    let mut client = ClientConfig::new();
    client
        .set("bootstrap.servers", addr())
        .set("security.protocol", "plaintext");
    let result = KafkaProvider::connect(client, KafkaConfig::default()).await;
    assert!(
        result.is_err(),
        "a plaintext client reached the TLS listener"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn tls_without_a_client_certificate_is_refused() {
    let _broker = BROKER.read().await;
    // The broker is up for a mutual-TLS client, so the refusal below is
    // the listener's doing and not an outage.
    provider(1).await;
    let result = KafkaProvider::connect(tls_client(false), KafkaConfig::default()).await;
    assert!(
        result.is_err(),
        "the broker accepted a TLS client with no certificate"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn tls_with_an_untrusted_broker_ca_is_refused() {
    let _broker = BROKER.read().await;
    // The broker is up for a mutual-TLS client, so the refusal below is
    // the listener's doing and not an outage.
    provider(1).await;
    // The client certificate is not a CA, so verifying the broker against it
    // must fail: the client really checks who it is talking to.
    let mut client = tls_client(true);
    client.set("ssl.ca.location", cert("client.crt"));
    let result = KafkaProvider::connect(client, KafkaConfig::default()).await;
    assert!(
        result.is_err(),
        "the client trusted a broker its CA did not sign"
    );
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn lifecycle_over_tls() {
    let _broker = BROKER.read().await;
    let (provider, prefix) = provider(1).await;

    // Explicit id, sanitised the way Go sanitises it.
    let explicit = provider
        .create(Stream {
            id: "orders:eu".into(),
            name: "orders".into(),
            description: "before".into(),
            subjects: vec![SUBJECT.into()],
            user_id: "u-1".into(),
            active: false,
        })
        .await
        .unwrap();
    assert_eq!(explicit.id, "orders-eu");
    assert!(explicit.active);

    // Generated id.
    let generated = provider
        .create(Stream {
            name: "users".into(),
            subjects: vec![SUBJECT.into()],
            ..Stream::default()
        })
        .await
        .unwrap();
    assert!(!generated.id.is_empty());
    assert_ne!(generated.id, explicit.id);

    let got = provider.get("orders:eu").await.unwrap();
    assert_eq!(got, explicit);

    let listed: Vec<String> = provider
        .list()
        .await
        .unwrap()
        .into_iter()
        .map(|stream| stream.id)
        .collect();
    let mut want = vec![explicit.id.clone(), generated.id.clone()];
    want.sort();
    assert_eq!(listed, want, "list is ordered by id and holds both");

    // Update adds a subject, which creates its topic.
    let updated = provider
        .update(
            &explicit.id,
            Stream {
                name: "orders".into(),
                description: "after".into(),
                subjects: vec![SUBJECT.into(), OTHER.into()],
                user_id: "u-2".into(),
                ..Stream::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.id, explicit.id);
    let got = provider.get(&explicit.id).await.unwrap();
    assert_eq!(
        (
            got.description.as_str(),
            got.user_id.as_str(),
            got.subjects.len()
        ),
        ("after", "u-2", 2)
    );
    let all = tokio::task::spawn_blocking(topics).await.unwrap();
    for subject in [SUBJECT, OTHER] {
        let name = format!("{prefix}.{}.{subject}", explicit.id);
        assert!(all.contains(&name), "{name} was not created");
    }

    // Bind, publish, subscribe on the added subject.
    let manager = provider.bind(&explicit.id).await.unwrap();
    let mut messages = manager.subscribe(OTHER, &Options::default()).await.unwrap();
    let id = manager
        .publish(OTHER, event("ada", "deleted"), &Options::default())
        .await
        .unwrap();
    let message = next_message(&mut messages).await;
    assert_eq!((message.id(), message.subject()), (id.as_str(), OTHER));
    assert_eq!(message.codec(), "json");
    assert_eq!(message.json::<Event>().unwrap().action, "deleted");
    drop(messages);

    // Delete both, then everything about them is NotFound.
    for stream in [&explicit, &generated] {
        provider.delete(&stream.id).await.unwrap();
        assert!(matches!(
            provider.get(&stream.id).await,
            Err(StreamError::NotFound(_))
        ));
        assert_topics_gone(&prefix, &stream.id).await;
    }
    assert!(provider.list().await.unwrap().is_empty());
    assert!(matches!(
        provider.bind(&explicit.id).await,
        Err(StreamError::NotFound(_))
    ));
    assert!(matches!(
        provider.update(&explicit.id, Stream::default()).await,
        Err(StreamError::NotFound(_))
    ));
    assert!(matches!(
        provider.delete(&explicit.id).await,
        Err(StreamError::NotFound(_))
    ));

    // Tails and metadata reads assign partitions rather than join a group, so
    // none of them leaves a group behind on the broker.
    let readers: Vec<String> = tokio::task::spawn_blocking(groups)
        .await
        .unwrap()
        .into_iter()
        .filter(|group| group.starts_with("streams-reader-"))
        .collect();
    assert!(readers.is_empty(), "reader groups left behind: {readers:?}");

    provider.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn undeclared_subjects_are_refused_everywhere() {
    let _broker = BROKER.read().await;
    let (provider, _) = provider(1).await;
    let (stream, manager) = declare(&provider).await;
    let unknown = |error: StreamError| matches!(error, StreamError::UnknownSubject { .. });

    assert!(unknown(
        manager
            .publish("typo", event("a", "b"), &Options::default())
            .await
            .unwrap_err()
    ));
    assert!(unknown(
        manager
            .subscribe("typo", &Options::default())
            .await
            .err()
            .unwrap()
    ));
    assert!(unknown(
        as_durable(manager.as_ref())
            .unwrap()
            .consume("typo", "c", &Options::default())
            .await
            .err()
            .unwrap()
    ));
    assert!(unknown(
        as_positioned(manager.as_ref())
            .unwrap()
            .consume_from("typo", "c", Position::Earliest, &Options::default())
            .await
            .err()
            .unwrap()
    ));
    assert!(unknown(
        as_batch(manager.as_ref())
            .unwrap()
            .publish_batch("typo", vec![event("a", "b")], &Options::default())
            .await
            .unwrap_err()
    ));
    provider.delete(&stream.id).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_typed_view_round_trips_over_tls() {
    let _broker = BROKER.read().await;
    let (provider, _) = provider(1).await;
    let (stream, manager) = declare(&provider).await;
    let typed = streams::typed::<Event>(manager);
    let mut events = typed.subscribe(SUBJECT, &Options::default()).await.unwrap();
    let sent = Event {
        user: "grace".into(),
        action: "typed".into(),
    };
    typed
        .publish(SUBJECT, &sent, &Options::default())
        .await
        .unwrap();
    let got = tokio::time::timeout(WITHIN, events.next())
        .await
        .expect("nothing typed arrived")
        .unwrap();
    assert_eq!(got, sent);
    drop(events);
    provider.delete(&stream.id).await.unwrap();
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn options_are_honoured_or_refused() {
    let _broker = BROKER.read().await;
    let (provider, _) = provider(1).await;
    let (stream, manager) = declare(&provider).await;

    // id: published under the caller's id.
    let mut messages = manager
        .subscribe(SUBJECT, &Options::default())
        .await
        .unwrap();
    let id = manager
        .publish(
            SUBJECT,
            event("ada", "id"),
            &Options::default().with_id("order-42"),
        )
        .await
        .unwrap();
    assert_eq!(id, "order-42");
    assert_eq!(next_message(&mut messages).await.id(), "order-42");
    drop(messages);

    // ttl: Kafka cannot schedule, so it is refused rather than dropped.
    let delayed = Options::default().with_ttl(Duration::from_secs(1));
    assert!(matches!(
        manager.publish(SUBJECT, event("a", "b"), &delayed).await,
        Err(StreamError::Unsupported(_))
    ));
    assert!(matches!(
        as_batch(manager.as_ref())
            .unwrap()
            .publish_batch(SUBJECT, vec![event("a", "b")], &delayed)
            .await,
        Err(StreamError::Unsupported(_))
    ));
    // A zero ttl means none, as in Go.
    manager
        .publish(
            SUBJECT,
            event("a", "zero-ttl"),
            &Options::default().with_ttl(Duration::ZERO),
        )
        .await
        .unwrap();

    // id on a batch: one id cannot name several messages.
    assert!(matches!(
        as_batch(manager.as_ref())
            .unwrap()
            .publish_batch(
                SUBJECT,
                vec![event("a", "b"), event("c", "d")],
                &Options::default().with_id("x")
            )
            .await,
        Err(StreamError::Unsupported(_))
    ));

    // group on a durable consumer: the consumer name already is the group.
    let durable = as_durable(manager.as_ref()).unwrap();
    assert!(matches!(
        durable
            .consume(SUBJECT, "billing", &Options::default().with_group("g"))
            .await
            .err()
            .unwrap(),
        StreamError::Unsupported(_)
    ));
    // An unnamed durable consumer has no position to keep.
    assert!(matches!(
        durable
            .consume(SUBJECT, "", &Options::default())
            .await
            .err()
            .unwrap(),
        StreamError::Unsupported(_)
    ));

    // prefetch: a window of one still delivers everything, in order.
    for n in 0..3 {
        manager
            .publish(SUBJECT, event("p", &n.to_string()), &Options::default())
            .await
            .unwrap();
    }
    let mut deliveries = as_positioned(manager.as_ref())
        .unwrap()
        .consume_from(
            SUBJECT,
            "prefetch",
            Position::Earliest,
            &Options::default().with_prefetch(1),
        )
        .await
        .unwrap();
    let mut seen = Vec::new();
    while seen.len() < 5 {
        let delivery = next(&mut deliveries).await;
        seen.push(delivery.json::<Event>().unwrap().action);
        delivery.ack().await.unwrap();
    }
    assert_eq!(seen, ["id", "zero-ttl", "0", "1", "2"]);
    provider.delete(&stream.id).await.unwrap();
}

/// Messages sharing a key land on one partition and keep their order across
/// a four-partition topic.
#[tokio::test(flavor = "multi_thread")]
async fn a_partition_key_keeps_order() {
    let _broker = BROKER.read().await;
    let (provider, _) = provider(4).await;
    let (stream, manager) = declare(&provider).await;
    let keyed = Options::default().with_partition_key("ada");
    let want: Vec<String> = (0..10).map(|n| n.to_string()).collect();
    for action in &want {
        manager
            .publish(SUBJECT, event("ada", action), &keyed)
            .await
            .unwrap();
    }
    let mut deliveries = as_positioned(manager.as_ref())
        .unwrap()
        .consume_from(SUBJECT, "ordering", Position::Earliest, &Options::default())
        .await
        .unwrap();
    for action in &want {
        let delivery = next(&mut deliveries).await;
        assert_eq!(&delivery.json::<Event>().unwrap().action, action);
        delivery.ack().await.unwrap();
    }
    drop(deliveries);
    provider.delete(&stream.id).await.unwrap();
}

/// Without a group every subscriber gets every message; with one, members
/// share the subject and each message reaches exactly one of them.
#[tokio::test(flavor = "multi_thread")]
async fn a_group_shares_a_subject() {
    let _broker = BROKER.read().await;
    let (provider, _) = provider(1).await;
    let (stream, manager) = declare(&provider).await;
    let shared = Options::default().with_group("workers");
    let mut a = manager.subscribe(SUBJECT, &shared).await.unwrap();
    let mut b = manager.subscribe(SUBJECT, &shared).await.unwrap();

    // Group membership settles after subscribe returns, and the second
    // member's join can rebalance the first, so probe until three probes in a
    // row arrive promptly: by then both members have joined.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut streak = 0;
    while streak < 3 {
        assert!(Instant::now() < deadline, "the group never settled");
        let probe = manager
            .publish(SUBJECT, event("probe", ""), &Options::default())
            .await
            .unwrap();
        let landed = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let got = tokio::select! {
                    m = a.next() => m,
                    m = b.next() => m,
                };
                match got {
                    Some(message) if message.id() == probe => return true,
                    Some(_) => continue,
                    None => return false,
                }
            }
        })
        .await
        .unwrap_or(false);
        streak = if landed { streak + 1 } else { 0 };
    }

    const TOTAL: usize = 10;
    let mut want = HashSet::new();
    for n in 0..TOTAL {
        want.insert(
            manager
                .publish(SUBJECT, event("w", &n.to_string()), &Options::default())
                .await
                .unwrap(),
        );
    }
    // Which member saw which message. A rebalance may legitimately hand a
    // message to the other member too (at-least-once), so the check is that
    // everything arrives and the subject is not fanned out to both.
    let (mut seen_a, mut seen_b) = (HashSet::new(), HashSet::new());
    let collect_until = Instant::now() + WITHIN;
    while !want
        .iter()
        .all(|id| seen_a.contains(id) || seen_b.contains(id))
        && Instant::now() < collect_until
    {
        let got = tokio::time::timeout(Duration::from_millis(500), async {
            tokio::select! {
                m = a.next() => m.map(|m| (true, m)),
                m = b.next() => m.map(|m| (false, m)),
            }
        })
        .await;
        if let Ok(Some((from_a, message))) = got {
            let id = message.id().to_owned();
            if from_a {
                seen_a.insert(id);
            } else {
                seen_b.insert(id);
            }
        }
    }
    let missing: Vec<_> = want
        .iter()
        .filter(|id| !seen_a.contains(*id) && !seen_b.contains(*id))
        .collect();
    assert!(
        missing.is_empty(),
        "grouped messages never arrived: {missing:?}"
    );
    assert!(
        !(want.is_subset(&seen_a) && want.is_subset(&seen_b)),
        "both members received every message: a group fanned out"
    );
    drop((a, b));
    provider.delete(&stream.id).await.unwrap();
}

// ---------------------------------------------------------------------------
// Capabilities
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn every_capability_is_present() {
    let _broker = BROKER.read().await;
    let (provider, _) = provider(1).await;
    let (stream, manager) = declare(&provider).await;
    assert!(as_durable(manager.as_ref()).is_ok());
    assert!(as_positioned(manager.as_ref()).is_ok());
    assert!(as_batch(manager.as_ref()).is_ok());
    provider.delete(&stream.id).await.unwrap();
}

/// Ack commits; nak leaves the offset where it was, so the name reads the
/// message again when it next attaches. Kafka keeps no delivery count, so the
/// attempt is zero both times, as the contract asks of a provider that cannot
/// count.
#[tokio::test(flavor = "multi_thread")]
async fn ack_commits_and_nak_redelivers() {
    let _broker = BROKER.read().await;
    let (provider, _) = provider(1).await;
    let (stream, manager) = declare(&provider).await;
    for who in ["handled", "retried"] {
        manager
            .publish(SUBJECT, event(who, ""), &Options::default())
            .await
            .unwrap();
    }
    let positioned = as_positioned(manager.as_ref()).unwrap();
    let mut deliveries = positioned
        .consume_from(SUBJECT, "billing", Position::Earliest, &Options::default())
        .await
        .unwrap();
    let first = next(&mut deliveries).await;
    assert_eq!(first.json::<Event>().unwrap().user, "handled");
    assert_eq!(first.attempt(), 0);
    first.ack().await.unwrap();
    let second = next(&mut deliveries).await;
    assert_eq!(second.json::<Event>().unwrap().user, "retried");
    let retried_id = second.id().to_owned();
    second.nak().await.unwrap();
    drop(deliveries);

    // Same name, New: the committed offset wins over the reset policy.
    let mut again = as_durable(manager.as_ref())
        .unwrap()
        .consume(SUBJECT, "billing", &Options::default())
        .await
        .unwrap();
    let redelivered = next(&mut again).await;
    assert_eq!(redelivered.id(), retried_id);
    assert_eq!(redelivered.attempt(), 0);
    redelivered.ack().await.unwrap();
    drop(again);
    provider.delete(&stream.id).await.unwrap();
}

/// Earliest replays history to a fresh name; New does not.
#[tokio::test(flavor = "multi_thread")]
async fn positioned_replay() {
    let _broker = BROKER.read().await;
    let (provider, _) = provider(1).await;
    let (stream, manager) = declare(&provider).await;
    let old = manager
        .publish(SUBJECT, event("old", ""), &Options::default())
        .await
        .unwrap();

    let positioned = as_positioned(manager.as_ref()).unwrap();
    let mut replay = positioned
        .consume_from(SUBJECT, "replay", Position::Earliest, &Options::default())
        .await
        .unwrap();
    let first = next(&mut replay).await;
    assert_eq!(first.id(), old);
    first.ack().await.unwrap();
    drop(replay);

    // A fresh name at New starts at the end. Its group joins after
    // consume_from returns, so probe until it reads, and check history never
    // shows up.
    let mut tail = positioned
        .consume_from(SUBJECT, "tail", Position::New, &Options::default())
        .await
        .unwrap();
    let deadline = Instant::now() + WITHIN;
    let probe = loop {
        assert!(Instant::now() < deadline, "the New consumer never read");
        let probe = manager
            .publish(SUBJECT, event("new", ""), &Options::default())
            .await
            .unwrap();
        if let Ok(Some(delivery)) =
            tokio::time::timeout(Duration::from_millis(500), tail.next()).await
        {
            assert_ne!(delivery.id(), old, "New replayed history");
            delivery.ack().await.unwrap();
            break probe;
        }
    };
    assert!(!probe.is_empty());
    drop(tail);
    provider.delete(&stream.id).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_native_batch_delivers_every_entry() {
    let _broker = BROKER.read().await;
    let (provider, _) = provider(1).await;
    let (stream, manager) = declare(&provider).await;
    const TOTAL: usize = 100;
    let ids = as_batch(manager.as_ref())
        .unwrap()
        .publish_batch(
            SUBJECT,
            (0..TOTAL).map(|n| event("b", &n.to_string())).collect(),
            &Options::default(),
        )
        .await
        .unwrap();
    assert_eq!(ids.len(), TOTAL);
    let unique: HashSet<_> = ids.iter().cloned().collect();
    assert_eq!(unique.len(), TOTAL, "an id was assigned twice");

    let mut deliveries = as_positioned(manager.as_ref())
        .unwrap()
        .consume_from(SUBJECT, "batch", Position::Earliest, &Options::default())
        .await
        .unwrap();
    let mut seen = HashSet::new();
    for _ in 0..TOTAL {
        let delivery = next(&mut deliveries).await;
        assert!(unique.contains(delivery.id()), "an id nobody published");
        seen.insert(delivery.id().to_owned());
        delivery.ack().await.unwrap();
    }
    assert_eq!(seen, unique);
    drop(deliveries);

    // An empty batch is not an error.
    assert!(
        as_batch(manager.as_ref())
            .unwrap()
            .publish_batch(SUBJECT, Vec::new(), &Options::default())
            .await
            .unwrap()
            .is_empty()
    );
    provider.delete(&stream.id).await.unwrap();
}

// ---------------------------------------------------------------------------
// Durability across a broker restart
// ---------------------------------------------------------------------------

fn health() -> String {
    let out = Command::new("docker")
        .args(["inspect", "-f", "{{.State.Health.Status}}", CONTAINER])
        .output()
        .expect("docker is not on PATH");
    String::from_utf8_lossy(&out.stdout).trim().to_owned()
}

async fn restart_broker() {
    let status = Command::new("docker")
        .args(["restart", "-t", "30", CONTAINER])
        .status()
        .expect("docker is not on PATH");
    assert!(status.success(), "docker restart {CONTAINER} failed");
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        let status = tokio::task::spawn_blocking(health).await.unwrap();
        if status == "healthy" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{CONTAINER} did not become healthy after restart (last: {status})"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    // The healthcheck speaks to the in-container plaintext listener; wait
    // until the TLS listener also answers a few metadata reads in a row, so
    // the tests that run after this one do not meet a broker still settling.
    let mut streak = 0;
    while streak < 3 {
        let ok = tokio::task::spawn_blocking(|| {
            let consumer: BaseConsumer = tls_client(true).create().unwrap();
            consumer
                .fetch_metadata(None, Duration::from_secs(5))
                .is_ok()
        })
        .await
        .unwrap();
        streak = if ok { streak + 1 } else { 0 };
        assert!(
            Instant::now() < deadline,
            "{CONTAINER} TLS listener never settled after restart"
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// An acked message stays acked and an unacked one is redelivered after the
/// broker restarts; the stream declaration and the log survive too.
#[tokio::test(flavor = "multi_thread")]
async fn acknowledged_state_survives_a_broker_restart() {
    if std::env::var("STREAMS_E2E_RESTART").as_deref() != Ok("1") {
        eprintln!("skipped: set STREAMS_E2E_RESTART=1 to restart {CONTAINER}");
        return;
    }
    let _broker = BROKER.write().await;
    let prefix = fresh_prefix();
    let (stream_id, acked, pending) = {
        let provider = provider_at(&prefix, 1).await;
        let (stream, manager) = declare(&provider).await;
        let acked = manager
            .publish(SUBJECT, event("acked", ""), &Options::default())
            .await
            .unwrap();
        let pending = manager
            .publish(SUBJECT, event("pending", ""), &Options::default())
            .await
            .unwrap();
        let mut deliveries = as_positioned(manager.as_ref())
            .unwrap()
            .consume_from(SUBJECT, "survivor", Position::Earliest, &Options::default())
            .await
            .unwrap();
        let first = next(&mut deliveries).await;
        assert_eq!(first.id(), acked);
        first.ack().await.unwrap();
        let second = next(&mut deliveries).await;
        assert_eq!(second.id(), pending);
        // Left unsettled on purpose.
        drop(second);
        drop(deliveries);
        provider.close().await.unwrap();
        (stream.id, acked, pending)
    };

    restart_broker().await;

    // A new provider, as a restarted process would build.
    let provider = provider_at(&prefix, 1).await;
    let stream = provider
        .get(&stream_id)
        .await
        .expect("the declaration did not survive");
    assert_eq!(stream.subjects, [SUBJECT]);
    let manager = provider.bind(&stream_id).await.unwrap();

    // The committed offset survived: the name resumes at the unacked message.
    let mut resumed = as_durable(manager.as_ref())
        .unwrap()
        .consume(SUBJECT, "survivor", &Options::default())
        .await
        .unwrap();
    let delivery = next(&mut resumed).await;
    assert_eq!(
        delivery.id(),
        pending,
        "the acked message came back, or the unacked one did not"
    );
    delivery.ack().await.unwrap();
    drop(resumed);

    // The log survived whole: a fresh name replays both from the start.
    let mut replay = as_positioned(manager.as_ref())
        .unwrap()
        .consume_from(SUBJECT, "auditor", Position::Earliest, &Options::default())
        .await
        .unwrap();
    for want in [&acked, &pending] {
        let delivery = next(&mut replay).await;
        assert_eq!(delivery.id(), want.as_str());
        delivery.ack().await.unwrap();
    }
    drop(replay);

    // And the restarted broker takes new publishes.
    let mut tail = manager
        .subscribe(SUBJECT, &Options::default())
        .await
        .unwrap();
    let id = manager
        .publish(SUBJECT, event("after", ""), &Options::default())
        .await
        .unwrap();
    assert_eq!(next_message(&mut tail).await.id(), id);
    drop(tail);

    provider.delete(&stream_id).await.unwrap();
    assert_topics_gone(&prefix, &stream_id).await;
    provider.close().await.unwrap();
}
