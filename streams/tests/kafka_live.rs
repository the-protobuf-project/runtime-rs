//! The Kafka provider against a real broker.
//!
//! Start one with `docker compose -f docker/compose.kafka.yaml up -d --wait`,
//! then `cargo test --features kafka,live-tests --test kafka_live`. Point it
//! elsewhere with `KAFKA_ADDR`. Every test runs under its own topic prefix, so
//! they share a broker without reading each other's streams.

use std::{process::Command, sync::Arc, time::Duration};

use futures::StreamExt;
use streams::{
    Deliveries, Delivery, Headers, Manager, Options, Payload, Position, Stream, StreamError,
    Streams, as_batch, as_durable, as_positioned,
    drivers::kafka::{
        KafkaConfig, KafkaProvider,
        rdkafka::{
            ClientConfig, Message as _, Offset, TopicPartitionList,
            consumer::{BaseConsumer, Consumer},
        },
    },
};

const SUBJECT: &str = "user.created";
const WITHIN: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize)]
struct Event {
    user: String,
    action: String,
}

/// A JSON `Event` payload; the user tells messages apart.
fn event(user: &str, action: &str) -> Payload {
    Payload::json(&Event {
        user: user.into(),
        action: action.into(),
    })
    .unwrap()
}

/// A plaintext client for `KAFKA_ADDR`, default the compose broker.
fn client() -> ClientConfig {
    let mut client = ClientConfig::new();
    client.set(
        "bootstrap.servers",
        std::env::var("KAFKA_ADDR").unwrap_or_else(|_| "127.0.0.1:19092".into()),
    );
    client
}

/// A provider under a prefix no other test uses.
async fn provider(partitions: i32) -> KafkaProvider {
    provider_with_prefix(partitions).await.0
}

/// A provider and the prefix its topics are under.
async fn provider_with_prefix(partitions: i32) -> (KafkaProvider, String) {
    let prefix = format!("live-{}", streams::provider::new_id().to_lowercase());
    let provider = KafkaProvider::connect(
        client(),
        KafkaConfig {
            prefix: prefix.clone(),
            partitions,
            ..KafkaConfig::default()
        },
    )
    .await
    .expect("Kafka is not reachable; start docker/compose.kafka.yaml");
    (provider, prefix)
}

/// Creates a stream over [`SUBJECT`] and binds a manager to it.
async fn declare(provider: &KafkaProvider) -> (Stream, Arc<dyn Manager>) {
    let stream = provider
        .create(Stream {
            name: "test".into(),
            subjects: vec![SUBJECT.into()],
            ..Stream::default()
        })
        .await
        .unwrap();
    let manager = provider.bind(&stream.id).await.unwrap();
    (stream, manager)
}

/// The next delivery within [`WITHIN`], panicking on a timeout or an ended stream.
async fn next(deliveries: &mut Deliveries) -> Delivery {
    tokio::time::timeout(WITHIN, deliveries.next())
        .await
        .expect("no delivery in time")
        .expect("deliveries ended before a message arrived")
}

#[tokio::test(flavor = "multi_thread")]
async fn lifecycle() {
    let provider = provider(1).await;
    let created = provider
        .create(Stream {
            name: "users".into(),
            description: "before".into(),
            subjects: vec![SUBJECT.into()],
            user_id: "u-1".into(),
            ..Stream::default()
        })
        .await
        .unwrap();
    assert!(!created.id.is_empty());

    // Kafka has nowhere on a topic to hang these, so they ride in the
    // compacted metadata topic. If that breaks they come back empty.
    let got = provider.get(&created.id).await.unwrap();
    assert_eq!(
        (
            got.name.as_str(),
            got.user_id.as_str(),
            got.description.as_str()
        ),
        ("users", "u-1", "before")
    );

    provider
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
    assert_eq!(
        provider.get(&created.id).await.unwrap().description,
        "after"
    );
    assert_eq!(provider.list().await.unwrap().len(), 1);

    provider.delete(&created.id).await.unwrap();
    assert!(matches!(
        provider.get(&created.id).await,
        Err(StreamError::NotFound(_))
    ));
    provider.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_stream_is_not_found() {
    let provider = provider(1).await;
    assert!(matches!(
        provider.get("absent").await,
        Err(StreamError::NotFound(_))
    ));
    assert!(matches!(
        provider.bind("absent").await,
        Err(StreamError::NotFound(_))
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn publish_and_subscribe() {
    let provider = provider(1).await;
    let (_, manager) = declare(&provider).await;

    // The tail is pinned at the end before subscribe returns, so no sleep.
    let mut messages = manager
        .subscribe(SUBJECT, &Options::default())
        .await
        .unwrap();
    let id = manager
        .publish(SUBJECT, event("ada", "created"), &Options::default())
        .await
        .unwrap();

    let message = tokio::time::timeout(WITHIN, messages.next())
        .await
        .expect("nothing was delivered")
        .unwrap();
    assert_eq!(message.id(), id);
    assert_eq!(
        message.json::<Event>().unwrap(),
        Event {
            user: "ada".into(),
            action: "created".into()
        }
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn consume_delivers_and_acknowledges() {
    let provider = provider(1).await;
    let (_, manager) = declare(&provider).await;
    manager
        .publish(SUBJECT, event("grace", ""), &Options::default())
        .await
        .unwrap();

    let positioned = as_positioned(manager.as_ref()).unwrap();
    // Earliest, so the publish above is in range: a fresh group starting at
    // the end would skip it.
    let mut deliveries = positioned
        .consume_from(SUBJECT, "billing", Position::Earliest, &Options::default())
        .await
        .unwrap();
    let delivery = next(&mut deliveries).await;
    assert_eq!(delivery.json::<Event>().unwrap().user, "grace");
    // Kafka cannot count redeliveries, and the contract asks for zero there.
    assert_eq!(delivery.attempt(), 0);
    delivery.ack().await.unwrap();
}

/// A committed offset is the position that survives the process.
#[tokio::test(flavor = "multi_thread")]
async fn a_consumer_resumes_where_the_name_left_off() {
    let provider = provider(1).await;
    let (_, manager) = declare(&provider).await;
    for who in ["first", "second"] {
        manager
            .publish(SUBJECT, event(who, ""), &Options::default())
            .await
            .unwrap();
    }
    let positioned = as_positioned(manager.as_ref()).unwrap();

    let mut first = positioned
        .consume_from(SUBJECT, "billing", Position::Earliest, &Options::default())
        .await
        .unwrap();
    next(&mut first).await.ack().await.unwrap();
    drop(first);

    // The same name, and this time New: the position is the committed offset,
    // not the reset policy, so it picks up at the second message.
    let mut again = as_durable(manager.as_ref())
        .unwrap()
        .consume(SUBJECT, "billing", &Options::default())
        .await
        .unwrap();
    assert_eq!(
        next(&mut again).await.json::<Event>().unwrap().user,
        "second"
    );
}

/// Messages sharing a key land on one partition and keep their order.
#[tokio::test(flavor = "multi_thread")]
async fn a_partition_key_keeps_order() {
    let provider = provider(4).await;
    let (_, manager) = declare(&provider).await;
    let keyed = Options::default().with_partition_key("ada");
    for action in ["one", "two"] {
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
    for want in ["one", "two"] {
        let delivery = next(&mut deliveries).await;
        assert_eq!(delivery.json::<Event>().unwrap().action, want);
        delivery.ack().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_batch_delivers_every_entry() {
    let provider = provider(1).await;
    let (_, manager) = declare(&provider).await;
    const TOTAL: usize = 25;
    let ids = as_batch(manager.as_ref())
        .unwrap()
        .publish_batch(
            SUBJECT,
            (0..TOTAL).map(|_| event("ada", "placed")).collect(),
            &Options::default(),
        )
        .await
        .unwrap();
    assert_eq!(ids.len(), TOTAL);
    let unique: std::collections::HashSet<_> = ids.iter().collect();
    assert_eq!(unique.len(), TOTAL, "an id was assigned twice");

    let mut deliveries = as_positioned(manager.as_ref())
        .unwrap()
        .consume_from(
            SUBJECT,
            "batch-reader",
            Position::Earliest,
            &Options::default(),
        )
        .await
        .unwrap();
    for _ in 0..TOTAL {
        let delivery = next(&mut deliveries).await;
        assert!(
            unique.contains(&delivery.id().to_owned()),
            "an id nobody published"
        );
        delivery.ack().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn connect_fails_without_a_broker_list() {
    assert!(
        KafkaProvider::connect(ClientConfig::new(), KafkaConfig::default())
            .await
            .is_err()
    );
}

// ---------------------------------------------------------------------------
// Headers
// ---------------------------------------------------------------------------

/// Headers from literal pairs, for comparing with what arrives.
fn headers<const N: usize>(pairs: [(&str, &str); N]) -> Headers {
    pairs.into_iter().collect()
}

/// A stream whose declaration carries default headers.
async fn declare_with_headers(provider: &KafkaProvider) -> (Stream, Arc<dyn Manager>) {
    let stream = provider
        .create(
            Stream::named("headers")
                .subject(SUBJECT)
                .header("lang", "rust")
                .header("site", "plant-3"),
        )
        .await
        .unwrap();
    let manager = provider.bind(&stream.id).await.unwrap();
    (stream, manager)
}

/// One record as Kafka holds it: its headers in order, then its payload.
type RawRecord = (Vec<(String, Vec<u8>)>, Vec<u8>);

/// Every record on `topic`, read raw with librdkafka from the start: the
/// headers as Kafka holds them and the payload bytes.
fn raw_records(topic: &str, want: usize) -> Vec<RawRecord> {
    use streams::drivers::kafka::rdkafka::message::Headers as _;
    let mut config = client();
    config
        .set("group.id", format!("raw-{}", streams::provider::new_id()))
        .set("enable.auto.commit", "false");
    let consumer: BaseConsumer = config.create().unwrap();
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, 0, Offset::Beginning)
        .unwrap();
    consumer.assign(&tpl).unwrap();
    let deadline = std::time::Instant::now() + WITHIN;
    let mut out = Vec::new();
    while out.len() < want {
        assert!(
            std::time::Instant::now() < deadline,
            "only {} raw records",
            out.len()
        );
        let Some(record) = consumer.poll(Duration::from_millis(500)) else {
            continue;
        };
        let record = record.unwrap();
        let headers = record
            .headers()
            .map(|h| {
                h.iter()
                    .map(|h| (h.key.to_owned(), h.value.unwrap_or_default().to_vec()))
                    .collect()
            })
            .unwrap_or_default();
        out.push((headers, record.payload().unwrap_or_default().to_vec()));
    }
    out
}

/// What Kafka's own console consumer, run inside the broker container,
/// prints for the headers of the first `want` records on `topic`.
fn console_headers(topic: &str, want: usize) -> Vec<String> {
    let container = std::env::var("KAFKA_CONTAINER").unwrap_or_else(|_| "tpp-streams-kafka".into());
    let out = Command::new("docker")
        .args([
            "exec",
            &container,
            "/opt/kafka/bin/kafka-console-consumer.sh",
        ])
        .args(["--bootstrap-server", "localhost:29092", "--topic", topic])
        .args(["--from-beginning", "--max-messages", &want.to_string()])
        .args(["--timeout-ms", "20000"])
        .args(["--property", "print.headers=true"])
        .args(["--property", "print.value=false"])
        .output()
        .expect("docker is not on PATH");
    assert!(
        out.status.success(),
        "kafka-console-consumer failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(str::to_owned)
        .collect()
}

/// Stream defaults arrive, a publish's own win, and they travel as Kafka
/// record headers: outside the payload, which stays the version-1 frame
/// runtime-go reads, and visible to Kafka's own tools.
#[tokio::test(flavor = "multi_thread")]
async fn headers_travel_as_record_headers() {
    let (provider, prefix) = provider_with_prefix(1).await;
    let (stream, manager) = declare_with_headers(&provider).await;

    let mut tail = manager
        .subscribe(SUBJECT, &Options::default())
        .await
        .unwrap();
    let overridden = Options::default()
        .header("site", "plant-4")
        .header("traceparent", "00-abc-def-01");
    manager
        .publish(SUBJECT, event("ada", "one"), &overridden)
        .await
        .unwrap();
    let message = tokio::time::timeout(WITHIN, tail.next())
        .await
        .unwrap()
        .unwrap();
    let want = headers([
        ("lang", "rust"),
        ("site", "plant-4"),
        ("traceparent", "00-abc-def-01"),
    ]);
    assert_eq!(message.headers(), &want);
    assert_eq!(message.json::<Event>().unwrap().action, "one");

    // Only the stream's defaults.
    manager
        .publish(SUBJECT, event("ada", "two"), &Options::default())
        .await
        .unwrap();
    let message = tokio::time::timeout(WITHIN, tail.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        message.headers(),
        &headers([("lang", "rust"), ("site", "plant-3")])
    );
    drop(tail);

    // As Kafka holds them: record headers with UTF-8 values, and a payload
    // that is the plain version-1 frame with no header text in it.
    let topic = format!("{prefix}.{}.{SUBJECT}", stream.id);
    let raw = tokio::task::spawn_blocking({
        let topic = topic.clone();
        move || raw_records(&topic, 2)
    })
    .await
    .unwrap();
    assert_eq!(
        raw[0].0,
        [
            ("lang".to_owned(), b"rust".to_vec()),
            ("site".to_owned(), b"plant-4".to_vec()),
            ("traceparent".to_owned(), b"00-abc-def-01".to_vec()),
        ]
    );
    for (_, payload) in &raw {
        assert_eq!(
            &payload[..2],
            [0xB7, 0x01],
            "the payload is a version-1 frame"
        );
        let text = String::from_utf8_lossy(payload);
        assert!(
            !text.contains("plant") && !text.contains("lang"),
            "headers leaked into the payload"
        );
    }

    // And Kafka's own console consumer sees them.
    let printed = tokio::task::spawn_blocking(move || console_headers(&topic, 2))
        .await
        .unwrap();
    assert_eq!(
        printed,
        [
            "lang:rust,site:plant-4,traceparent:00-abc-def-01",
            "lang:rust,site:plant-3"
        ],
    );
    provider.delete(&stream.id).await.unwrap();
}

/// A message with no headers is exactly what runtime-go writes: no header
/// block on the record, and it still round-trips.
#[tokio::test(flavor = "multi_thread")]
async fn a_message_without_headers_has_no_record_headers() {
    let (provider, prefix) = provider_with_prefix(1).await;
    let (stream, manager) = declare(&provider).await;
    let mut tail = manager
        .subscribe(SUBJECT, &Options::default())
        .await
        .unwrap();
    let id = manager
        .publish(SUBJECT, event("go", "peer"), &Options::default())
        .await
        .unwrap();
    let message = tokio::time::timeout(WITHIN, tail.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(message.id(), id);
    assert!(message.headers().is_empty());
    let topic = format!("{prefix}.{}.{SUBJECT}", stream.id);
    let raw = tokio::task::spawn_blocking({
        let topic = topic.clone();
        move || raw_records(&topic, 1)
    })
    .await
    .unwrap();
    assert!(
        raw[0].0.is_empty(),
        "a header-less publish wrote headers: {:?}",
        raw[0].0
    );
    let printed = tokio::task::spawn_blocking(move || console_headers(&topic, 1))
        .await
        .unwrap();
    assert_eq!(printed, ["NO_HEADERS"]);
    provider.delete(&stream.id).await.unwrap();
}

/// Every producer in the pool and every record of a batch carries the
/// headers, and a durable redelivery brings them back.
#[tokio::test(flavor = "multi_thread")]
async fn the_producer_pool_batches_and_redeliveries_carry_headers() {
    let provider = provider(1).await;
    let (stream, manager) = declare_with_headers(&provider).await;
    let opts = |n: usize| Options::default().header("n", n.to_string());

    // Sixteen unkeyed publishes, so they take turns over all eight producers.
    const SINGLES: usize = 16;
    let options: Vec<Options> = (0..SINGLES).map(opts).collect();
    let singles = futures::future::join_all(
        options
            .iter()
            .enumerate()
            .map(|(n, opts)| manager.publish(SUBJECT, event("pool", &n.to_string()), opts)),
    )
    .await;
    assert!(singles.iter().all(Result::is_ok));
    const BATCH: usize = 10;
    as_batch(manager.as_ref())
        .unwrap()
        .publish_batch(
            SUBJECT,
            (0..BATCH).map(|n| event("batch", &n.to_string())).collect(),
            &Options::default().header("site", "plant-9"),
        )
        .await
        .unwrap();

    let positioned = as_positioned(manager.as_ref()).unwrap();
    let mut deliveries = positioned
        .consume_from(SUBJECT, "headers", Position::Earliest, &Options::default())
        .await
        .unwrap();
    let mut naked = None;
    for index in 0..SINGLES + BATCH {
        let delivery = next(&mut deliveries).await;
        let body = delivery.json::<Event>().unwrap();
        assert_eq!(delivery.headers().get("lang"), Some("rust"), "{body:?}");
        if body.user == "pool" {
            assert_eq!(delivery.headers().get("n"), Some(body.action.as_str()));
            assert_eq!(delivery.headers().get("site"), Some("plant-3"));
        } else {
            assert_eq!(delivery.headers().get("site"), Some("plant-9"), "{body:?}");
        }
        // Kafka cannot hold a redelivery back; asking costs the delivery
        // nothing it would not lose to a plain nak.
        assert!(!delivery.can_delay_redelivery());
        // The second, so the first's ack gives the name a committed offset
        // to resume from.
        if index == 1 {
            naked = Some((delivery.id().to_owned(), delivery.headers().clone()));
            let error = delivery
                .nak_after(Duration::from_secs(1))
                .await
                .unwrap_err();
            assert!(matches!(error, StreamError::Unsupported(_)), "{error}");
        } else {
            delivery.ack().await.unwrap();
        }
    }
    drop(deliveries);

    // The unsettled second record holds the partition: the name reads it again,
    // headers and all.
    let (id, sent) = naked.unwrap();
    let mut again = as_durable(manager.as_ref())
        .unwrap()
        .consume(SUBJECT, "headers", &Options::default())
        .await
        .unwrap();
    let redelivered = next(&mut again).await;
    assert_eq!(redelivered.id(), id);
    assert_eq!(redelivered.headers(), &sent);
    redelivered.ack().await.unwrap();
    drop(again);
    provider.delete(&stream.id).await.unwrap();
}

/// Default headers ride in the stored declaration: get, list, and update see
/// them, and a bad one is refused at the call that set it.
#[tokio::test(flavor = "multi_thread")]
async fn stream_headers_are_stored_and_checked() {
    let provider = provider(1).await;
    let (stream, _) = declare_with_headers(&provider).await;
    let want = headers([("lang", "rust"), ("site", "plant-3")]);
    assert_eq!(stream.headers, want);
    assert_eq!(provider.get(&stream.id).await.unwrap().headers, want);
    assert_eq!(provider.list().await.unwrap()[0].headers, want);

    let updated = provider
        .update(
            &stream.id,
            Stream::named("headers")
                .subject(SUBJECT)
                .header("lang", "go"),
        )
        .await
        .unwrap();
    assert_eq!(updated.headers, headers([("lang", "go")]));
    assert_eq!(
        provider.get(&stream.id).await.unwrap().headers,
        updated.headers
    );

    for bad in ["has space", "has:colon", ""] {
        let error = provider
            .create(Stream::named("bad").subject(SUBJECT).header(bad, "x"))
            .await
            .unwrap_err();
        assert!(
            matches!(error, StreamError::InvalidHeader(_)),
            "{bad:?}: {error}"
        );
        let error = provider
            .update(
                &stream.id,
                Stream::named("bad").subject(SUBJECT).header(bad, "x"),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error, StreamError::InvalidHeader(_)),
            "{bad:?}: {error}"
        );
    }
    // The refused update left the declaration alone.
    assert_eq!(
        provider.get(&stream.id).await.unwrap().headers,
        updated.headers
    );
    assert_eq!(
        provider.list().await.unwrap().len(),
        1,
        "a refused create declared nothing"
    );
    provider.delete(&stream.id).await.unwrap();
}

/// What Kafka cannot do per message is refused by name, on publish and batch,
/// and a bad publish header is refused before anything is sent.
#[tokio::test(flavor = "multi_thread")]
async fn expiry_retain_and_bad_headers_are_refused() {
    let (provider, prefix) = provider_with_prefix(1).await;
    let (stream, manager) = declare(&provider).await;
    let batch = as_batch(manager.as_ref()).unwrap();
    for (opts, says) in [
        (
            Options::default().with_expiry(Duration::from_secs(5)),
            "expiry",
        ),
        (Options::default().with_retain(), "retain"),
    ] {
        for error in [
            manager
                .publish(SUBJECT, event("a", "b"), &opts)
                .await
                .unwrap_err(),
            batch
                .publish_batch(SUBJECT, vec![event("a", "b")], &opts)
                .await
                .unwrap_err(),
        ] {
            assert!(
                matches!(&error, StreamError::Unsupported(why) if why.contains(says)),
                "{error}"
            );
        }
    }
    for bad in [
        Options::default().header("has space", "x"),
        Options::default().header("lang", "two\nlines"),
        Options::default().header("big", "v".repeat(streams::core::MAX_VALUE + 1)),
    ] {
        for error in [
            manager
                .publish(SUBJECT, event("a", "b"), &bad)
                .await
                .unwrap_err(),
            batch
                .publish_batch(SUBJECT, vec![event("a", "b")], &bad)
                .await
                .unwrap_err(),
        ] {
            assert!(matches!(error, StreamError::InvalidHeader(_)), "{error}");
        }
    }
    // A zero expiry is none, and goes through.
    manager
        .publish(
            SUBJECT,
            event("a", "zero"),
            &Options::default().with_expiry(Duration::ZERO),
        )
        .await
        .unwrap();
    let topic = format!("{prefix}.{}.{SUBJECT}", stream.id);
    let raw = tokio::task::spawn_blocking(move || raw_records(&topic, 1))
        .await
        .unwrap();
    assert_eq!(raw.len(), 1);
    provider.delete(&stream.id).await.unwrap();
}
