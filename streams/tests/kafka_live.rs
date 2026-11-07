//! The Kafka provider against a real broker.
//!
//! Start one with `docker compose -f docker/compose.kafka.yaml up -d --wait`,
//! then `cargo test --features kafka,live-tests --test kafka_live`. Point it
//! elsewhere with `KAFKA_ADDR`. Every test runs under its own topic prefix, so
//! they share a broker without reading each other's streams.

use std::{sync::Arc, time::Duration};

use futures::StreamExt;
use streams::{
    Deliveries, Delivery, Manager, Options, Payload, Position, Stream, StreamError, Streams,
    as_batch, as_durable, as_positioned,
    drivers::kafka::{KafkaConfig, KafkaProvider, rdkafka::ClientConfig},
};

const SUBJECT: &str = "user.created";
const WITHIN: Duration = Duration::from_secs(30);

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

/// A provider under a prefix no other test uses.
async fn provider(partitions: i32) -> KafkaProvider {
    let mut client = ClientConfig::new();
    client.set(
        "bootstrap.servers",
        std::env::var("KAFKA_ADDR").unwrap_or_else(|_| "127.0.0.1:19092".into()),
    );
    KafkaProvider::connect(
        client,
        KafkaConfig {
            prefix: format!("live-{}", streams::provider::new_id().to_lowercase()),
            partitions,
            ..KafkaConfig::default()
        },
    )
    .await
    .expect("Kafka is not reachable; start docker/compose.kafka.yaml")
}

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
