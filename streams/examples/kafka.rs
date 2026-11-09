//! Ordering by partition key and a durable consumer, over Kafka.
//!
//! ```text
//! docker compose -f docker/compose.kafka.yaml up -d --wait
//! cargo run --features kafka --example kafka
//! ```
//!
//! Set `KAFKA_ADDR` to use a broker other than `127.0.0.1:19092`.

use std::time::Duration;

use futures::StreamExt;
use streams::{
    Options, Position, Stream, Streams, as_positioned,
    drivers::kafka::{KafkaConfig, KafkaProvider, rdkafka::ClientConfig},
    typed,
};

/// This program's model.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct Order {
    account: String,
    action: String,
}

const SUBJECT: &str = "order.placed";

/// Publishes three orders under one partition key, then reads them back in
/// order through a consumer group starting from the earliest offset.
#[tokio::main]
async fn main() -> streams::Result<()> {
    // The config is yours: TLS and SASL go here, and every client the provider
    // builds, one per named consumer, is made from a clone of it.
    let mut client = ClientConfig::new();
    client.set(
        "bootstrap.servers",
        std::env::var("KAFKA_ADDR").unwrap_or_else(|_| "127.0.0.1:19092".into()),
    );
    let provider = KafkaProvider::connect(
        client,
        KafkaConfig {
            prefix: "example".into(),
            // More than one partition, so the partition key has something to
            // choose between. With one, everything is ordered and it is moot.
            partitions: 4,
            ..KafkaConfig::default()
        },
    )
    .await?;

    let stream = provider
        .create(Stream {
            name: "orders".into(),
            subjects: vec![SUBJECT.into()],
            ..Stream::default()
        })
        .await?;
    let manager = provider.bind(&stream.id).await?;

    // Kafka orders within a partition and nowhere else, so messages that must
    // be seen in order share a key.
    println!("--- ordering by partition key ---");
    let orders = typed::<Order>(manager.clone());
    let keyed = Options::default().with_partition_key("acct-1");
    for action in ["placed", "paid", "shipped"] {
        let order = Order {
            account: "acct-1".into(),
            action: action.into(),
        };
        orders.publish(SUBJECT, &order, &keyed).await?;
    }
    println!("published three messages under one key, so they share a partition");

    // A named consumer is a Kafka consumer group. Earliest because the
    // messages above were published before it existed.
    let mut deliveries = as_positioned(manager.as_ref())?
        .consume_from(SUBJECT, "billing", Position::Earliest, &Options::default())
        .await?;
    for _ in 0..3 {
        let Ok(Some(delivery)) =
            tokio::time::timeout(Duration::from_secs(30), deliveries.next()).await
        else {
            eprintln!("no delivery within 30s");
            break;
        };
        let order: Order = delivery.json()?;
        println!("billing received {} for {}", order.action, order.account);
        // Ack after the work: it commits this offset and everything before it
        // in the partition.
        delivery.ack().await?;
    }
    // Dropping the stream leaves the group.
    drop(deliveries);

    provider.delete(&stream.id).await?;
    provider.close().await
}
