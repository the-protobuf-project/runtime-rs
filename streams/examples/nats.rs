//! Core NATS and JetStream through one contract.
//!
//! Start a server with `docker compose -f streams/docker/compose.nats.yaml up
//! -d --wait`, then `cargo run -p tpp-streams --features nats --example nats`.
//! `NATS_URL` overrides the default `nats://127.0.0.1:14222`.

use std::time::Duration;

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use streams::{
    Options, Position, Stream, StreamError, Streams, as_durable, as_positioned,
    drivers::nats::{JetStreamProvider, NatsConfig, NatsProvider},
    typed,
};

#[derive(Debug, Deserialize, Serialize)]
struct Event {
    user: String,
    action: String,
}

/// Runs the same contract over core NATS and JetStream on one client.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:14222".into());
    // The client is yours: credentials, TLS, and reconnect handling are set
    // here, and the providers neither dial nor close it.
    let client = async_nats::connect(&url).await?;

    run_core(NatsProvider::new(client.clone(), NatsConfig::default())).await?;
    run_jetstream(JetStreamProvider::new(client, NatsConfig::default()).await?).await?;
    println!("done");
    Ok(())
}

/// Core NATS: delivery to whoever is listening, and nothing kept.
async fn run_core(provider: NatsProvider) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- core NATS ---");
    let stream = provider
        .create(Stream {
            name: "users".into(),
            description: "user lifecycle events".into(),
            subjects: vec!["user.*".into()],
            ..Stream::default()
        })
        .await?;
    let manager = provider.bind(&stream.id).await?;

    // Subscribe first: the subscription is live by the time this returns, so a
    // value published afterwards is delivered rather than raced.
    let users = typed::<Event>(manager.clone());
    let mut created = users.subscribe("user.created", &Options::default()).await?;
    users
        .publish(
            "user.created",
            &Event {
                user: "alice".into(),
                action: "created".into(),
            },
            &Options::default(),
        )
        .await?;
    match tokio::time::timeout(Duration::from_secs(5), created.next()).await? {
        Some(event) => println!("received {event:?}"),
        None => return Err("the subscription ended".into()),
    }

    // The stream declares a wildcard, so it accepts any single token under
    // "user.", but a message still has to land somewhere specific.
    let wildcard = users
        .publish(
            "user.*",
            &Event {
                user: String::new(),
                action: String::new(),
            },
            &Options::default(),
        )
        .await;
    if let Err(StreamError::UnknownSubject { .. }) = wildcard {
        println!("publishing to a wildcard was rejected, as expected");
    }

    // Core NATS keeps no log, so it refuses the durable half by name.
    if let Err(error) = as_durable(manager.as_ref()) {
        println!("core NATS is not durable, as expected: {error}");
    }
    provider.delete(&stream.id).await?;
    Ok(())
}

/// JetStream: the same contract backed by a stored log.
async fn run_jetstream(provider: JetStreamProvider) -> Result<(), Box<dyn std::error::Error>> {
    println!("--- JetStream ---");
    let stream = provider
        .create(Stream {
            name: "orders".into(),
            subjects: vec!["order.*".into()],
            ..Stream::default()
        })
        .await?;
    let manager = provider.bind(&stream.id).await?;
    let orders = typed::<Event>(manager.clone());

    // Published before anyone consumes: the log keeps it.
    orders
        .publish(
            "order.placed",
            &Event {
                user: "bob".into(),
                action: "placed".into(),
            },
            &Options::default(),
        )
        .await?;

    // A named consumer reading from the start of the log. The name is the
    // position that survives a restart.
    let mut deliveries = as_positioned(manager.as_ref())?
        .consume_from(
            "order.placed",
            "billing",
            Position::Earliest,
            &Options::default(),
        )
        .await?;
    let delivery = tokio::time::timeout(Duration::from_secs(5), deliveries.next())
        .await?
        .ok_or("the consumer ended")?;
    let event: Event = delivery.json()?;
    println!(
        "delivery {} (attempt {}) -> {event:?}",
        delivery.id(),
        delivery.attempt()
    );
    // Ack after the work is done, not on receipt.
    delivery.ack().await?;

    drop(deliveries);
    provider.delete(&stream.id).await?;
    Ok(())
}
