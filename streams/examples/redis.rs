//! The three Redis delivery modes, end to end.
//!
//! Start a server with `docker compose -f docker/compose.redis.yaml up -d
//! --wait`, then `cargo run --features redis --example redis`. Point it
//! elsewhere with `REDIS_ADDR` (default `127.0.0.1:16380`).

use std::time::Duration;

use futures::StreamExt;
use streams::{
    Options, Payload, Stream, StreamError, Streams, as_durable,
    drivers::redis::{RedisConfig, RedisProvider, redis},
};

/// This program's model.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct Event {
    user: String,
    action: String,
}

/// A JSON-encoded event.
fn event(user: &str, action: &str) -> streams::Result<Payload> {
    Payload::json(&Event {
        user: user.into(),
        action: action.into(),
    })
}

/// Walks the immediate, scheduled and durable providers on one client, with
/// a short ack timeout so the durable redelivery shows quickly.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("REDIS_ADDR").unwrap_or_else(|_| "127.0.0.1:16380".into());
    // The client is yours; the providers open connections from it and never
    // close anything you made.
    let client = redis::Client::open(format!("redis://{addr}"))?;
    let config = RedisConfig {
        prefix: "example".into(),
        // Short so this demo does not wait thirty seconds to show a
        // redelivery. In a real program it belongs above your slowest handler.
        reclaim_after: Duration::from_secs(2),
        ..RedisConfig::default()
    };

    immediate(RedisProvider::new(client.clone(), config.clone())).await?;
    scheduled(RedisProvider::scheduled(client.clone(), config.clone())).await?;
    durable(RedisProvider::durable(client, config)).await?;
    println!("done");
    Ok(())
}

/// Delivery at publish time, over pub/sub.
async fn immediate(provider: RedisProvider) -> streams::Result<()> {
    println!("--- immediate ---");
    let subject = "user.created";
    let stream = provider
        .create(Stream {
            name: "users".into(),
            subjects: vec![subject.into()],
            ..Stream::default()
        })
        .await?;
    let m = provider.bind(&stream.id).await?;

    // Subscribe first: it is live when this returns, so a publish afterwards
    // is delivered rather than raced.
    let mut messages = m.subscribe(subject, &Options::default()).await?;
    m.publish(subject, event("alice", "created")?, &Options::default())
        .await?;
    if let Some(message) = messages.next().await {
        println!(
            "received {} -> {:?}",
            message.id(),
            message.json::<Event>()?
        );
    }

    if let Err(StreamError::UnknownSubject { .. }) = m
        .publish("user.typo", event("", "")?, &Options::default())
        .await
    {
        println!("an undeclared subject was rejected, as expected");
    }
    let delayed = Options::default().with_ttl(Duration::from_secs(1));
    if let Err(StreamError::Unsupported(_)) = m.publish(subject, event("", "")?, &delayed).await {
        println!("an immediate stream rejected a TTL, as expected");
    }
    provider.delete(&stream.id).await
}

/// Delivery when a TTL expires, over keyspace events. The server needs
/// `--notify-keyspace-events Ex`, which the compose file sets.
async fn scheduled(provider: RedisProvider) -> streams::Result<()> {
    println!("--- scheduled ---");
    let subject = "reminder";
    let stream = provider
        .create(Stream {
            name: "reminders".into(),
            subjects: vec![subject.into()],
            ..Stream::default()
        })
        .await?;
    let m = provider.bind(&stream.id).await?;
    let mut reminders = m.subscribe(subject, &Options::default()).await?;

    // The expiry is the delivery, so a TTL is required here.
    let in_a_second = Options::default().with_ttl(Duration::from_secs(1));
    m.publish(subject, event("alice", "take a pill")?, &in_a_second)
        .await?;
    println!("scheduled a reminder for 1s from now");
    match tokio::time::timeout(Duration::from_secs(10), reminders.next()).await {
        Ok(Some(message)) => println!("reminder fired: {:?}", message.json::<Event>()?),
        _ => println!("the reminder never fired (is --notify-keyspace-events Ex set?)"),
    }
    provider.delete(&stream.id).await
}

/// Delivery that survives a consumer dying, over Redis Streams.
async fn durable(provider: RedisProvider) -> streams::Result<()> {
    println!("--- durable ---");
    let subject = "order.placed";
    let stream = provider
        .create(Stream {
            name: "orders".into(),
            subjects: vec![subject.into()],
            ..Stream::default()
        })
        .await?;
    let m = provider.bind(&stream.id).await?;
    let d = as_durable(m.as_ref())?;

    // The first consumer takes the message and dies without acknowledging,
    // which is the case the whole capability exists for.
    let mut first = d.consume(subject, "billing", &Options::default()).await?;
    m.publish(subject, event("alice", "ordered")?, &Options::default())
        .await?;
    if let Some(delivery) = first.next().await {
        println!(
            "consumer one took {} on attempt {}, then died without acknowledging",
            delivery.id(),
            delivery.attempt()
        );
    }
    drop(first);

    // A second consumer under the same name is handed it back.
    let mut second = d.consume(subject, "billing", &Options::default()).await?;
    if let Ok(Some(delivery)) = tokio::time::timeout(Duration::from_secs(20), second.next()).await {
        println!(
            "consumer two was handed it back on attempt {} -> {:?}",
            delivery.attempt(),
            delivery.json::<Event>()?
        );
        // Acknowledge after the work, not on receipt.
        delivery.ack().await?;
        println!("acknowledged; it will not be delivered again");
    }
    drop(second);
    provider.delete(&stream.id).await
}
