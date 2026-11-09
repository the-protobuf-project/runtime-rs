//! An MQTT session outliving its consumer, end to end.
//!
//! Start a broker with `docker compose -f docker/compose.mqtt.yaml up -d
//! --wait`, then `cargo run --features mqtt --example mqtt`. Point it elsewhere
//! with `MQTT_HOST` and `MQTT_PORT` (default `127.0.0.1:11883`).

use std::time::Duration;

use futures::StreamExt;
use streams::{
    Deliveries, Delivery, Options, Payload, Stream, StreamError, Streams, as_durable,
    as_positioned,
    drivers::mqtt::{MqttConfig, MqttOptions, MqttProvider},
};

/// This program's model.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct Reading {
    device: String,
    value: f64,
}

/// A JSON-encoded reading.
fn reading(device: &str, value: f64) -> streams::Result<Payload> {
    Payload::json(&Reading {
        device: device.into(),
        value,
    })
}

/// The next delivery, or an error after 15s or if the session's stream ends.
async fn receive(deliveries: &mut Deliveries) -> Result<Delivery, Box<dyn std::error::Error>> {
    match tokio::time::timeout(Duration::from_secs(15), deliveries.next()).await {
        Ok(Some(delivery)) => Ok(delivery),
        Ok(None) => Err("the deliveries ended early".into()),
        Err(_) => Err("timed out waiting for a delivery".into()),
    }
}

/// Shows a named session keeping a message published while its consumer was
/// offline, and that MQTT refuses positioned reads.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = std::env::var("MQTT_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port = std::env::var("MQTT_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(11883);
    const SUBJECT: &str = "sensor/temperature";

    // The options are the connection; the provider opens one per session from
    // them, which is why it takes options and not a client.
    let provider = MqttProvider::connect(
        MqttOptions::new("example", host, port),
        MqttConfig {
            prefix: "example".into(),
            ..Default::default()
        },
    )
    .await?;

    let stream = provider
        .create(Stream {
            name: "sensors".into(),
            subjects: vec!["sensor/+".into()],
            ..Default::default()
        })
        .await?;
    let manager = provider.bind(&stream.id).await?;

    // MQTT is durable but not positioned: a session holds what a consumer has
    // not handled, but a session is a queue rather than a log, so there is
    // nothing to seek in.
    let durable = as_durable(manager.as_ref())?;
    if let Err(StreamError::Unsupported(why)) = as_positioned(manager.as_ref()) {
        println!("MQTT keeps no log to replay, as expected: {why}");
    }

    println!("--- a session outlives the consumer ---");

    // Attach once so the broker creates the session and its subscription.
    let mut first = durable
        .consume(SUBJECT, "recorder", &Options::default())
        .await?;
    manager
        .publish(SUBJECT, reading("kitchen", 21.5)?, &Options::default())
        .await?;
    let delivery = receive(&mut first).await?;
    println!("consumer one received {}", delivery.id());
    delivery.ack().await?;

    // The acknowledgement is a packet on the wire, so give it a moment to land
    // before dropping the connection, or the broker never hears it and
    // delivers the message again.
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(first);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Published while nobody is attached. A clean session would drop this; the
    // named one keeps it.
    manager
        .publish(SUBJECT, reading("kitchen", 22.0)?, &Options::default())
        .await?;
    println!("published with the consumer offline");

    let mut again = durable
        .consume(SUBJECT, "recorder", &Options::default())
        .await?;
    let resumed = receive(&mut again).await?;
    println!("the session had kept it: {:?}", resumed.json::<Reading>()?);
    resumed.ack().await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    drop(again);
    provider.delete(&stream.id).await?;
    provider.close().await?;
    println!("done");
    Ok(())
}
