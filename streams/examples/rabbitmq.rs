//! Queues a job on RabbitMQ, returns it with a negative acknowledgement, and
//! acknowledges it on the second attempt.
//!
//! ```text
//! docker compose -f streams/docker/compose.rabbitmq.yaml up -d --wait
//! cargo run -p tpp-streams --example rabbitmq --features rabbitmq
//! ```
//!
//! `AMQP_URL` points it at another broker.

use std::{sync::Arc, time::Duration};

use futures::StreamExt;
use lapin::{Connection, ConnectionProperties};
use serde::{Deserialize, Serialize};
use streams::{
    Deliveries, Delivery, Options, Payload, Stream, StreamError, Streams, as_durable,
    drivers::rabbitmq::{RabbitConfig, RabbitProvider},
};

/// This program's model.
#[derive(Debug, Deserialize, Serialize)]
struct Job {
    id: String,
    kind: String,
}

const SUBJECT: &str = "job.queued";

/// Queues a job, naks it straight back, and acks the redelivery.
#[tokio::main]
async fn main() -> streams::Result<()> {
    let url = std::env::var("AMQP_URL")
        .unwrap_or_else(|_| "amqp://guest:guest@127.0.0.1:15682/%2f".into());

    // The connection is this program's: the provider opens channels on it and
    // never closes it.
    let conn = Connection::connect(&url, ConnectionProperties::default())
        .await
        .map_err(|e| StreamError::internal("RabbitMQ is not reachable", e))?;
    let conn = Arc::new(conn);
    let provider =
        RabbitProvider::new(conn.clone(), RabbitConfig::default().with_prefix("example")).await?;

    let stream = provider
        .create(Stream {
            name: "jobs".into(),
            subjects: vec![SUBJECT.into()],
            ..Default::default()
        })
        .await?;
    let manager = provider.bind(&stream.id).await?;
    let durable = as_durable(manager.as_ref())?;

    let mut deliveries = durable
        .consume(SUBJECT, "workers", &Options::default())
        .await?;
    let job = Job {
        id: "j-1".into(),
        kind: "resize".into(),
    };
    manager
        .publish(SUBJECT, Payload::json(&job)?, &Options::default())
        .await?;

    // This is what RabbitMQ has that the others do not: a true negative
    // acknowledgement. Nak hands the message straight back rather than waiting
    // out a visibility timeout, as Redis does, or doing nothing until the
    // partition moves, as Kafka does.
    println!("--- nak returns it immediately ---");
    let first = receive(&mut deliveries).await?;
    println!(
        "received {} on attempt {}; returning it with nak",
        first.id(),
        first.attempt()
    );
    first.nak().await?;

    let second = receive(&mut deliveries).await?;
    let got: Job = second.json()?;
    println!(
        "handed straight back on attempt {} -> {got:?}",
        second.attempt()
    );

    // Acknowledge after the work, not on receipt.
    second.ack().await?;
    println!("acknowledged; it will not be delivered again");

    // Dropping the deliveries cancels the consumer and closes its channel. The
    // durable "workers" queue stays on the broker, as a durable consumer's should.
    drop(deliveries);
    provider.delete(&stream.id).await?;
    provider.close().await?;
    conn.close(200, "OK".into())
        .await
        .map_err(|e| StreamError::internal("cannot close the connection", e))?;
    println!("done");
    Ok(())
}

/// The next delivery, or an error after 15s or if the consumer ends.
async fn receive(deliveries: &mut Deliveries) -> streams::Result<Delivery> {
    tokio::time::timeout(Duration::from_secs(15), deliveries.next())
        .await
        .map_err(|_| StreamError::Internal("timed out waiting for a delivery".into()))?
        .ok_or_else(|| StreamError::Internal("the deliveries ended early".into()))
}
