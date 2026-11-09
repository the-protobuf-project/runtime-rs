//! Publishes a tick over ZeroMQ and receives it, with no broker in between.
//!
//! Run with `cargo run -p tpp-streams --example zeromq --features zeromq`.

use std::time::Duration;

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use streams::{
    Manager, Options, Payload, Stream, StreamError, Streams,
    drivers::zeromq::{ZmqConfig, ZmqProvider},
};

/// This program's model.
#[derive(Debug, Deserialize, Serialize)]
struct Tick {
    symbol: String,
    price: f64,
}

/// Both sides declare the stream under this id, because the id is part of the
/// wire topic and two processes have to agree on it.
const STREAM_ID: &str = "market";
const SUBJECT: &str = "tick";

/// Binds a publisher and connects a subscriber in one process, sends a tick,
/// and shows the subscriber refusing to publish and to be durable.
#[tokio::main]
async fn main() -> streams::Result<()> {
    // The publisher binds and the subscriber connects. They are separate
    // constructors because getting the role wrong in ZeroMQ is a silent no-op.
    // In a real program these are two processes; port 0 lets the system pick.
    let publisher = ZmqProvider::publisher(ZmqConfig::new("tcp://127.0.0.1:0")).await?;
    let subscriber = ZmqProvider::subscriber(
        // The subscription reaches the publisher asynchronously and is never
        // acknowledged, so subscribe waits this long before returning. It
        // narrows the slow-joiner window; nothing closes it.
        ZmqConfig::new(publisher.endpoint()).with_settle(Duration::from_millis(500)),
    )?;

    let pm = declare(&publisher).await?;
    let sm = declare(&subscriber).await?;

    let mut ticks = sm.subscribe(SUBJECT, &Options::default()).await?;
    let tick = Tick {
        symbol: "ACME".into(),
        price: 42.5,
    };
    pm.publish(SUBJECT, Payload::json(&tick)?, &Options::default())
        .await?;

    match tokio::time::timeout(Duration::from_secs(10), ticks.next()).await {
        Ok(Some(message)) => println!("received {} -> {:?}", message.id(), message.json::<Tick>()?),
        _ => return Err(StreamError::Internal("nothing was delivered".into())),
    }

    // A SUB socket cannot send, and refusing beats a message that goes nowhere.
    if let Err(StreamError::Unsupported(why)) = sm
        .publish(SUBJECT, Payload::json(&tick)?, &Options::default())
        .await
    {
        println!("the subscriber refused to publish: {why}");
    }

    // Brokerless means nothing is stored, so there is nothing to redeliver.
    if let Err(error) = streams::as_durable(pm.as_ref()) {
        println!("not durable, as expected: {error}");
    }

    drop(ticks);
    subscriber.close().await?;
    publisher.close().await?;
    println!("done");
    Ok(())
}

/// Declares the stream on one side and binds a manager to it.
async fn declare(provider: &ZmqProvider) -> streams::Result<std::sync::Arc<dyn Manager>> {
    provider
        .create(Stream {
            id: STREAM_ID.into(),
            name: "market data".into(),
            subjects: vec![SUBJECT.into()],
            ..Stream::default()
        })
        .await?;
    provider.bind(STREAM_ID).await
}
