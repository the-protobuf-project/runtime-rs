//! OPC UA PubSub over MQTT: read what Microsoft's OPC Publisher publishes
//! from the opc-plc simulator, and publish a DataSet of our own beside it.
//!
//! ```bash
//! docker compose -f streams/docker/compose.opcua.pubsub.yaml up -d --wait
//! cargo run -p tpp-streams --features opcua --example opcua_pubsub
//! ```
//!
//! `OPCUA_PUBSUB_MQTT_ADDR` (default `127.0.0.1:11885`) points it elsewhere.

use std::time::Duration;

use futures::StreamExt;
use streams::{
    Options, Stream, Streams,
    drivers::opcua::{
        DataValue, OpcUaValue,
        pubsub::{DataSet, MqttOptions, PubSubConfig, PubSubProvider},
    },
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("OPCUA_PUBSUB_MQTT_ADDR").unwrap_or_else(|_| "127.0.0.1:11885".into());
    let (host, port) = addr.rsplit_once(':').ok_or("host:port")?;
    let mut options = MqttOptions::new("example", host, port.parse()?);
    options.set_max_packet_size(Some(1 << 20));
    let provider = PubSubProvider::connect(
        options,
        PubSubConfig {
            publisher_id: "tpp-example".into(),
            ..PubSubConfig::default()
        },
    )
    .await?;

    // OPC Publisher's WriterGroup `plc`, writer `simulation`: read-only here.
    provider
        .create(Stream {
            id: "opc-publisher/plc".into(),
            subjects: vec!["simulation".into()],
            ..Default::default()
        })
        .await?;
    let plc = provider.bind("opc-publisher/plc").await?;
    let mut values = plc.subscribe("simulation", &Options::default()).await?;

    // Our own WriterGroup, with metadata announced and retained.
    provider
        .create(Stream {
            id: "line-a".into(),
            subjects: vec!["boiler".into()],
            ..Default::default()
        })
        .await?;
    let line = provider.bind("line-a").await?;
    let set = DataSet::new()
        .with("Temperature", DataValue::new_now(81.5f64))
        .with("Running", DataValue::new_now(true));
    provider
        .publish_metadata("line-a", "boiler", set.metadata("boiler"))
        .await?;
    let id = line
        .publish("boiler", set.payload(), &Options::default())
        .await?;
    println!(
        "published {id} on {}",
        provider.data_topic("line-a", "boiler")?
    );

    for _ in 0..6 {
        let Ok(Some(message)) = tokio::time::timeout(Duration::from_secs(30), values.next()).await
        else {
            break;
        };
        let value = OpcUaValue::data_value(&message)?;
        println!(
            "{} = {:?} at {:?}",
            message.subject(),
            value.value,
            value.source_timestamp
        );
    }
    provider.close().await?;
    Ok(())
}
