//! OPC UA PubSub interop: the provider subscribes to what Microsoft's OPC
//! Publisher, a third-party Part 14 implementation on the OPC Foundation's
//! .NET stack, publishes over MQTT from the opc-plc simulator.
//!
//! Start them with
//! `docker compose -f streams/docker/compose.opcua.pubsub.yaml up -d --wait`,
//! then run
//! `cargo test -p tpp-streams --features opcua,live-tests --test opcua_pubsub_live`.
//!
//! `OPCUA_PUBSUB_MQTT_ADDR` (default `127.0.0.1:11885`) points it elsewhere.
//! OPC Publisher sends a DataSetMessage each publishing interval (500 ms)
//! in which a value changed, so the first one can take a few seconds while it
//! connects to the PLC after a cold start.

use std::time::Duration;

use futures::StreamExt;
use streams::{
    Message, Options, Stream, StreamError, Streams, Subscription,
    drivers::opcua::{
        CODEC_NAME, OpcUaValue, Variant,
        pubsub::{MqttOptions, PubSubConfig, PubSubProvider},
    },
};
use tokio::time::timeout;

/// OPC Publisher's PublisherId, WriterGroup and DataSetWriter, from the
/// compose file and compose.opcua.pubsub.publishednodes.json.
const STREAM: &str = "opc-publisher/plc";
const WRITER: &str = "simulation";
const WITHIN: Duration = Duration::from_secs(60);

async fn connect() -> PubSubProvider {
    let addr =
        std::env::var("OPCUA_PUBSUB_MQTT_ADDR").unwrap_or_else(|_| "127.0.0.1:11885".into());
    let (host, port) = addr
        .rsplit_once(':')
        .expect("OPCUA_PUBSUB_MQTT_ADDR is host:port");
    let mut options = MqttOptions::new("template", host, port.parse().expect("a port"));
    options.set_connection_timeout(5);
    options.set_max_packet_size(Some(1 << 20));
    let provider = PubSubProvider::connect(
        options,
        PubSubConfig {
            publisher_id: format!("tpp-live-{}", ulid::Ulid::new().to_string().to_lowercase()),
            ..PubSubConfig::default()
        },
    )
    .await
    .expect("connect to the broker; is compose.opcua.pubsub.yaml up?");
    provider
        .create(Stream {
            id: STREAM.into(),
            subjects: vec![WRITER.into()],
            ..Default::default()
        })
        .await
        .unwrap();
    provider
}

async fn next(subscription: &mut Subscription) -> Message {
    timeout(WITHIN, subscription.next())
        .await
        .expect("OPC Publisher sent nothing within the deadline")
        .expect("the subscription ended")
}

fn as_u32(message: &Message) -> u32 {
    match OpcUaValue::data_value(message).unwrap().value {
        Some(Variant::UInt32(n)) => n,
        other => panic!("StepUp is a UInt32 on opc-plc, got {other:?}"),
    }
}

#[tokio::test]
async fn reads_opc_publisher_data_sets() {
    let provider = connect().await;
    let manager = provider.bind(STREAM).await.unwrap();
    let mut step_up = manager
        .subscribe(&format!("{WRITER}/StepUp"), &Options::default())
        .await
        .unwrap();
    let mut all = manager.subscribe(WRITER, &Options::default()).await.unwrap();

    // StepUp counts up on the PLC; OPC Publisher reports each new value.
    let first = next(&mut step_up).await;
    assert_eq!(first.subject(), "simulation/StepUp");
    assert_eq!(first.codec(), CODEC_NAME);
    let value = OpcUaValue::data_value(&first).unwrap();
    assert!(value.source_timestamp.is_some(), "the PLC's timestamp travels");
    assert!(value.server_timestamp.is_some());
    let mut last = as_u32(&first);
    for _ in 0..4 {
        let n = as_u32(&next(&mut step_up).await);
        assert!(n > last, "StepUp went from {last} to {n}");
        last = n;
    }

    // The whole writer: every field OPC Publisher sends, typed.
    let mut seen = std::collections::BTreeSet::new();
    while seen.len() < 2 {
        let message = next(&mut all).await;
        let field = message.subject().strip_prefix("simulation/").unwrap().to_owned();
        let value = OpcUaValue::data_value(&message).unwrap().value;
        match field.as_str() {
            "StepUp" => assert!(matches!(value, Some(Variant::UInt32(_))), "{value:?}"),
            "RandomSignedInt32" => assert!(matches!(value, Some(Variant::Int32(_))), "{value:?}"),
            "AlternatingBoolean" => assert!(matches!(value, Some(Variant::Boolean(_)))),
            other => panic!("unexpected field {other}"),
        }
        seen.insert(field);
    }
    assert_eq!(
        provider.sequence_gaps(),
        0,
        "OPC Publisher's SequenceNumbers arrived without a gap"
    );
}

#[tokio::test]
async fn reads_opc_publisher_metadata() {
    let provider = connect().await;
    // OPC Publisher retains it, so it is there whenever a reader asks.
    let mut meta = None;
    for _ in 0..30 {
        match provider.metadata(STREAM, WRITER).await {
            Ok(found) => {
                meta = Some(found);
                break;
            }
            Err(StreamError::NotFound(_)) => tokio::time::sleep(Duration::from_secs(1)).await,
            Err(error) => panic!("{error}"),
        }
    }
    let meta = meta.expect("OPC Publisher retained its metadata");
    assert_eq!(meta.publisher_id.as_deref(), Some("opc-publisher"));
    assert_eq!(meta.writer_group_name.as_deref(), Some("plc"));
    let fields = meta.meta_data.fields.unwrap();
    let step_up = fields
        .iter()
        .find(|f| f.name.as_ref() == "StepUp")
        .expect("StepUp is described");
    assert_eq!(step_up.built_in_type, 7, "UInt32");
    assert_eq!(fields.len(), 3);
}

#[tokio::test]
async fn opc_publishers_group_is_not_ours_to_write() {
    let provider = connect().await;
    let manager = provider.bind(STREAM).await.unwrap();
    let error = manager
        .publish(
            "simulation/StepUp",
            OpcUaValue::payload(1u32),
            &Options::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, StreamError::Unsupported(_)), "{error}");
}
