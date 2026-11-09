//! OPC UA PubSub over MQTT against a broker started in-process by `rumqttd`,
//! so these run in plain `cargo test --features opcua`.
//!
//! Besides the provider talking to itself, a raw MQTT client reads what goes
//! on the wire and writes NetworkMessages by hand, as another publisher would.

use std::{
    collections::HashMap,
    net::{SocketAddr, TcpListener},
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use futures::StreamExt;
use rumqttc::v5::{
    AsyncClient, Event, EventLoop, Incoming,
    mqttbytes::{QoS, v5::Publish},
};
use rumqttd::{Broker, Config, ConnectionSettings, RouterConfig, ServerSettings};
use serde_json::{Value, json};
use streams::{
    Manager, Message, Options, Payload, Stream, StreamError, Streams, Subscription, as_batch,
    as_durable, as_positioned,
    drivers::opcua::{
        CODEC_NAME, DataValue, OpcUaValue, StatusCode, Variant,
        pubsub::{DataSet, MqttOptions, NetworkMessage, PubSubConfig, PubSubProvider},
    },
};
use tokio::time::timeout;

const WITHIN: Duration = Duration::from_secs(10);
const PUBLISHER: &str = "plant-1";

/// A broker on a free localhost port; see tests/mqtt.rs for why the port is
/// reserved and released.
fn broker() -> u16 {
    let port = TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .map(|a| a.port())
        .expect("reserve a port");
    let server = ServerSettings {
        name: format!("v5-{port}"),
        listen: SocketAddr::from(([127, 0, 0, 1], port)),
        tls: None,
        next_connection_delay_ms: 1,
        connections: ConnectionSettings {
            connection_timeout_ms: 60_000,
            max_payload_size: 1 << 20,
            max_inflight_count: 100,
            auth: None,
            external_auth: None,
            dynamic_filters: true,
        },
    };
    let config = Config {
        id: 0,
        router: RouterConfig {
            max_connections: 1_000,
            max_outgoing_packet_count: 200,
            max_segment_size: 1 << 20,
            max_segment_count: 10,
            ..Default::default()
        },
        v5: Some(HashMap::from([("1".to_owned(), server)])),
        ..Default::default()
    };
    std::thread::spawn(move || {
        let _ = Broker::new(config).start();
    });
    port
}

/// A template for the in-process broker on `port`, with a 1 MiB packet limit
/// because a batch's NetworkMessage outgrows rumqttc's 10 KiB default.
fn options(port: u16) -> MqttOptions {
    let mut options = MqttOptions::new("template", "127.0.0.1", port);
    // NetworkMessages of a batch are larger than rumqttc's 10 KiB default.
    options.set_max_packet_size(Some(1 << 20));
    options
}

/// A provider publishing as `publisher`, retrying for about 2.5s while the
/// broker thread starts listening.
async fn provider_on(port: u16, publisher: &str) -> PubSubProvider {
    let config = PubSubConfig {
        publisher_id: publisher.into(),
        ..PubSubConfig::default()
    };
    let mut last = None;
    for _ in 0..50 {
        match PubSubProvider::connect(options(port), config.clone()).await {
            Ok(provider) => return provider,
            Err(error) => last = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("broker never came up: {last:?}");
}

/// A provider publishing as [`PUBLISHER`] on a broker of its own, and that broker's port.
async fn provider() -> (PubSubProvider, u16) {
    let port = broker();
    (provider_on(port, PUBLISHER).await, port)
}

/// Declares a stream (a WriterGroup) with a fixed `id` over `subjects` and binds a manager to it.
async fn declare(streams: &PubSubProvider, id: &str, subjects: &[&str]) -> Arc<dyn Manager> {
    let stream = streams
        .create(Stream {
            id: id.into(),
            subjects: subjects.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        })
        .await
        .expect("create");
    streams.bind(&stream.id).await.expect("bind")
}

/// The next message within [`WITHIN`], panicking on a timeout or an ended subscription.
async fn next(subscription: &mut Subscription) -> Message {
    timeout(WITHIN, subscription.next())
        .await
        .expect("a message within the deadline")
        .expect("subscription ended before a message arrived")
}

/// Asserts nothing arrives within 300ms.
async fn quiet(subscription: &mut Subscription) {
    if let Ok(Some(message)) = timeout(Duration::from_millis(300), subscription.next()).await {
        panic!("expected nothing, got {message:?}");
    }
}

/// The OPC UA value a message carries, or `Empty` when the DataValue has none.
fn value(message: &Message) -> Variant {
    OpcUaValue::data_value(message)
        .expect("an OPC UA DataValue")
        .value
        .unwrap_or(Variant::Empty)
}

/// A raw MQTT client, subscribed to `filter`, for reading the wire.
async fn raw(port: u16, filter: Option<&str>) -> (AsyncClient, EventLoop) {
    let mut options = options(port);
    options.set_client_id(format!("raw-{}", ulid::Ulid::new()));
    let (client, mut events) = AsyncClient::new(options, 64);
    if let Some(filter) = filter {
        client
            .subscribe(filter, QoS::AtLeastOnce)
            .await
            .expect("subscribe");
    }
    loop {
        match timeout(WITHIN, events.poll()).await.expect("raw client") {
            Ok(Event::Incoming(Incoming::SubAck(_))) if filter.is_some() => break,
            Ok(Event::Incoming(Incoming::ConnAck(_))) if filter.is_none() => break,
            Ok(_) => {}
            Err(error) => panic!("raw client: {error}"),
        }
    }
    (client, events)
}

/// The next PUBLISH the raw client receives, skipping other events; each
/// poll must arrive within [`WITHIN`].
async fn raw_next(events: &mut EventLoop) -> Publish {
    loop {
        match timeout(WITHIN, events.poll()).await.expect("a publish") {
            Ok(Event::Incoming(Incoming::Publish(publish))) => return publish,
            Ok(_) => {}
            Err(error) => panic!("raw client: {error}"),
        }
    }
}

/// Publishes `body` with a raw client and drives it until the broker has it.
async fn inject(client: &AsyncClient, events: &mut EventLoop, topic: &str, body: Value) {
    inject_raw(client, events, topic, &body.to_string()).await;
}

/// Publishes `body` verbatim at QoS 1 and drives the raw client until the
/// broker acks it, so malformed JSON can be sent too.
async fn inject_raw(client: &AsyncClient, events: &mut EventLoop, topic: &str, body: &str) {
    client
        .publish(topic, QoS::AtLeastOnce, false, Bytes::from(body.to_owned()))
        .await
        .unwrap();
    loop {
        if let Ok(Event::Incoming(Incoming::PubAck(_))) =
            timeout(WITHIN, events.poll()).await.expect("puback")
        {
            return;
        }
    }
}

/// A JSON DataSetMessage from `writer` carrying `field` as an Int32 (type 6) `body`.
fn data_set_message(writer: &str, sequence: u32, field: &str, body: i32) -> Value {
    json!({
        "DataSetWriterName": writer,
        "SequenceNumber": sequence,
        "Payload": { field: { "Value": { "Type": 6, "Body": body } } },
    })
}

#[tokio::test]
async fn lifecycle() {
    let (streams, _) = provider().await;
    assert_eq!(streams.publisher_id(), PUBLISHER);
    let created = streams
        .create(Stream {
            id: "line-a".into(),
            description: "before".into(),
            subjects: vec!["boiler".into(), "pump/Speed".into()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(created.active);
    assert_eq!(streams.get("line-a").await.unwrap(), created);
    assert_eq!(
        streams.data_topic("line-a", "boiler").unwrap(),
        "opcua/json/data/plant-1/line-a/boiler"
    );

    let generated = streams
        .create(Stream {
            name: "named".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(generated.id.ends_with(":named"), "{}", generated.id);
    assert_eq!(streams.list().await.unwrap().len(), 2);

    let updated = streams
        .update(
            "line-a",
            Stream {
                description: "after".into(),
                subjects: vec!["boiler".into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.id, "line-a");
    assert_eq!(updated.description, "after");

    streams.delete("line-a").await.unwrap();
    for error in [
        streams.get("line-a").await.unwrap_err(),
        streams.bind("line-a").await.err().unwrap(),
        streams.delete("line-a").await.unwrap_err(),
        streams
            .update("line-a", Stream::default())
            .await
            .unwrap_err(),
    ] {
        assert!(matches!(error, StreamError::NotFound(_)), "{error}");
    }
    streams.close().await.unwrap();
}

#[tokio::test]
async fn create_refuses_what_is_not_a_topic_level() {
    let (streams, _) = provider().await;
    for (id, subject) in [
        ("a/b/c", "w"),
        ("line+", "w"),
        ("line", "w#"),
        ("line", "/field"),
        ("line", "w/"),
    ] {
        let error = streams
            .create(Stream {
                id: id.into(),
                subjects: vec![subject.into()],
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(
            matches!(error, StreamError::Unsupported(_)),
            "{id} {subject}: {error}"
        );
    }
    assert!(streams.list().await.unwrap().is_empty());
}

#[tokio::test]
async fn a_field_round_trips_as_a_typed_value() {
    let (streams, _) = provider().await;
    let manager = declare(&streams, "line-a", &["boiler/Temperature", "boiler/Mode"]).await;
    let mut temperature = manager
        .subscribe("boiler/Temperature", &Options::default())
        .await
        .unwrap();
    let mut mode = manager
        .subscribe("boiler/Mode", &Options::default())
        .await
        .unwrap();

    let at = opcua::types::DateTime::now();
    let sent = DataValue {
        value: Some(Variant::from(21.5f64)),
        status: Some(StatusCode::UncertainLastUsableValue),
        source_timestamp: Some(at),
        ..DataValue::default()
    };
    let id = manager
        .publish(
            "boiler/Temperature",
            OpcUaValue::payload_of(&sent),
            &Options::default(),
        )
        .await
        .unwrap();
    manager
        .publish(
            "boiler/Mode",
            OpcUaValue::payload("auto"),
            &Options::default().with_id("chosen"),
        )
        .await
        .unwrap();

    let got = next(&mut temperature).await;
    assert_eq!(got.subject(), "boiler/Temperature");
    assert_eq!(got.codec(), CODEC_NAME);
    assert_eq!(got.id(), id, "the MessageId travels");
    assert_eq!(OpcUaValue::data_value(&got).unwrap(), sent);
    let got = next(&mut mode).await;
    assert_eq!(got.id(), "chosen");
    assert_eq!(value(&got), Variant::from("auto"));
    // Each field subscription saw only its own field.
    quiet(&mut temperature).await;
}

#[tokio::test]
async fn a_writer_publishes_and_delivers_its_whole_data_set() {
    let (streams, _) = provider().await;
    let manager = declare(&streams, "line-a", &["boiler"]).await;
    let mut all = manager
        .subscribe("boiler", &Options::default())
        .await
        .unwrap();
    // A writer declared whole accepts each of its fields too.
    let mut one = manager
        .subscribe("boiler/Pressure", &Options::default())
        .await
        .unwrap();

    let set = DataSet::new()
        .with("Temperature", DataValue::value_only(80.0f64))
        .with("Pressure", DataValue::value_only(2i32))
        .with("Running", DataValue::value_only(true));
    let id = manager
        .publish("boiler", set.payload(), &Options::default())
        .await
        .unwrap();

    let mut fields = Vec::new();
    for _ in 0..3 {
        let message = next(&mut all).await;
        assert_eq!(
            message.id(),
            id,
            "the fields of one DataSetMessage share its id"
        );
        fields.push((message.subject().to_owned(), value(&message)));
    }
    assert_eq!(
        fields,
        vec![
            ("boiler/Temperature".into(), Variant::from(80.0f64)),
            ("boiler/Pressure".into(), Variant::from(2i32)),
            ("boiler/Running".into(), Variant::from(true)),
        ]
    );
    let pressure = next(&mut one).await;
    assert_eq!(pressure.subject(), "boiler/Pressure");
    quiet(&mut one).await;

    manager
        .publish(
            "boiler/Running",
            OpcUaValue::payload(false),
            &Options::default(),
        )
        .await
        .unwrap();
    let running = next(&mut all).await;
    assert_eq!(running.subject(), "boiler/Running");
}

#[tokio::test]
async fn the_wire_is_part_14_json() {
    let (streams, port) = provider().await;
    let manager = declare(&streams, "line-a", &["boiler"]).await;
    let (_client, mut events) = raw(port, Some("opcua/json/data/plant-1/line-a/#")).await;

    let id = manager
        .publish(
            "boiler/Temperature",
            OpcUaValue::payload(21.5f64),
            &Options::default(),
        )
        .await
        .unwrap();
    manager
        .publish(
            "boiler/Temperature",
            OpcUaValue::payload(22.0f64),
            &Options::default(),
        )
        .await
        .unwrap();

    let publish = raw_next(&mut events).await;
    assert_eq!(publish.topic, "opcua/json/data/plant-1/line-a/boiler");
    assert!(!publish.retain);
    assert_eq!(
        publish
            .properties
            .as_ref()
            .and_then(|p| p.content_type.as_deref()),
        Some("application/json")
    );
    let json: Value = serde_json::from_slice(&publish.payload).unwrap();
    assert_eq!(json["MessageId"], id.as_str());
    assert_eq!(json["MessageType"], "ua-data");
    assert_eq!(json["PublisherId"], "plant-1");
    assert_eq!(json["WriterGroupName"], "line-a");
    let message = &json["Messages"][0];
    assert_eq!(message["DataSetWriterName"], "boiler");
    assert_eq!(
        message["DataSetWriterId"],
        streams::drivers::opcua::pubsub::writer_id("boiler")
    );
    assert_eq!(message["SequenceNumber"], 1);
    assert!(message["Timestamp"].is_string());
    assert_eq!(
        message["Payload"]["Temperature"],
        json!({"Value": {"Type": 11, "Body": 21.5}})
    );
    // The writer numbers its messages.
    let second: Value = serde_json::from_slice(&raw_next(&mut events).await.payload).unwrap();
    assert_eq!(second["Messages"][0]["SequenceNumber"], 2);
}

#[tokio::test]
async fn a_batch_is_one_network_message() {
    let (streams, port) = provider().await;
    let manager = declare(&streams, "line-a", &["boiler/Count"]).await;
    let mut subscription = manager
        .subscribe("boiler/Count", &Options::default())
        .await
        .unwrap();
    let (_client, mut events) = raw(port, Some("opcua/json/data/plant-1/line-a/boiler")).await;

    let batch = as_batch(manager.as_ref()).unwrap();
    let ids = batch
        .publish_batch(
            "boiler/Count",
            (0..5i32).map(OpcUaValue::payload).collect(),
            &Options::default(),
        )
        .await
        .unwrap();
    assert_eq!(ids.len(), 5);

    let publish = raw_next(&mut events).await;
    let network = NetworkMessage::from_json(&publish.payload).unwrap();
    assert_eq!(
        network.messages.len(),
        5,
        "one NetworkMessage for the batch"
    );
    let message_id = network.message_id.clone().unwrap();
    for (n, (message, id)) in network.messages.iter().zip(&ids).enumerate() {
        assert_eq!(message.sequence_number, Some(n as u32 + 1));
        assert_eq!(*id, format!("{message_id}.{n}"));
    }
    for (n, id) in ids.iter().enumerate() {
        let message = next(&mut subscription).await;
        assert_eq!(message.id(), id);
        assert_eq!(value(&message), Variant::from(n as i32));
    }

    // A payload that is not an OPC UA value fails alone.
    let error = batch
        .publish_batch(
            "boiler/Count",
            vec![
                OpcUaValue::payload(10i32),
                Payload::json(&11).unwrap(),
                OpcUaValue::payload(12i32),
            ],
            &Options::default(),
        )
        .await
        .unwrap_err();
    let StreamError::PartialBatch { failures, ids, .. } = error else {
        panic!("{error}");
    };
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].0, 1);
    assert_eq!(ids.len(), 3);
    assert_eq!(value(&next(&mut subscription).await), Variant::from(10i32));
    assert_eq!(value(&next(&mut subscription).await), Variant::from(12i32));

    // An id cannot name several messages.
    let error = batch
        .publish_batch(
            "boiler/Count",
            vec![OpcUaValue::payload(1i32)],
            &Options::default().with_id("x"),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, StreamError::Unsupported(_)), "{error}");
}

#[tokio::test]
async fn a_long_batch_is_split() {
    let port = broker();
    let config = PubSubConfig {
        publisher_id: PUBLISHER.into(),
        max_dataset_messages: 4,
        ..PubSubConfig::default()
    };
    let mut streams = None;
    for _ in 0..50 {
        if let Ok(p) = PubSubProvider::connect(options(port), config.clone()).await {
            streams = Some(p);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let streams = streams.expect("broker");
    let manager = declare(&streams, "line-a", &["w/f"]).await;
    let mut subscription = manager.subscribe("w/f", &Options::default()).await.unwrap();
    let ids = as_batch(manager.as_ref())
        .unwrap()
        .publish_batch(
            "w/f",
            (0..10i32).map(OpcUaValue::payload).collect(),
            &Options::default(),
        )
        .await
        .unwrap();
    for (n, id) in ids.iter().enumerate() {
        let message = next(&mut subscription).await;
        assert_eq!(message.id(), id);
        assert_eq!(value(&message), Variant::from(n as i32));
    }
    // Ten in fours: 4, 4, 2, and the last NetworkMessage's ids are its own.
    let prefixes: std::collections::BTreeSet<&str> =
        ids.iter().map(|id| id.split('.').next().unwrap()).collect();
    assert_eq!(prefixes.len(), 3);
    assert_eq!(streams.sequence_gaps(), 0);
}

#[tokio::test]
async fn refusals() {
    let (streams, _) = provider().await;
    let manager = declare(&streams, "line-a", &["boiler", "pump/Speed"]).await;
    let opts = Options::default();

    for subject in ["typo", "pump", "pump/Other", "boiler/"] {
        let error = manager
            .publish(subject, OpcUaValue::payload(1i32), &opts)
            .await
            .unwrap_err();
        assert!(
            matches!(error, StreamError::UnknownSubject { .. }),
            "{subject}: {error}"
        );
        let error = manager.subscribe(subject, &opts).await.err().unwrap();
        assert!(
            matches!(error, StreamError::UnknownSubject { .. }),
            "{subject}: {error}"
        );
    }

    // A field takes an OPC UA value, a writer a DataSet.
    for (subject, payload) in [
        ("pump/Speed", Payload::json(&1).unwrap()),
        ("boiler", OpcUaValue::payload(1i32)),
    ] {
        let error = manager.publish(subject, payload, &opts).await.unwrap_err();
        assert!(
            matches!(error, StreamError::Unsupported(_)),
            "{subject}: {error}"
        );
    }

    let error = manager
        .publish(
            "pump/Speed",
            OpcUaValue::payload(1i32),
            &Options::default().with_ttl(Duration::from_secs(1)),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, StreamError::Unsupported(_)), "{error}");

    for error in [
        as_durable(manager.as_ref()).err().unwrap(),
        as_positioned(manager.as_ref()).err().unwrap(),
    ] {
        assert!(matches!(error, StreamError::Unsupported(_)), "{error}");
    }

    // Another publisher's WriterGroup can be read, not written.
    let foreign = declare(&streams, "plant-2/line-a", &["boiler"]).await;
    let error = foreign
        .publish("boiler/x", OpcUaValue::payload(1i32), &opts)
        .await
        .unwrap_err();
    assert!(matches!(error, StreamError::Unsupported(_)), "{error}");
    assert!(error.to_string().contains("plant-2"), "{error}");
    let error = streams
        .publish_metadata("plant-2/line-a", "boiler", Default::default())
        .await
        .unwrap_err();
    assert!(matches!(error, StreamError::Unsupported(_)), "{error}");
    let error = streams
        .publish_metadata("line-a", "nobody", Default::default())
        .await
        .unwrap_err();
    assert!(
        matches!(error, StreamError::UnknownSubject { .. }),
        "{error}"
    );

    // A closed provider publishes nothing.
    streams.close().await.unwrap();
    let error = manager
        .publish("pump/Speed", OpcUaValue::payload(1i32), &opts)
        .await
        .unwrap_err();
    assert!(matches!(error, StreamError::Internal(_)), "{error}");
}

#[tokio::test]
async fn reads_another_publisher_and_detects_gaps() {
    let (streams, port) = provider().await;
    let manager = declare(&streams, "plant-2/line-b", &["press"]).await;
    let mut subscription = manager
        .subscribe("press", &Options::default())
        .await
        .unwrap();
    let (client, mut events) = raw(port, None).await;
    let writer_topic = "opcua/json/data/plant-2/line-b/press";
    let group_topic = "opcua/json/data/plant-2/line-b";

    // On the writer's topic, as another stack writes it: no MessageId, a
    // non-reversible value.
    inject(
        &client,
        &mut events,
        writer_topic,
        json!({
            "MessageType": "ua-data",
            "PublisherId": "plant-2",
            "Messages": [{ "SequenceNumber": 1, "Payload": { "Force": { "Value": 12.5 } } }],
        }),
    )
    .await;
    let message = next(&mut subscription).await;
    assert_eq!(message.subject(), "press/Force");
    assert_eq!(value(&message), Variant::from(12.5f64));

    // On the group's topic, among another writer's messages.
    inject(
        &client,
        &mut events,
        group_topic,
        json!({
            "MessageId": "g-1",
            "MessageType": "ua-data",
            "PublisherId": "plant-2",
            "Messages": [
                data_set_message("other", 1, "x", 1),
                data_set_message("press", 2, "Force", 2),
            ],
        }),
    )
    .await;
    let message = next(&mut subscription).await;
    assert_eq!(message.id(), "g-1.1");
    assert_eq!(value(&message), Variant::from(2i32));
    assert_eq!(streams.sequence_gaps(), 0);

    // 3 and 4 never arrive: a gap of two. Then 5 again is a repeat.
    for sequence in [5, 5, 6] {
        inject(
            &client,
            &mut events,
            writer_topic,
            json!({
                "MessageType": "ua-data",
                "PublisherId": "plant-2",
                "Messages": [data_set_message("press", sequence, "Force", sequence as i32)],
            }),
        )
        .await;
    }
    assert_eq!(value(&next(&mut subscription).await), Variant::from(5i32));
    assert_eq!(value(&next(&mut subscription).await), Variant::from(6i32));
    assert_eq!(streams.sequence_gaps(), 2);

    // Garbage and metadata on the data topic are dropped; the stream lives on.
    inject_raw(&client, &mut events, writer_topic, "not json").await;
    inject(
        &client,
        &mut events,
        writer_topic,
        json!({"MessageType": "ua-metadata", "MetaData": {}}),
    )
    .await;
    // A restarted publisher counts from 1 again, which is not a gap.
    inject(
        &client,
        &mut events,
        writer_topic,
        json!({
            "MessageType": "ua-data",
            "PublisherId": "plant-2",
            "Messages": [data_set_message("press", 1, "Force", 100)],
        }),
    )
    .await;
    assert_eq!(value(&next(&mut subscription).await), Variant::from(100i32));
    assert_eq!(streams.sequence_gaps(), 2);
}

#[tokio::test]
async fn a_group_shares_the_writer() {
    let (streams, _) = provider().await;
    let manager = declare(&streams, "line-a", &["w/f"]).await;
    let opts = Options::default().with_group("workers");
    let mut first = manager.subscribe("w/f", &opts).await.unwrap();
    let mut second = manager.subscribe("w/f", &opts).await.unwrap();
    const N: i32 = 40;
    for n in 0..N {
        manager
            .publish("w/f", OpcUaValue::payload(n), &Options::default())
            .await
            .unwrap();
    }
    let mut seen = Vec::new();
    let (mut a, mut b) = (0, 0);
    while seen.len() < N as usize {
        tokio::select! {
            Some(m) = first.next() => { a += 1; seen.push(value(&m)); }
            Some(m) = second.next() => { b += 1; seen.push(value(&m)); }
            _ = tokio::time::sleep(WITHIN) => panic!("only {} of {N} arrived", seen.len()),
        }
    }
    assert!(a > 0 && b > 0, "the broker split the writer: {a} and {b}");
    quiet(&mut first).await;
    assert_eq!(
        streams.sequence_gaps(),
        0,
        "a group member does not track gaps"
    );
}

#[tokio::test]
async fn metadata_is_retained_and_versions_the_data() {
    let (streams, port) = provider().await;
    let manager = declare(&streams, "line-a", &["boiler"]).await;
    let set = DataSet::new()
        .with("Temperature", DataValue::value_only(80.0f64))
        .with("Running", DataValue::value_only(true));
    let version = streams
        .publish_metadata("line-a", "boiler", set.metadata("boiler"))
        .await
        .unwrap();
    assert_ne!(version.major_version, 0, "a zero version is stamped");

    // A reader that arrives afterwards still finds it, retained.
    let (_client, mut events) = raw(port, Some("opcua/json/metadata/plant-1/line-a/boiler")).await;
    let publish = raw_next(&mut events).await;
    assert!(publish.retain);
    let json: Value = serde_json::from_slice(&publish.payload).unwrap();
    assert_eq!(json["MessageType"], "ua-metadata");
    assert_eq!(json["DataSetWriterName"], "boiler");
    assert_eq!(json["MetaData"]["Fields"][0]["Name"], "Temperature");
    assert_eq!(json["MetaData"]["Fields"][0]["BuiltInType"], 11);

    let read = streams.metadata("line-a", "boiler").await.unwrap();
    assert_eq!(read.meta_data.configuration_version, version);
    assert_eq!(read.meta_data.fields.unwrap().len(), 2);
    let error = streams.metadata("line-a", "nobody").await;
    assert!(matches!(error, Err(StreamError::NotFound(_))), "{error:?}");

    // DataSetMessages now say which metadata they follow.
    let (_client, mut events) = raw(port, Some("opcua/json/data/plant-1/line-a/boiler")).await;
    manager
        .publish("boiler", set.payload(), &Options::default())
        .await
        .unwrap();
    let network = NetworkMessage::from_json(&raw_next(&mut events).await.payload).unwrap();
    assert_eq!(network.messages[0].meta_data_version, Some(version));
}

#[tokio::test]
async fn retained_data_is_not_delivered() {
    let (streams, port) = provider().await;
    let manager = declare(&streams, "line-a", &["w"]).await;
    let (client, mut events) = raw(port, None).await;
    client
        .publish(
            "opcua/json/data/plant-1/line-a/w",
            QoS::AtLeastOnce,
            true,
            data_set_message("w", 1, "f", 1).to_string(),
        )
        .await
        .unwrap();
    loop {
        if let Ok(Event::Incoming(Incoming::PubAck(_))) =
            timeout(WITHIN, events.poll()).await.unwrap()
        {
            break;
        }
    }
    let mut subscription = manager.subscribe("w", &Options::default()).await.unwrap();
    quiet(&mut subscription).await;
    manager
        .publish("w/f", OpcUaValue::payload(2i32), &Options::default())
        .await
        .unwrap();
    assert_eq!(value(&next(&mut subscription).await), Variant::from(2i32));
}

// Headers ride as MQTT 5 user properties on the NetworkMessage's PUBLISH;
// the Part 14 JSON is unchanged.
#[tokio::test]
async fn headers_are_user_properties_beside_part_14_json() {
    let (streams, port) = provider().await;
    let stream = streams
        .create(
            Stream::named("h")
                .with_id("line-h")
                .subject("boiler")
                .header("lang", "rust")
                .header("site", "plant-3"),
        )
        .await
        .unwrap();
    let manager = streams.bind(&stream.id).await.unwrap();
    let (_client, mut events) = raw(port, Some("opcua/json/data/plant-1/line-h/#")).await;
    let mut subscription = manager
        .subscribe("boiler", &Options::default())
        .await
        .unwrap();

    let opts = Options::default()
        .header("site", "plant-4")
        .with_expiry(Duration::from_millis(2500));
    manager
        .publish("boiler/Temperature", OpcUaValue::payload(21.5f64), &opts)
        .await
        .unwrap();

    let message = next(&mut subscription).await;
    assert_eq!(message.subject(), "boiler/Temperature");
    assert_eq!(message.headers().get("lang"), Some("rust"));
    assert_eq!(message.headers().get("site"), Some("plant-4"));

    let publish = raw_next(&mut events).await;
    let properties = publish.properties.clone().expect("properties");
    assert_eq!(properties.content_type.as_deref(), Some("application/json"));
    assert_eq!(
        properties.user_properties,
        vec![
            ("lang".to_owned(), "rust".to_owned()),
            ("site".to_owned(), "plant-4".to_owned()),
        ]
    );
    let interval = properties.message_expiry_interval.expect("an expiry");
    assert!((2..=3).contains(&interval), "rounded up: {interval}");
    let json: Value = serde_json::from_slice(&publish.payload).unwrap();
    assert!(
        !publish.payload.windows(4).any(|w| w == b"lang"),
        "no header inside the Part 14 JSON: {json}"
    );
    assert_eq!(json["MessageType"], "ua-data");

    // A batch's NetworkMessage carries them too.
    as_batch(manager.as_ref())
        .unwrap()
        .publish_batch(
            "boiler/Temperature",
            vec![OpcUaValue::payload(1.0f64), OpcUaValue::payload(2.0f64)],
            &Options::default().header("n", "b"),
        )
        .await
        .unwrap();
    for _ in 0..2 {
        let message = next(&mut subscription).await;
        assert_eq!(message.headers().get("n"), Some("b"));
        assert_eq!(message.headers().get("lang"), Some("rust"));
    }
}

#[tokio::test]
async fn another_publishers_user_properties_arrive_as_headers() {
    let (streams, port) = provider().await;
    let manager = declare(&streams, "plant-2/line-a", &["w"]).await;
    let mut subscription = manager.subscribe("w", &Options::default()).await.unwrap();
    let (client, mut events) = raw(port, None).await;
    let body = json!({
        "MessageId": "m-1",
        "MessageType": "ua-data",
        "PublisherId": "plant-2",
        "Messages": [data_set_message("w", 1, "f", 7)],
    });
    client
        .publish_with_properties(
            "opcua/json/data/plant-2/line-a/w",
            QoS::AtLeastOnce,
            false,
            Bytes::from(body.to_string()),
            rumqttc::v5::mqttbytes::v5::PublishProperties {
                user_properties: vec![("lang".into(), "dotnet".into())],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    tokio::spawn(async move { while events.poll().await.is_ok() {} });
    let message = next(&mut subscription).await;
    assert_eq!(value(&message), Variant::from(7i32));
    assert_eq!(message.headers().get("lang"), Some("dotnet"));
}

#[tokio::test]
async fn retain_is_refused_and_invalid_headers_fail_at_create() {
    let (streams, _) = provider().await;
    let manager = declare(&streams, "line-r", &["w"]).await;
    for result in [
        manager
            .publish(
                "w/f",
                OpcUaValue::payload(1i32),
                &Options::default().with_retain(),
            )
            .await
            .map(|_| ()),
        as_batch(manager.as_ref())
            .unwrap()
            .publish_batch(
                "w/f",
                vec![OpcUaValue::payload(1i32)],
                &Options::default().with_retain(),
            )
            .await
            .map(|_| ()),
    ] {
        match result {
            Err(StreamError::Unsupported(why)) => assert!(why.contains("retained"), "{why}"),
            other => panic!("retain: {other:?}"),
        }
    }
    let error = streams
        .create(
            Stream::named("bad")
                .with_id("line-b")
                .subject("w")
                .header("a b", "x"),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, StreamError::InvalidHeader(_)), "{error}");
    let error = manager
        .publish(
            "w/f",
            OpcUaValue::payload(1i32),
            &Options::default().header("k", "two\nlines"),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, StreamError::InvalidHeader(_)), "{error}");
}
