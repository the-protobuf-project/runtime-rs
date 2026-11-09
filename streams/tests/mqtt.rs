//! The MQTT provider against a broker started in-process by `rumqttd`, so these
//! need no broker installed and no container, and run in plain `cargo test`.

use std::{
    collections::HashMap,
    net::{SocketAddr, TcpListener},
    sync::Arc,
    time::Duration,
};

use futures::StreamExt;
use rumqttd::{Broker, Config, ConnectionSettings, RouterConfig, ServerSettings};
use serde::{Deserialize, Serialize};
use streams::{
    Deliveries, Manager, Options, Payload, Stream, StreamError, Streams, Subscription, as_batch,
    as_durable, as_positioned,
    drivers::mqtt::{MqttConfig, MqttOptions, MqttProvider},
};
use tokio::time::timeout;

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
struct Event {
    user: String,
    action: String,
}

const SUBJECT: &str = "user/created";
const WITHIN: Duration = Duration::from_secs(10);

/// Starts a broker on a free localhost port and returns the port.
///
/// rumqttd binds inside its own threads and does not report the port it got,
/// so the port is reserved here and released just before the broker takes it.
/// The window that leaves is closed by [`provider`], which retries the
/// connection until the broker answers rather than probing it with a raw
/// socket that never sends CONNECT.
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
    // The broker runs on threads of its own and lives as long as the test
    // process; each test gets one so no state leaks between them.
    std::thread::spawn(move || {
        let _ = Broker::new(config).start();
    });
    port
}

fn options(port: u16) -> MqttOptions {
    MqttOptions::new("template", "127.0.0.1", port)
}

async fn provider() -> MqttProvider {
    provider_on(broker()).await
}

async fn provider_on(port: u16) -> MqttProvider {
    let mut last = None;
    for _ in 0..50 {
        match MqttProvider::connect(options(port), MqttConfig::default()).await {
            Ok(provider) => return provider,
            Err(error) => last = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("broker never came up: {last:?}");
}

async fn declare(streams: &MqttProvider, subjects: &[&str]) -> Arc<dyn Manager> {
    let stream = streams
        .create(Stream {
            name: "test".into(),
            subjects: subjects.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        })
        .await
        .expect("create");
    streams.bind(&stream.id).await.expect("bind")
}

fn event(user: &str, action: &str) -> Payload {
    Payload::json(&Event {
        user: user.into(),
        action: action.into(),
    })
    .unwrap()
}

async fn next(subscription: &mut Subscription) -> streams::Message {
    timeout(WITHIN, subscription.next())
        .await
        .expect("a message within the deadline")
        .expect("subscription ended before a message arrived")
}

async fn next_delivery(deliveries: &mut Deliveries) -> streams::Delivery {
    timeout(WITHIN, deliveries.next())
        .await
        .expect("a delivery within the deadline")
        .expect("deliveries ended before a message arrived")
}

#[tokio::test]
async fn lifecycle() {
    let streams = provider().await;
    let created = streams
        .create(Stream {
            name: "users".into(),
            description: "before".into(),
            subjects: vec![SUBJECT.into()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(!created.id.is_empty(), "create did not assign an id");
    assert!(created.active);

    streams
        .update(
            &created.id,
            Stream {
                name: "users".into(),
                description: "after".into(),
                subjects: vec![SUBJECT.into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(streams.get(&created.id).await.unwrap().description, "after");
    assert_eq!(streams.list().await.unwrap().len(), 1);

    streams.delete(&created.id).await.unwrap();
    assert!(matches!(
        streams.get(&created.id).await,
        Err(StreamError::NotFound(_))
    ));
    streams.close().await.unwrap();
}

#[tokio::test]
async fn publish_and_subscribe() {
    let streams = provider().await;
    let manager = declare(&streams, &[SUBJECT]).await;

    let mut subscription = manager
        .subscribe(SUBJECT, &Options::default())
        .await
        .unwrap();
    let id = manager
        .publish(SUBJECT, event("ada", "created"), &Options::default())
        .await
        .unwrap();

    let message = next(&mut subscription).await;
    assert_eq!(message.id(), id);
    assert_eq!(message.subject(), SUBJECT);
    assert_eq!(
        message.json::<Event>().unwrap(),
        Event {
            user: "ada".into(),
            action: "created".into()
        }
    );
}

// MQTT is durable but has no log to seek, which is exactly why the contract
// keeps the two capabilities apart.
#[tokio::test]
async fn mqtt_is_durable_but_not_positioned() {
    let streams = provider().await;
    let manager = declare(&streams, &[SUBJECT]).await;

    assert!(as_durable(manager.as_ref()).is_ok());
    assert!(as_batch(manager.as_ref()).is_ok());
    assert!(matches!(
        as_positioned(manager.as_ref()),
        Err(StreamError::Unsupported(_))
    ));
}

#[tokio::test]
async fn consume_delivers_and_acknowledges() {
    let streams = provider().await;
    let manager = declare(&streams, &[SUBJECT]).await;
    let durable = as_durable(manager.as_ref()).unwrap();

    let mut deliveries = durable
        .consume(SUBJECT, "billing", &Options::default())
        .await
        .unwrap();
    manager
        .publish(SUBJECT, event("grace", "created"), &Options::default())
        .await
        .unwrap();

    let delivery = next_delivery(&mut deliveries).await;
    assert_eq!(delivery.json::<Event>().unwrap().user, "grace");
    // MQTT flags a repeat but does not count them, and the contract asks for
    // zero rather than a number that would be invented.
    assert_eq!(delivery.attempt(), 0);
    delivery.ack().await.unwrap();
}

// The session is the name: a message published while the consumer was away is
// waiting when it comes back.
#[tokio::test]
async fn a_session_keeps_messages_while_the_consumer_is_away() {
    let streams = provider().await;
    let manager = declare(&streams, &[SUBJECT]).await;
    let durable = as_durable(manager.as_ref()).unwrap();

    // Attach once so the broker creates the session and its subscription.
    let mut first = durable
        .consume(SUBJECT, "billing", &Options::default())
        .await
        .unwrap();
    manager
        .publish(SUBJECT, event("first", ""), &Options::default())
        .await
        .unwrap();
    next_delivery(&mut first).await.ack().await.unwrap();
    // The acknowledgement is a packet on the wire, and disconnecting in the
    // same breath can close the connection before it is written.
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(first);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Published with nobody attached. A clean session would drop this.
    manager
        .publish(SUBJECT, event("second", ""), &Options::default())
        .await
        .unwrap();

    let mut again = durable
        .consume(SUBJECT, "billing", &Options::default())
        .await
        .unwrap();
    let resumed = next_delivery(&mut again).await;
    assert_eq!(
        resumed.json::<Event>().unwrap().user,
        "second",
        "resumed somewhere other than the message published while detached"
    );
}

// A shared subscription is how MQTT splits work, and it is what group means
// here, unlike the other providers where the consumer name is the group.
#[tokio::test]
async fn group_shares_the_subject() {
    let streams = provider().await;
    let manager = declare(&streams, &[SUBJECT]).await;
    let durable = as_durable(manager.as_ref()).unwrap();
    let group = Options::default().with_group("billing");

    let mut first = durable.consume(SUBJECT, "worker-a", &group).await.unwrap();
    let mut second = durable.consume(SUBJECT, "worker-b", &group).await.unwrap();

    const TOTAL: usize = 6;
    for i in 0..TOTAL {
        manager
            .publish(SUBJECT, event("u", &i.to_string()), &Options::default())
            .await
            .unwrap();
    }

    let (mut seen, mut from_first, mut from_second) = (0, 0, 0);
    let deadline = tokio::time::sleep(Duration::from_secs(15));
    tokio::pin!(deadline);
    while seen < TOTAL {
        tokio::select! {
            Some(d) = first.next() => { seen += 1; from_first += 1; d.ack().await.unwrap(); }
            Some(d) = second.next() => { seen += 1; from_second += 1; d.ack().await.unwrap(); }
            _ = &mut deadline => panic!("saw {seen} of {TOTAL} messages across the group"),
        }
    }
    // Shared means each message went to one member, not to both.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        timeout(Duration::from_millis(200), first.next())
            .await
            .is_err()
            && timeout(Duration::from_millis(200), second.next())
                .await
                .is_err(),
        "a message was delivered to more than one member ({from_first} + {from_second})"
    );
}

#[tokio::test]
async fn subscribe_with_a_group_shares_the_subject() {
    let streams = provider().await;
    let manager = declare(&streams, &[SUBJECT]).await;
    let group = Options::default().with_group("readers");

    let mut first = manager.subscribe(SUBJECT, &group).await.unwrap();
    let mut second = manager.subscribe(SUBJECT, &group).await.unwrap();
    for i in 0..4 {
        manager
            .publish(SUBJECT, event("u", &i.to_string()), &Options::default())
            .await
            .unwrap();
    }
    let mut seen = 0;
    while seen < 4 {
        tokio::select! {
            Some(_) = first.next() => seen += 1,
            Some(_) = second.next() => seen += 1,
            _ = tokio::time::sleep(WITHIN) => panic!("saw {seen} of 4"),
        }
    }
    assert!(
        timeout(Duration::from_millis(300), first.next())
            .await
            .is_err()
    );
    assert!(
        timeout(Duration::from_millis(300), second.next())
            .await
            .is_err()
    );
}

// A stream may declare a filter; a message still has to land somewhere specific.
#[tokio::test]
async fn wildcard_subjects() {
    let streams = provider().await;
    let manager = declare(&streams, &["user/+"]).await;

    let mut subscription = manager
        .subscribe("user/+", &Options::default())
        .await
        .unwrap();
    manager
        .publish("user/created", event("ada", ""), &Options::default())
        .await
        .unwrap();
    let message = next(&mut subscription).await;
    assert_eq!(message.subject(), "user/created");

    assert!(matches!(
        manager
            .publish("user/+", event("", ""), &Options::default())
            .await,
        Err(StreamError::Unsupported(_))
    ));
}

#[tokio::test]
async fn dropping_a_subscription_stops_it() {
    let streams = provider().await;
    let manager = declare(&streams, &[SUBJECT]).await;

    let first = manager
        .subscribe(SUBJECT, &Options::default())
        .await
        .unwrap();
    drop(first);
    // A second subscriber still gets everything, and publishing to a topic
    // whose old subscriber is gone does not fail or stall.
    //
    // Under heavy load the in-process rumqttd sometimes closes the new
    // connection while the dropped one is leaving the same filter (a few runs
    // in a hundred, only with many test brokers in one busy process). The
    // driver reports it as an error rather than hanging, and the same steps
    // against Mosquitto never failed under the same load, so a connection the
    // broker closed is retried here; any other error still fails the test.
    let mut second = None;
    for _ in 0..3 {
        match manager.subscribe(SUBJECT, &Options::default()).await {
            Ok(subscription) => {
                second = Some(subscription);
                break;
            }
            Err(StreamError::Internal(error))
                if error.contains("Connection reset by peer")
                    || error.contains("Connection closed by peer") =>
            {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => panic!("subscribe: {error}"),
        }
    }
    let mut second = second.expect("the broker closed every attempt to subscribe");
    for _ in 0..3 {
        manager
            .publish(SUBJECT, event("ada", ""), &Options::default())
            .await
            .unwrap();
    }
    for _ in 0..3 {
        next(&mut second).await;
    }
}

#[tokio::test]
async fn publish_batch_publishes_each() {
    let streams = provider().await;
    let manager = declare(&streams, &[SUBJECT]).await;
    let batch = as_batch(manager.as_ref()).unwrap();

    let mut subscription = manager
        .subscribe(SUBJECT, &Options::default())
        .await
        .unwrap();
    let ids = batch
        .publish_batch(
            SUBJECT,
            vec![event("a", ""), event("b", ""), event("c", "")],
            &Options::default(),
        )
        .await
        .unwrap();
    assert_eq!(ids.len(), 3);
    for id in &ids {
        assert_eq!(next(&mut subscription).await.id(), id);
    }

    assert!(matches!(
        batch
            .publish_batch(
                SUBJECT,
                vec![event("a", "")],
                &Options::default().with_id("x")
            )
            .await,
        Err(StreamError::Unsupported(_))
    ));
}

#[tokio::test]
async fn publish_rejects_an_undeclared_subject() {
    let streams = provider().await;
    let manager = declare(&streams, &[SUBJECT]).await;
    assert!(matches!(
        manager
            .publish("typo", event("", ""), &Options::default())
            .await,
        Err(StreamError::UnknownSubject { .. })
    ));
    assert!(matches!(
        manager.subscribe("typo", &Options::default()).await,
        Err(StreamError::UnknownSubject { .. })
    ));
}

#[tokio::test]
async fn publish_rejects_a_ttl() {
    let streams = provider().await;
    let manager = declare(&streams, &[SUBJECT]).await;
    let opts = Options::default().with_ttl(Duration::from_secs(1));
    assert!(matches!(
        manager.publish(SUBJECT, event("", ""), &opts).await,
        Err(StreamError::Unsupported(_))
    ));
}

#[tokio::test]
async fn consume_requires_a_consumer_name() {
    let streams = provider().await;
    let manager = declare(&streams, &[SUBJECT]).await;
    let durable = as_durable(manager.as_ref()).unwrap();
    assert!(matches!(
        durable.consume(SUBJECT, "", &Options::default()).await,
        Err(StreamError::Unsupported(_))
    ));
}

#[tokio::test]
async fn publishing_after_close_fails() {
    let streams = provider().await;
    let manager = declare(&streams, &[SUBJECT]).await;
    streams.close().await.unwrap();
    assert!(
        manager
            .publish(SUBJECT, event("", ""), &Options::default())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn connect_rejects_an_empty_address() {
    let options = MqttOptions::new("template", "", 1883);
    assert!(
        MqttProvider::connect(options, MqttConfig::default())
            .await
            .is_err()
    );
}

// Headers, expiry, retain and delayed nak.

/// A raw MQTT 5 client subscribed to `filter`, reading the wire as another
/// tool on the broker would.
async fn raw(port: u16, filter: &str) -> (rumqttc::v5::AsyncClient, rumqttc::v5::EventLoop) {
    use rumqttc::v5::{Event as Ev, Incoming, mqttbytes::QoS};
    let options = MqttOptions::new(format!("raw-{}", ulid_ish()), "127.0.0.1", port);
    let (client, mut events) = rumqttc::v5::AsyncClient::new(options, 16);
    client.subscribe(filter, QoS::AtLeastOnce).await.unwrap();
    loop {
        if let Ev::Incoming(Incoming::SubAck(_)) =
            timeout(WITHIN, events.poll()).await.unwrap().unwrap()
        {
            return (client, events);
        }
    }
}

fn ulid_ish() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

async fn raw_next(events: &mut rumqttc::v5::EventLoop) -> rumqttc::v5::mqttbytes::v5::Publish {
    use rumqttc::v5::{Event as Ev, Incoming};
    loop {
        if let Ev::Incoming(Incoming::Publish(publish)) =
            timeout(WITHIN, events.poll()).await.unwrap().unwrap()
        {
            return publish;
        }
    }
}

async fn declare_with(streams: &MqttProvider, stream: Stream) -> (String, Arc<dyn Manager>) {
    let stream = streams.create(stream).await.expect("create");
    let manager = streams.bind(&stream.id).await.expect("bind");
    (stream.id, manager)
}

#[tokio::test]
async fn headers_travel_as_user_properties() {
    let port = broker();
    let streams = provider_on(port).await;
    let (id, manager) = declare_with(
        &streams,
        Stream::named("headers")
            .subject(SUBJECT)
            .header("lang", "rust")
            .header("site", "plant-3"),
    )
    .await;
    let (_raw, mut wire) = raw(port, &format!("{id}/{SUBJECT}")).await;
    let mut subscription = manager
        .subscribe(SUBJECT, &Options::default())
        .await
        .unwrap();

    let opts = Options::default()
        .header("site", "plant-4")
        .header("traceparent", "00-ab");
    let sent = manager
        .publish(SUBJECT, event("ada", "created"), &opts)
        .await
        .unwrap();

    let message = next(&mut subscription).await;
    assert_eq!(message.id(), sent);
    assert_eq!(
        message.headers().get("lang"),
        Some("rust"),
        "stream default"
    );
    assert_eq!(
        message.headers().get("site"),
        Some("plant-4"),
        "publish wins"
    );
    assert_eq!(message.headers().get("traceparent"), Some("00-ab"));
    assert_eq!(message.headers().len(), 3);

    // On the wire: user properties, and a payload a Go reader still reads,
    // a version-1 frame with nothing of the headers in it.
    let publish = raw_next(&mut wire).await;
    let properties = publish.properties.expect("user properties");
    assert_eq!(
        properties.user_properties,
        vec![
            ("lang".to_owned(), "rust".to_owned()),
            ("site".to_owned(), "plant-4".to_owned()),
            ("traceparent".to_owned(), "00-ab".to_owned()),
        ]
    );
    assert_eq!(&publish.payload[..2], &[0xB7, 1], "a version-1 frame");
    let unframed =
        streams::provider::unpack(&streams::Registry::new(), SUBJECT, publish.payload).unwrap();
    assert!(
        unframed.headers().is_empty(),
        "no headers inside the payload"
    );
}

#[tokio::test]
async fn a_message_without_headers_is_what_runtime_go_sends() {
    let port = broker();
    let streams = provider_on(port).await;
    let (id, manager) = declare_with(&streams, Stream::named("plain").subject(SUBJECT)).await;
    let (_raw, mut wire) = raw(port, &format!("{id}/{SUBJECT}")).await;
    let mut subscription = manager
        .subscribe(SUBJECT, &Options::default())
        .await
        .unwrap();
    manager
        .publish(SUBJECT, event("go", ""), &Options::default())
        .await
        .unwrap();
    assert!(next(&mut subscription).await.headers().is_empty());
    let publish = raw_next(&mut wire).await;
    assert!(
        publish
            .properties
            .is_none_or(|p| p.user_properties.is_empty() && p.message_expiry_interval.is_none()),
        "no properties on a header-less publish"
    );

    // And a message another MQTT 5 client sends, with user properties of
    // its own, arrives carrying them.
    let (raw_client, mut raw_events) = raw(port, "unused/topic").await;
    tokio::spawn(async move { while raw_events.poll().await.is_ok() {} });
    let frame = streams::provider::pack("from-go", &event("go", ""));
    raw_client
        .publish_with_properties(
            format!("{id}/{SUBJECT}"),
            rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
            false,
            frame,
            rumqttc::v5::mqttbytes::v5::PublishProperties {
                user_properties: vec![("lang".into(), "go".into())],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let message = next(&mut subscription).await;
    assert_eq!(message.id(), "from-go");
    assert_eq!(message.headers().get("lang"), Some("go"));
}

#[tokio::test]
async fn a_batch_carries_the_headers_on_every_message() {
    let streams = provider().await;
    let (_, manager) = declare_with(
        &streams,
        Stream::named("batch")
            .subject(SUBJECT)
            .header("lang", "rust"),
    )
    .await;
    let batch = as_batch(manager.as_ref()).unwrap();
    let mut subscription = manager
        .subscribe(SUBJECT, &Options::default())
        .await
        .unwrap();
    let ids = batch
        .publish_batch(
            SUBJECT,
            vec![event("a", ""), event("b", "")],
            &Options::default().header("n", "1"),
        )
        .await
        .unwrap();
    for id in &ids {
        let message = next(&mut subscription).await;
        assert_eq!(message.id(), id);
        assert_eq!(message.headers().get("lang"), Some("rust"));
        assert_eq!(message.headers().get("n"), Some("1"));
    }
}

#[tokio::test]
async fn headers_survive_a_redelivery() {
    let streams = provider().await;
    let (_, manager) = declare_with(
        &streams,
        Stream::named("durable")
            .subject(SUBJECT)
            .header("lang", "rust"),
    )
    .await;
    let durable = as_durable(manager.as_ref()).unwrap();
    let mut first = durable
        .consume(SUBJECT, "headers", &Options::default())
        .await
        .unwrap();
    manager
        .publish(
            SUBJECT,
            event("a", ""),
            &Options::default().header("try", "1"),
        )
        .await
        .unwrap();
    let delivery = next_delivery(&mut first).await;
    assert_eq!(delivery.headers().get("try"), Some("1"));
    // Not acknowledged: the session keeps it and hands it back on resume.
    assert!(!delivery.can_delay_redelivery());
    delivery.nak().await.unwrap();
    drop(first);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut again = durable
        .consume(SUBJECT, "headers", &Options::default())
        .await
        .unwrap();
    let redelivered = next_delivery(&mut again).await;
    assert_eq!(redelivered.headers().get("lang"), Some("rust"));
    assert_eq!(redelivered.headers().get("try"), Some("1"));
    redelivered.ack().await.unwrap();
}

#[tokio::test]
async fn delayed_nak_is_refused_by_name() {
    let streams = provider().await;
    let manager = declare(&streams, &[SUBJECT]).await;
    let durable = as_durable(manager.as_ref()).unwrap();
    let mut deliveries = durable
        .consume(SUBJECT, "delay", &Options::default())
        .await
        .unwrap();
    manager
        .publish(SUBJECT, event("a", ""), &Options::default())
        .await
        .unwrap();
    let delivery = next_delivery(&mut deliveries).await;
    assert!(!delivery.can_delay_redelivery());
    match delivery.nak_after(Duration::from_secs(5)).await {
        Err(StreamError::Unsupported(why)) => {
            assert!(why.contains("no negative acknowledgement"), "{why}")
        }
        other => panic!("nak_after: {other:?}"),
    }
}

#[tokio::test]
async fn invalid_headers_are_refused_at_create_update_and_publish() {
    let streams = provider().await;
    let error = streams
        .create(
            Stream::named("bad")
                .subject(SUBJECT)
                .header("has space", "x"),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, StreamError::InvalidHeader(_)), "{error}");

    let (id, manager) = declare_with(&streams, Stream::named("good").subject(SUBJECT)).await;
    let error = streams
        .update(
            &id,
            Stream::named("good")
                .subject(SUBJECT)
                .header("k", "two\nlines"),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, StreamError::InvalidHeader(_)), "{error}");

    for opts in [
        Options::default().header("", "x"),
        Options::default().header("a:b", "x"),
        Options::default().header("k", "v".repeat(64 * 1024)),
    ] {
        let error = manager
            .publish(SUBJECT, event("", ""), &opts)
            .await
            .unwrap_err();
        assert!(matches!(error, StreamError::InvalidHeader(_)), "{error}");
        assert!(error.is_settled());
        let error = as_batch(manager.as_ref())
            .unwrap()
            .publish_batch(SUBJECT, vec![event("", "")], &opts)
            .await
            .unwrap_err();
        assert!(matches!(error, StreamError::InvalidHeader(_)), "{error}");
    }
}

#[tokio::test]
async fn expiry_is_the_message_expiry_interval() {
    let port = broker();
    let streams = provider_on(port).await;
    let (id, manager) = declare_with(&streams, Stream::named("expiry").subject(SUBJECT)).await;
    let (_raw, mut wire) = raw(port, &format!("{id}/{SUBJECT}")).await;
    manager
        .publish(
            SUBJECT,
            event("", ""),
            &Options::default().with_expiry(Duration::from_millis(1500)),
        )
        .await
        .unwrap();
    let publish = raw_next(&mut wire).await;
    let interval = publish
        .properties
        .and_then(|p| p.message_expiry_interval)
        .expect("an expiry interval");
    // Rounded up to whole seconds; a broker forwarding it may count down
    // the time it held the message.
    assert!((1..=2).contains(&interval), "{interval}");
}

#[tokio::test]
async fn a_retained_message_reaches_a_later_subscriber_and_can_be_cleared() {
    let streams = provider().await;
    let (id, manager) = declare_with(&streams, Stream::named("retain").subject(SUBJECT)).await;
    let sent = manager
        .publish(
            SUBJECT,
            event("last", "value"),
            &Options::default().with_retain().header("lang", "rust"),
        )
        .await
        .unwrap();

    // Subscribed after the publish, and still handed it, decoded.
    let mut late = manager
        .subscribe(SUBJECT, &Options::default())
        .await
        .unwrap();
    let message = next(&mut late).await;
    assert_eq!(message.id(), sent);
    assert_eq!(message.json::<Event>().unwrap().user, "last");
    assert_eq!(message.headers().get("lang"), Some("rust"));

    // Cleared: the empty retained publish reaches the live subscriber and
    // is skipped, and a subscriber joining later gets nothing.
    streams.clear_retained(&id, SUBJECT).await.unwrap();
    let after = manager
        .publish(SUBJECT, event("next", ""), &Options::default())
        .await
        .unwrap();
    assert_eq!(next(&mut late).await.id(), after, "the clear was skipped");
    let mut later = manager
        .subscribe(SUBJECT, &Options::default())
        .await
        .unwrap();
    assert!(
        timeout(Duration::from_millis(500), later.next())
            .await
            .is_err(),
        "nothing is retained after the clear"
    );
}
