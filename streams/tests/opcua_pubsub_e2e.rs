//! OPC UA PubSub over the secure Mosquitto the MQTT e2e suite uses: mutual
//! TLS, then a username and password, under ACLs that give each user only
//! the topics below its own name.
//!
//! Start the broker with
//! `docker compose -p tpp-streams-e2e-mqtt -f streams/docker/compose.mqtt.secure.yaml up -d --wait`,
//! then run
//! `cargo test -p tpp-streams --features opcua,tls,live-tests --test opcua_pubsub_e2e`.
//!
//! Part 14 topics start with a prefix, `opcua` by default; under these ACLs
//! the prefix is `e2e-app/<run>/opcua`, so the topic tree is the one the user
//! owns. `MQTT_E2E_PASSWORD_ADDR` (default `127.0.0.1:18884`) and
//! `STREAMS_E2E_TLS_DIR` (default `docker/tls/out`) point it elsewhere.

use std::{path::PathBuf, sync::Arc, time::Duration};

use futures::StreamExt;
use rumqttc::{TlsConfiguration, Transport};
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};
use streams::{
    Manager, Message, Options, Stream, StreamError, Streams, Subscription, as_batch,
    drivers::opcua::{
        CODEC_NAME, DataValue, OpcUaValue, Variant,
        pubsub::{DataSet, MqttOptions, PubSubConfig, PubSubProvider},
    },
};
use tokio::time::timeout;

const WITHIN: Duration = Duration::from_secs(10);
const APP: (&str, &str) = ("e2e-app", "e2e-app-secret");
const OTHER: (&str, &str) = ("e2e-other", "e2e-other-secret");

/// The test certificate directory, `STREAMS_E2E_TLS_DIR` or docker/tls/out.
fn tls_dir() -> PathBuf {
    std::env::var("STREAMS_E2E_TLS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/docker/tls/out")))
}

/// Mutual TLS on ring: the test CA, the test client certificate.
fn tls() -> Arc<ClientConfig> {
    let dir = tls_dir();
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(dir.join("ca.crt")).expect("read ca.crt") {
        roots
            .add(cert.expect("parse ca.crt"))
            .expect("trust ca.crt");
    }
    let chain = CertificateDer::pem_file_iter(dir.join("client.crt"))
        .expect("read client.crt")
        .collect::<Result<Vec<_>, _>>()
        .expect("parse client.crt");
    let key = PrivateKeyDer::from_pem_file(dir.join("client.key")).expect("read client.key");
    Arc::new(
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .expect("ring supports the default protocol versions")
            .with_root_certificates(roots)
            .with_client_auth_cert(chain, key)
            .expect("client certificate"),
    )
}

/// A template for the broker's password listener as `user`, over mutual TLS,
/// with a 1 MiB packet limit for NetworkMessages larger than rumqttc's default.
fn options((user, password): (&str, &str)) -> MqttOptions {
    let addr = std::env::var("MQTT_E2E_PASSWORD_ADDR").unwrap_or_else(|_| "127.0.0.1:18884".into());
    let (host, port) = addr.rsplit_once(':').expect("host:port");
    let mut options = MqttOptions::new("template", host, port.parse().expect("a port"));
    options.set_connection_timeout(5);
    options.set_max_packet_size(Some(1 << 20));
    options.set_transport(Transport::tls_with_config(TlsConfiguration::Rustls(tls())));
    options.set_credentials(user, password);
    options
}

/// A prefix no earlier run used, inside `user`'s topic tree, so retained
/// metadata from a previous run is never read as this one's.
fn prefix(user: &str) -> String {
    format!(
        "{user}/pubsub-{}/opcua",
        ulid::Ulid::new().to_string().to_lowercase()
    )
}

/// A PubSub provider as `user` publishing under `prefix`.
async fn connect(user: (&str, &str), prefix: &str) -> PubSubProvider {
    PubSubProvider::connect(
        options(user),
        PubSubConfig {
            publisher_id: "plant-e2e".into(),
            prefix: prefix.into(),
            ..PubSubConfig::default()
        },
    )
    .await
    .unwrap_or_else(|e| panic!("connect as {} over mutual TLS: {e}", user.0))
}

/// Declares stream `line-a` over `subjects` and binds a manager to it.
async fn declare(streams: &PubSubProvider, subjects: &[&str]) -> Arc<dyn Manager> {
    let stream = streams
        .create(Stream {
            id: "line-a".into(),
            subjects: subjects.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        })
        .await
        .unwrap();
    streams.bind(&stream.id).await.unwrap()
}

/// The next message within [`WITHIN`], panicking on a timeout or an ended subscription.
async fn next(subscription: &mut Subscription) -> Message {
    timeout(WITHIN, subscription.next())
        .await
        .expect("a message within the deadline")
        .expect("the subscription ended")
}

/// The OPC UA value a message carries, panicking if it has none.
fn value(message: &Message) -> Variant {
    OpcUaValue::data_value(message).unwrap().value.unwrap()
}

#[tokio::test]
async fn pubsub_round_trips_over_mutual_tls_and_a_password() {
    let prefix = prefix(APP.0);
    let streams = connect(APP, &prefix).await;
    let manager = declare(&streams, &["boiler"]).await;
    let mut all = manager
        .subscribe("boiler", &Options::default())
        .await
        .unwrap();

    let id = manager
        .publish(
            "boiler/Temperature",
            OpcUaValue::payload(21.5f64),
            &Options::default(),
        )
        .await
        .unwrap();
    let message = next(&mut all).await;
    assert_eq!(message.id(), id);
    assert_eq!(message.subject(), "boiler/Temperature");
    assert_eq!(message.codec(), CODEC_NAME);
    assert_eq!(value(&message), Variant::from(21.5f64));

    let set = DataSet::new()
        .with("Temperature", DataValue::value_only(22.0f64))
        .with("Running", DataValue::value_only(true));
    manager
        .publish("boiler", set.payload(), &Options::default())
        .await
        .unwrap();
    assert_eq!(value(&next(&mut all).await), Variant::from(22.0f64));
    assert_eq!(value(&next(&mut all).await), Variant::from(true));

    let ids = as_batch(manager.as_ref())
        .unwrap()
        .publish_batch(
            "boiler/Count",
            (0..20i32).map(OpcUaValue::payload).collect(),
            &Options::default(),
        )
        .await
        .unwrap();
    for (n, id) in ids.iter().enumerate() {
        let message = next(&mut all).await;
        assert_eq!(message.id(), id);
        assert_eq!(value(&message), Variant::from(n as i32));
    }
    assert_eq!(streams.sequence_gaps(), 0);
    drop(all);
    streams.close().await.unwrap();
}

#[tokio::test]
async fn metadata_is_retained_under_the_acls() {
    let prefix = prefix(APP.0);
    let streams = connect(APP, &prefix).await;
    declare(&streams, &["boiler"]).await;
    let set = DataSet::new().with("Temperature", DataValue::value_only(1.0f64));
    let version = streams
        .publish_metadata("line-a", "boiler", set.metadata("boiler"))
        .await
        .unwrap();
    // A second provider, connecting later, reads the retained message.
    let reader = connect(APP, &prefix).await;
    reader
        .create(Stream {
            id: "plant-e2e/line-a".into(),
            subjects: vec!["boiler".into()],
            ..Default::default()
        })
        .await
        .unwrap();
    let read = reader.metadata("plant-e2e/line-a", "boiler").await.unwrap();
    assert_eq!(read.meta_data.configuration_version, version);
    assert_eq!(
        read.meta_data.fields.unwrap()[0].name.as_ref(),
        "Temperature"
    );
    streams.close().await.unwrap();
    reader.close().await.unwrap();
}

#[tokio::test]
async fn the_acls_hold() {
    // Part 14's bare `opcua/...` tree is nobody's: the broker refuses the
    // publish with reason 0x87, and the error says so.
    let streams = connect(APP, "opcua").await;
    let manager = declare(&streams, &["w/f"]).await;
    let error = manager
        .publish("w/f", OpcUaValue::payload(1i32), &Options::default())
        .await
        .unwrap_err();
    assert!(matches!(error, StreamError::Internal(_)), "{error}");
    assert!(error.to_string().contains("NotAuthorized"), "{error}");
    streams.close().await.unwrap();

    // Another user may subscribe to e2e-app's topics, but is handed nothing.
    let prefix = prefix(APP.0);
    let app = connect(APP, &prefix).await;
    let writer = declare(&app, &["w/f"]).await;
    let snoop = connect(OTHER, &prefix).await;
    let mut stolen = declare(&snoop, &["w/f"])
        .await
        .subscribe("w/f", &Options::default())
        .await
        .unwrap();
    let mut own = writer.subscribe("w/f", &Options::default()).await.unwrap();
    writer
        .publish("w/f", OpcUaValue::payload(7i32), &Options::default())
        .await
        .unwrap();
    assert_eq!(value(&next(&mut own).await), Variant::from(7i32));
    assert!(
        timeout(Duration::from_millis(500), stolen.next())
            .await
            .is_err(),
        "the ACL kept e2e-other out"
    );
    app.close().await.unwrap();
    snoop.close().await.unwrap();
}

#[tokio::test]
async fn a_wrong_password_is_refused() {
    let error = PubSubProvider::connect(
        options((APP.0, "wrong")),
        PubSubConfig {
            prefix: prefix(APP.0),
            ..PubSubConfig::default()
        },
    )
    .await
    .err()
    .expect("the broker refuses a wrong password");
    assert!(matches!(error, StreamError::Internal(_)), "{error}");
}

/// `mosquitto_sub` inside the broker's container (on its certificate-identity
/// listener, which has no ACLs), as MQTT 5, printing each message's user
/// properties and topic: another tool reading what the provider wrote.
fn mosquitto_sub(topic: &str) -> std::process::Command {
    let container =
        std::env::var("MQTT_E2E_CONTAINER").unwrap_or_else(|_| "tpp-streams-mqtt-secure".into());
    let mut command = std::process::Command::new("docker");
    command.args([
        "exec",
        &container,
        "mosquitto_sub",
        "-h",
        "localhost",
        "-p",
        "8883",
        "--cafile",
        "/certs/ca.crt",
        "--cert",
        "/certs/client.crt",
        "--key",
        "/certs/client.key",
        "-V",
        "mqttv5",
        "-F",
        "%P %t",
        "-t",
        topic,
        "-C",
        "1",
        "-W",
        "15",
    ]);
    command
}

// Headers ride as MQTT 5 user properties beside the Part 14 JSON, through
// the ACLs, and another MQTT 5 tool on the broker sees them.
#[tokio::test]
async fn headers_are_user_properties_another_tool_sees() {
    let prefix = prefix(APP.0);
    let streams = connect(APP, &prefix).await;
    let stream = streams
        .create(
            Stream::named("h")
                .with_id("line-h")
                .subject("boiler")
                .header("lang", "rust"),
        )
        .await
        .unwrap();
    let manager = streams.bind(&stream.id).await.unwrap();
    let mut all = manager
        .subscribe("boiler", &Options::default())
        .await
        .unwrap();

    let opts = Options::default()
        .header("site", "plant-3")
        .with_expiry(Duration::from_secs(30));
    manager
        .publish("boiler/Temperature", OpcUaValue::payload(21.5f64), &opts)
        .await
        .unwrap();
    let message = next(&mut all).await;
    assert_eq!(value(&message), Variant::from(21.5f64));
    assert_eq!(message.headers().get("lang"), Some("rust"));
    assert_eq!(message.headers().get("site"), Some("plant-3"));

    // The retained metadata carries the stream's headers; the tool started
    // afterwards is handed it at once.
    let set = DataSet::new().with("Temperature", DataValue::value_only(1.0f64));
    streams
        .publish_metadata("line-h", "boiler", set.metadata("boiler"))
        .await
        .unwrap();
    let topic = format!("{prefix}/json/metadata/plant-e2e/line-h/boiler");
    let out = mosquitto_sub(&topic).output().expect("run mosquitto_sub");
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        format!("lang:rust {topic}"),
        "mosquitto_sub: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Live data: the tool subscribes first; publish until it has heard one.
    let topic = format!("{prefix}/json/data/plant-e2e/line-h/boiler");
    let mut child = mosquitto_sub(&topic)
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("spawn mosquitto_sub");
    for _ in 0..50 {
        manager
            .publish("boiler/Temperature", OpcUaValue::payload(2.0f64), &opts)
            .await
            .unwrap();
        if child.try_wait().unwrap().is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let out = child.wait_with_output().unwrap();
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        format!("lang:rust site:plant-3 {topic}")
    );

    // Retain on data is refused by name.
    assert!(matches!(
        manager
            .publish(
                "boiler/Temperature",
                OpcUaValue::payload(1.0f64),
                &Options::default().with_retain(),
            )
            .await,
        Err(StreamError::Unsupported(_))
    ));
    drop(all);
    streams.close().await.unwrap();
}
