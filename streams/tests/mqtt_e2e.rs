//! The MQTT provider end to end against Mosquitto over mutual TLS, with
//! persistent sessions on disk, authenticated two ways: by the client
//! certificate's identity on one listener, and by username and password, under
//! ACLs, on another.
//!
//! Start the broker with
//! `docker compose -p tpp-streams-e2e-mqtt -f streams/docker/compose.mqtt.secure.yaml up -d --wait`,
//! then run
//! `STREAMS_E2E_RESTART=1 cargo test -p tpp-streams --features mqtt,tls,live-tests --test mqtt_e2e`.
//!
//! `MQTT_E2E_ADDR` (default `127.0.0.1:18883`, the certificate-identity
//! listener), `MQTT_E2E_PASSWORD_ADDR` (default `127.0.0.1:18884`, the
//! password listener), `STREAMS_E2E_TLS_DIR` (default
//! `docker/tls/out`), and `MQTT_E2E_CONTAINER` (default
//! `tpp-streams-mqtt-secure`) point it elsewhere. The restart test runs only
//! with `STREAMS_E2E_RESTART=1`, because it restarts that container.
//!
//! Every test holds one lock: the restart test takes the broker away from
//! everything else, and the `$SYS` counts the cleanup test reads are global.

use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    process::Command,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::StreamExt;
use rumqttc::{
    TlsConfiguration, Transport,
    v5::{AsyncClient, Event, Incoming},
};
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};
use serde::{Deserialize, Serialize};
use streams::{
    Deliveries, Delivery, Manager, Message, Options, Payload, Stream, StreamError, Streams,
    Subscription, as_batch, as_durable, as_positioned,
    drivers::mqtt::{MqttConfig, MqttOptions, MqttProvider, QoS},
};
use tokio::time::timeout;

const SUBJECT: &str = "user/created";
const WITHIN: Duration = Duration::from_secs(10);
/// Short enough that sessions these tests leave behind expire on their own,
/// long enough to outlive a broker restart.
const SESSION_EXPIRY: Duration = Duration::from_secs(300);

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
struct Event2 {
    user: String,
    action: String,
}

/// The test-only users of the password listener, from
/// `docker/compose.mqtt.secure.yaml`. `docker/compose.mqtt.secure.acl` confines
/// each to the topics under its own name.
const APP: (&str, &str) = ("e2e-app", "e2e-app-secret");
const OTHER: (&str, &str) = ("e2e-other", "e2e-other-secret");

fn parse_addr(var: &str, default: &str) -> (String, u16) {
    let addr = std::env::var(var).unwrap_or_else(|_| default.into());
    let (host, port) = addr
        .rsplit_once(':')
        .unwrap_or_else(|| panic!("{var} is host:port"));
    (host.to_owned(), port.parse().expect("a port number"))
}

/// The listener where the certificate's common name is the username.
fn addr() -> (String, u16) {
    parse_addr("MQTT_E2E_ADDR", "127.0.0.1:18883")
}

/// The listener that wants a certificate and then a username and password.
fn password_addr() -> (String, u16) {
    parse_addr("MQTT_E2E_PASSWORD_ADDR", "127.0.0.1:18884")
}

fn tls_dir() -> PathBuf {
    std::env::var("STREAMS_E2E_TLS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/docker/tls/out")))
}

fn container() -> String {
    std::env::var("MQTT_E2E_CONTAINER").unwrap_or_else(|_| "tpp-streams-mqtt-secure".into())
}

/// A rustls client config on ring, trusting the test CA when `trust_ca`, and
/// presenting the test client certificate when `client_cert`.
fn tls(client_cert: bool, trust_ca: bool) -> Arc<ClientConfig> {
    let dir = tls_dir();
    let mut roots = RootCertStore::empty();
    if trust_ca {
        for cert in CertificateDer::pem_file_iter(dir.join("ca.crt")).expect("read ca.crt") {
            roots
                .add(cert.expect("parse ca.crt"))
                .expect("trust ca.crt");
        }
    }
    let builder =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .expect("ring supports the default protocol versions")
            .with_root_certificates(roots);
    let config = if client_cert {
        let chain = CertificateDer::pem_file_iter(dir.join("client.crt"))
            .expect("read client.crt")
            .collect::<Result<Vec<_>, _>>()
            .expect("parse client.crt");
        let key = PrivateKeyDer::from_pem_file(dir.join("client.key")).expect("read client.key");
        builder
            .with_client_auth_cert(chain, key)
            .expect("client certificate")
    } else {
        builder.with_no_client_auth()
    };
    Arc::new(config)
}

/// The connection template: the certificate-identity address, and TLS when
/// given.
fn options(tls: Option<Arc<ClientConfig>>) -> MqttOptions {
    options_at(addr(), tls)
}

fn options_at((host, port): (String, u16), tls: Option<Arc<ClientConfig>>) -> MqttOptions {
    let mut options = MqttOptions::new("template", host, port);
    options.set_connection_timeout(5);
    if let Some(config) = tls {
        options.set_transport(Transport::tls_with_config(TlsConfiguration::Rustls(config)));
    }
    options
}

fn config(prefix: &str) -> MqttConfig {
    MqttConfig {
        prefix: prefix.into(),
        qos: QoS::AtLeastOnce,
        session_expiry: SESSION_EXPIRY,
        ..Default::default()
    }
}

/// A topic prefix no earlier run used, so sessions a previous run left on the
/// broker never see this run's messages.
fn fresh(tag: &str) -> String {
    format!("e2e-{tag}-{}", ulid::Ulid::new().to_string().to_lowercase())
}

async fn connect(prefix: &str) -> MqttProvider {
    MqttProvider::connect(options(Some(tls(true, true))), config(prefix))
        .await
        .expect("connect over mutual TLS")
}

/// The password listener's template, over mutual TLS, logging in as `user`
/// with `password` when given. The provider clones it for every connection it
/// opens, so the credentials have to survive that to reach subscribers,
/// durable sessions, and shared subscriptions.
fn password_options(user: Option<&str>, password: Option<&str>) -> MqttOptions {
    let mut options = options_at(password_addr(), Some(tls(true, true)));
    if let Some(user) = user {
        options.set_credentials(user, password.unwrap_or_default());
    }
    options
}

/// A provider logged in as `user` on the password listener.
async fn connect_as((user, password): (&str, &str), prefix: &str) -> MqttProvider {
    MqttProvider::connect(password_options(Some(user), Some(password)), config(prefix))
        .await
        .unwrap_or_else(|e| panic!("connect as {user} over mutual TLS: {e}"))
}

/// A fresh prefix inside the topic tree the ACLs give `user`.
fn fresh_for((user, _): (&str, &str), tag: &str) -> String {
    format!("{user}/{}", fresh(tag))
}

async fn declare(streams: &MqttProvider, id: &str, subjects: &[&str]) -> Arc<dyn Manager> {
    let stream = streams
        .create(Stream {
            id: id.into(),
            name: "e2e".into(),
            subjects: subjects.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        })
        .await
        .expect("create");
    streams.bind(&stream.id).await.expect("bind")
}

fn event(user: &str, action: &str) -> Payload {
    Payload::json(&Event2 {
        user: user.into(),
        action: action.into(),
    })
    .unwrap()
}

async fn next(subscription: &mut Subscription) -> Message {
    timeout(WITHIN, subscription.next())
        .await
        .expect("a message within the deadline")
        .expect("subscription ended before a message arrived")
}

async fn next_delivery(deliveries: &mut Deliveries) -> Delivery {
    timeout(WITHIN, deliveries.next())
        .await
        .expect("a delivery within the deadline")
        .expect("deliveries ended before a message arrived")
}

/// Collects every delivery that arrives within `window`, acknowledging each.
async fn drain(deliveries: &mut Deliveries, window: Duration) -> Vec<String> {
    let mut ids = Vec::new();
    while let Ok(Some(delivery)) = timeout(window, deliveries.next()).await {
        ids.push(delivery.id().to_owned());
        delivery.ack().await.unwrap();
    }
    ids
}

/// An acknowledgement is a packet on the wire; give it time to leave before
/// the connection closes.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(500)).await;
}

#[tokio::test]
async fn tls_is_required_and_mutual() {
    let _serial = SERIAL.lock().await;

    // Plaintext MQTT to the TLS port: the broker reads a CONNECT where it
    // expected a ClientHello and hangs up.
    assert!(
        MqttProvider::connect(options(None), config("plain"))
            .await
            .is_err(),
        "a plaintext connection to the secure port succeeded"
    );

    // TLS without a client certificate: require_certificate refuses it.
    assert!(
        MqttProvider::connect(options(Some(tls(false, true))), config("nocert"))
            .await
            .is_err(),
        "the broker accepted a TLS client without a certificate"
    );

    // TLS that does not trust the CA: the client refuses the broker.
    assert!(
        MqttProvider::connect(options(Some(tls(true, false))), config("untrusted"))
            .await
            .is_err(),
        "the client accepted a broker certificate from an untrusted CA"
    );

    // Mutual TLS works, end to end.
    let streams = connect(&fresh("tls")).await;
    let manager = declare(&streams, "tls", &[SUBJECT]).await;
    let mut subscription = manager
        .subscribe(SUBJECT, &Options::default())
        .await
        .unwrap();
    let id = manager
        .publish(SUBJECT, event("ada", "tls"), &Options::default())
        .await
        .unwrap();
    assert_eq!(next(&mut subscription).await.id(), id);
    drop(subscription);
    streams.close().await.unwrap();
}

/// Connects to the password listener with `user` and `password`, expecting a
/// refusal, and returns what the provider said.
async fn refused(user: Option<&str>, password: Option<&str>) -> String {
    match MqttProvider::connect(password_options(user, password), config("refused")).await {
        Ok(_) => panic!("the password listener accepted user {user:?} with password {password:?}"),
        Err(error) => error.to_string(),
    }
}

// The password listener takes a certificate and then a login. Mosquitto
// answers every bad login with CONNACK reason 0x87, not authorized, rather
// than 0x86, bad username or password, so the two cannot be told apart from
// the client, and the provider reports the broker's reason as given.
#[tokio::test]
async fn passwords_are_checked() {
    let _serial = SERIAL.lock().await;

    for (user, password, what) in [
        (Some(APP.0), Some("not-the-password"), "a wrong password"),
        (Some(APP.0), None, "a missing password"),
        (Some(APP.0), Some(OTHER.1), "another user's password"),
        (Some("nobody"), Some("anything"), "an unknown user"),
        // The certificate alone is the whole login on 18883, and none here.
        (None, None, "no credentials at all"),
    ] {
        let error = refused(user, password).await;
        assert!(
            error.contains("NotAuthorized"),
            "{what} was refused, but not as not authorized: {error}"
        );
    }

    // Without a client certificate the right password never gets that far:
    // the TLS handshake fails first.
    let mut no_cert = options_at(password_addr(), Some(tls(false, true)));
    no_cert.set_credentials(APP.0, APP.1);
    assert!(
        MqttProvider::connect(no_cert, config("nocert"))
            .await
            .is_err(),
        "the password listener accepted a client without a certificate"
    );

    // The right password over mutual TLS works end to end.
    let streams = connect_as(APP, &fresh_for(APP, "login")).await;
    let manager = declare(&streams, "login", &[SUBJECT]).await;
    let mut subscription = manager
        .subscribe(SUBJECT, &Options::default())
        .await
        .unwrap();
    let id = manager
        .publish(SUBJECT, event("ada", "login"), &Options::default())
        .await
        .unwrap();
    assert_eq!(next(&mut subscription).await.id(), id);
    drop(subscription);
    streams.close().await.unwrap();
}

/// Waits `window` and asserts nothing arrived on `subscription`.
async fn hears_nothing(subscription: &mut Subscription, window: Duration, what: &str) {
    if let Ok(message) = timeout(window, subscription.next()).await {
        panic!("{what} received {message:?}");
    }
}

// Each password user is confined to its own topic tree, and the ACLs are the
// password listener's alone: the certificate identity on 18883 is not
// confined.
#[tokio::test]
async fn acls_confine_each_user() {
    let _serial = SERIAL.lock().await;
    let theirs = fresh_for(OTHER, "acl");
    const QUIET: Duration = Duration::from_millis(1500);

    // e2e-other, in its own tree, sees its own messages.
    let owner = connect_as(OTHER, &theirs).await;
    let owned = declare(&owner, "acl", &[SUBJECT]).await;
    let mut owners_view = owned.subscribe(SUBJECT, &Options::default()).await.unwrap();

    // e2e-app aimed at e2e-other's tree: the same topics, its own login.
    // Its durable session expires a second after it leaves, rather than
    // lingering to move the broker's counts under a later test.
    let intruder = MqttProvider::connect(
        password_options(Some(APP.0), Some(APP.1)),
        MqttConfig {
            session_expiry: Duration::from_secs(1),
            ..config(&theirs)
        },
    )
    .await
    .expect("connect as e2e-app");
    let trespass = declare(&intruder, "acl", &[SUBJECT]).await;

    // Publishing outside its tree is refused outright: Mosquitto answers the
    // QoS 1 PUBLISH with PUBACK reason 0x87, and the provider fails the call
    // with that reason rather than reporting it published.
    let error = trespass
        .publish(SUBJECT, event("mallory", "write"), &Options::default())
        .await
        .expect_err("a publish outside the user's topics was acknowledged")
        .to_string();
    assert!(error.contains("NotAuthorized"), "{error}");
    hears_nothing(&mut owners_view, QUIET, "the owner, of a refused publish").await;

    // Subscribing outside its tree is not refused: Mosquitto's acl_file
    // grants the SUBACK and enforces read access per message instead. So the
    // subscription, a plain one, a shared one, and a durable one alike, is
    // made, and hears nothing of what is published there.
    let mut plain = trespass
        .subscribe(SUBJECT, &Options::default())
        .await
        .expect("Mosquitto grants the SUBACK; it denies each message instead");
    let mut shared = trespass
        .subscribe(SUBJECT, &Options::default().with_group(fresh("spies")))
        .await
        .expect("a shared subscription is granted the same way");
    let mut durable = as_durable(trespass.as_ref())
        .unwrap()
        .consume(SUBJECT, &fresh("spy"), &Options::default())
        .await
        .expect("a durable subscription is granted the same way");
    let id = owned
        .publish(SUBJECT, event("ada", "secret"), &Options::default())
        .await
        .unwrap();
    // It was published: its owner has it.
    assert_eq!(next(&mut owners_view).await.id(), id);
    hears_nothing(
        &mut plain,
        QUIET,
        "a subscription outside the user's topics",
    )
    .await;
    hears_nothing(&mut shared, QUIET, "a shared subscription outside them").await;
    if let Ok(delivery) = timeout(QUIET, durable.next()).await {
        panic!("a durable consumer outside the user's topics received {delivery:?}");
    }
    drop((plain, shared, durable));
    intruder.close().await.unwrap();

    // The certificate identity on 18883 is under no ACL: the same topics are
    // open to it, both ways.
    let identity = connect(&theirs).await;
    let unconfined = declare(&identity, "acl", &[SUBJECT]).await;
    let mut identitys_view = unconfined
        .subscribe(SUBJECT, &Options::default())
        .await
        .unwrap();
    let id = unconfined
        .publish(SUBJECT, event("cert", "write"), &Options::default())
        .await
        .expect("the certificate identity is not confined");
    assert_eq!(next(&mut identitys_view).await.id(), id);
    assert_eq!(next(&mut owners_view).await.id(), id);
    drop(identitys_view);
    identity.close().await.unwrap();

    drop(owners_view);
    owner.close().await.unwrap();
}

#[tokio::test]
async fn lifecycle() {
    let _serial = SERIAL.lock().await;
    // As the password user: its subscription is a connection of its own, which
    // logs in with the template's credentials.
    let streams = connect_as(APP, &fresh_for(APP, "life")).await;

    let explicit = streams
        .create(Stream {
            id: "orders".into(),
            name: "orders".into(),
            description: "before".into(),
            subjects: vec![SUBJECT.into()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(explicit.id, "orders");
    assert!(explicit.active);

    let generated = streams
        .create(Stream {
            name: "with/slashes+and#hashes".into(),
            subjects: vec![SUBJECT.into()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(!generated.id.is_empty(), "create did not assign an id");
    assert!(
        !generated.id.contains(['/', '+', '#']),
        "a generated id carries topic structure: {}",
        generated.id
    );

    assert_eq!(streams.get("orders").await.unwrap(), explicit);
    let mut listed: Vec<String> = streams
        .list()
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.id)
        .collect();
    listed.sort();
    let mut expected = vec![explicit.id.clone(), generated.id.clone()];
    expected.sort();
    assert_eq!(listed, expected);

    let updated = streams
        .update(
            "orders",
            Stream {
                name: "orders".into(),
                description: "after".into(),
                subjects: vec![SUBJECT.into(), "user/deleted".into()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.id, "orders");
    assert_eq!(streams.get("orders").await.unwrap().description, "after");

    // The update is what a later bind sees.
    let manager = streams.bind("orders").await.unwrap();
    let mut subscription = manager
        .subscribe("user/deleted", &Options::default())
        .await
        .unwrap();
    manager
        .publish("user/deleted", event("ada", "deleted"), &Options::default())
        .await
        .unwrap();
    assert_eq!(next(&mut subscription).await.subject(), "user/deleted");
    drop(subscription);

    for id in [&explicit.id, &generated.id] {
        streams.delete(id).await.unwrap();
        assert!(matches!(
            streams.get(id).await,
            Err(StreamError::NotFound(_))
        ));
        assert!(matches!(
            streams.bind(id).await,
            Err(StreamError::NotFound(_))
        ));
        assert!(matches!(
            streams.update(id, Stream::default()).await,
            Err(StreamError::NotFound(_))
        ));
        assert!(matches!(
            streams.delete(id).await,
            Err(StreamError::NotFound(_))
        ));
    }
    assert!(streams.list().await.unwrap().is_empty());
    streams.close().await.unwrap();
}

#[tokio::test]
async fn publish_subscribe_options_and_typed() {
    let _serial = SERIAL.lock().await;
    let streams = connect(&fresh("pubsub")).await;
    let manager = declare(&streams, "users", &[SUBJECT, "device/+"]).await;

    // A chosen id travels in the frame; a partition key is accepted and has
    // nothing to decide; prefetch bounds the buffer, not delivery.
    let mut subscription = manager
        .subscribe(SUBJECT, &Options::default().with_prefetch(1))
        .await
        .unwrap();
    let id = manager
        .publish(
            SUBJECT,
            event("ada", "created"),
            &Options::default()
                .with_id("chosen-id")
                .with_partition_key("ada"),
        )
        .await
        .unwrap();
    assert_eq!(id, "chosen-id");
    let message = next(&mut subscription).await;
    assert_eq!(message.id(), "chosen-id");
    assert_eq!(message.subject(), SUBJECT);
    assert_eq!(message.codec(), "json");
    assert_eq!(
        message.json::<Event2>().unwrap(),
        Event2 {
            user: "ada".into(),
            action: "created".into()
        }
    );
    for i in 0..5 {
        manager
            .publish(SUBJECT, event("u", &i.to_string()), &Options::default())
            .await
            .unwrap();
    }
    for i in 0..5 {
        let message = next(&mut subscription).await;
        assert_eq!(message.json::<Event2>().unwrap().action, i.to_string());
    }
    // A zero TTL is "no delay", as in Go.
    manager
        .publish(
            SUBJECT,
            event("zero", ""),
            &Options::default().with_ttl(Duration::ZERO),
        )
        .await
        .unwrap();
    next(&mut subscription).await;
    drop(subscription);

    // The typed view over the same manager.
    let typed = streams::typed::<Event2>(manager.clone());
    let mut values = typed.subscribe(SUBJECT, &Options::default()).await.unwrap();
    let value = Event2 {
        user: "grace".into(),
        action: "typed".into(),
    };
    typed
        .publish(SUBJECT, &value, &Options::default())
        .await
        .unwrap();
    assert_eq!(
        timeout(WITHIN, values.next()).await.unwrap().unwrap(),
        value
    );
    drop(values);

    // A declared filter subscribes to every subject below it, and reports
    // which one each message came in on.
    let mut devices = manager
        .subscribe("device/+", &Options::default())
        .await
        .unwrap();
    manager
        .publish("device/7", event("d", ""), &Options::default())
        .await
        .unwrap();
    assert_eq!(next(&mut devices).await.subject(), "device/7");
    drop(devices);

    // Refusals.
    assert!(matches!(
        manager
            .publish("device/+", event("", ""), &Options::default())
            .await,
        Err(StreamError::Unsupported(_))
    ));
    assert!(matches!(
        manager
            .publish(
                SUBJECT,
                event("", ""),
                &Options::default().with_ttl(Duration::from_secs(1))
            )
            .await,
        Err(StreamError::Unsupported(_))
    ));
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
    let durable = as_durable(manager.as_ref()).unwrap();
    assert!(matches!(
        durable.consume("typo", "c", &Options::default()).await,
        Err(StreamError::UnknownSubject { .. })
    ));
    assert!(matches!(
        durable.consume(SUBJECT, "", &Options::default()).await,
        Err(StreamError::Unsupported(_))
    ));
    streams.close().await.unwrap();
    assert!(
        manager
            .publish(SUBJECT, event("", ""), &Options::default())
            .await
            .is_err(),
        "publishing after close succeeded"
    );
}

#[tokio::test]
async fn capabilities_and_batch() {
    let _serial = SERIAL.lock().await;
    let streams = connect(&fresh("caps")).await;
    let manager = declare(&streams, "caps", &[SUBJECT]).await;

    assert!(as_durable(manager.as_ref()).is_ok());
    // A session is a queue, not a log: there is no earliest to replay from.
    assert!(matches!(
        as_positioned(manager.as_ref()),
        Err(StreamError::Unsupported(_))
    ));
    let batch = as_batch(manager.as_ref()).unwrap();

    let mut subscription = manager
        .subscribe(SUBJECT, &Options::default())
        .await
        .unwrap();
    let ids = batch
        .publish_batch(
            SUBJECT,
            vec![event("a", ""), event("b", ""), event("c", "")],
            &Options::default().with_partition_key("k"),
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
    assert!(matches!(
        batch
            .publish_batch("typo", vec![event("a", "")], &Options::default())
            .await,
        Err(StreamError::UnknownSubject { .. })
    ));
    drop(subscription);
    streams.close().await.unwrap();
}

// Ack settles; nak leaves the message with the session, and it comes back
// when the consumer does. MQTT does not count attempts, so attempt is zero.
#[tokio::test]
async fn durable_ack_nak_and_redelivery() {
    let _serial = SERIAL.lock().await;
    let streams = connect(&fresh("durable")).await;
    let manager = declare(&streams, "durable", &[SUBJECT]).await;
    let durable = as_durable(manager.as_ref()).unwrap();
    let consumer = fresh("billing");
    let opts = Options::default().with_prefetch(1);

    let mut deliveries = durable.consume(SUBJECT, &consumer, &opts).await.unwrap();
    let acked = manager
        .publish(SUBJECT, event("acked", ""), &Options::default())
        .await
        .unwrap();
    let naked = manager
        .publish(SUBJECT, event("naked", ""), &Options::default())
        .await
        .unwrap();

    let first = next_delivery(&mut deliveries).await;
    assert_eq!(first.id(), acked);
    assert_eq!(first.attempt(), 0);
    first.ack().await.unwrap();
    let second = next_delivery(&mut deliveries).await;
    assert_eq!(second.id(), naked);
    second.nak().await.unwrap();
    settle().await;
    drop(deliveries);
    settle().await;

    // Published while the consumer is away: the session queues it.
    let away = manager
        .publish(SUBJECT, event("away", ""), &Options::default())
        .await
        .unwrap();

    let mut again = durable.consume(SUBJECT, &consumer, &opts).await.unwrap();
    let ids = drain(&mut again, Duration::from_secs(3)).await;
    assert_eq!(
        ids,
        vec![naked, away],
        "the resumed session should redeliver the nak'd message, then the queued one, and never the acked one"
    );
    settle().await;
    drop(again);

    // The session is named by consumer and subject, not stream, so the same
    // name on another stream resumes it. It must still hear the new stream.
    let other = declare(&streams, "durable-other", &[SUBJECT]).await;
    let mut moved = as_durable(other.as_ref())
        .unwrap()
        .consume(SUBJECT, &consumer, &opts)
        .await
        .unwrap();
    let id = other
        .publish(SUBJECT, event("moved", ""), &Options::default())
        .await
        .unwrap();
    assert_eq!(drain(&mut moved, Duration::from_secs(3)).await, vec![id]);
    settle().await;
    drop(moved);
    streams.close().await.unwrap();
}

// A group is a shared subscription: each message goes to one member.
#[tokio::test]
async fn groups_share_the_subject() {
    let _serial = SERIAL.lock().await;
    // As the password user, so every member, durable or not, is a connection
    // that has to log in, and a shared subscription the ACLs have to allow.
    let streams = connect_as(APP, &fresh_for(APP, "group")).await;
    let manager = declare(&streams, "group", &[SUBJECT]).await;
    let durable = as_durable(manager.as_ref()).unwrap();
    const TOTAL: usize = 8;

    // Durable members, each a session of its own under one group.
    let group = Options::default().with_group(fresh("workers"));
    let mut a = durable.consume(SUBJECT, "worker", &group).await.unwrap();
    let mut b = durable.consume(SUBJECT, "worker", &group).await.unwrap();
    let mut sent = HashSet::new();
    for i in 0..TOTAL {
        sent.insert(
            manager
                .publish(SUBJECT, event("u", &i.to_string()), &Options::default())
                .await
                .unwrap(),
        );
    }
    let mut seen = Vec::new();
    while seen.len() < TOTAL {
        tokio::select! {
            Some(d) = a.next() => { seen.push(d.id().to_owned()); d.ack().await.unwrap(); }
            Some(d) = b.next() => { seen.push(d.id().to_owned()); d.ack().await.unwrap(); }
            _ = tokio::time::sleep(WITHIN) => panic!("saw {} of {TOTAL} across the group", seen.len()),
        }
    }
    assert_eq!(seen.iter().cloned().collect::<HashSet<_>>(), sent);
    assert!(timeout(Duration::from_millis(500), a.next()).await.is_err());
    assert!(timeout(Duration::from_millis(500), b.next()).await.is_err());
    settle().await;
    drop((a, b));

    // Plain subscribers in a group split the subject the same way.
    let readers = Options::default().with_group(fresh("readers"));
    let mut a = manager.subscribe(SUBJECT, &readers).await.unwrap();
    let mut b = manager.subscribe(SUBJECT, &readers).await.unwrap();
    for i in 0..TOTAL {
        manager
            .publish(SUBJECT, event("u", &i.to_string()), &Options::default())
            .await
            .unwrap();
    }
    let mut count = 0;
    while count < TOTAL {
        tokio::select! {
            Some(_) = a.next() => count += 1,
            Some(_) = b.next() => count += 1,
            _ = tokio::time::sleep(WITHIN) => panic!("saw {count} of {TOTAL} across the readers"),
        }
    }
    assert!(timeout(Duration::from_millis(500), a.next()).await.is_err());
    assert!(timeout(Duration::from_millis(500), b.next()).await.is_err());
    drop((a, b));
    streams.close().await.unwrap();
}

/// The broker's own `$SYS` counters, read over a mutual-TLS connection of
/// their own.
struct SysWatch {
    latest: Arc<Mutex<HashMap<String, u64>>>,
    task: tokio::task::JoinHandle<()>,
}

const CONNECTED: &str = "$SYS/broker/clients/connected";
const SUBSCRIPTIONS: &str = "$SYS/broker/subscriptions/count";

impl SysWatch {
    async fn start() -> Self {
        let mut options = options(Some(tls(true, true)));
        options.set_client_id(format!("e2e-sys-{}", ulid::Ulid::new()));
        let (client, mut events) = AsyncClient::new(options, 16);
        client
            .subscribe(CONNECTED, rumqttc::v5::mqttbytes::QoS::AtMostOnce)
            .await
            .unwrap();
        client
            .subscribe(SUBSCRIPTIONS, rumqttc::v5::mqttbytes::QoS::AtMostOnce)
            .await
            .unwrap();
        let latest = Arc::new(Mutex::new(HashMap::new()));
        let sink = latest.clone();
        let task = tokio::spawn(async move {
            let _client = client;
            while let Ok(event) = events.poll().await {
                if let Event::Incoming(Incoming::Publish(p)) = event {
                    let topic = String::from_utf8_lossy(&p.topic).into_owned();
                    if let Ok(n) = String::from_utf8_lossy(&p.payload).trim().parse() {
                        sink.lock().unwrap().insert(topic, n);
                    }
                }
            }
        });
        Self { latest, task }
    }

    fn get(&self, topic: &str) -> Option<u64> {
        self.latest.lock().unwrap().get(topic).copied()
    }

    /// The steady value of a counter: the least seen over a few seconds, so a
    /// healthcheck client connecting for a moment is not mistaken for ours.
    async fn baseline(&self, topic: &str) -> u64 {
        let mut least = None;
        for _ in 0..30 {
            if let Some(n) = self.get(topic) {
                least = Some(least.map_or(n, |l: u64| l.min(n)));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        least.unwrap_or_else(|| panic!("the broker never published {topic}"))
    }

    async fn reaches(&self, topic: &str, want: u64) -> Result<(), String> {
        let mut last = None;
        for _ in 0..150 {
            last = self.get(topic);
            if last == Some(want) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(format!("{topic} stayed at {last:?}, expected {want}"))
    }
}

impl SysWatch {
    async fn falls_to(&self, topic: &str, most: u64) {
        let mut last = None;
        for _ in 0..150 {
            last = self.get(topic);
            if last.is_some_and(|n| n <= most) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("{topic} stayed at {last:?}, expected at most {most}");
    }
}

impl Drop for SysWatch {
    fn drop(&mut self) {
        self.task.abort();
    }
}

// Dropping a subscription unsubscribes and disconnects, so the broker holds
// nothing for it; dropping a durable consumer disconnects but, as promised,
// keeps its session's subscription. Deleting a stream leaves nothing either,
// because a declaration never put anything on the broker.
#[tokio::test]
async fn dropping_leaves_nothing_on_the_broker() {
    let _serial = SERIAL.lock().await;
    let streams = connect(&fresh("cleanup")).await;
    let watch = SysWatch::start().await;
    let manager = declare(&streams, "cleanup", &[SUBJECT]).await;

    // The counts are the whole broker's. Sessions earlier tests, or earlier
    // runs, left behind expire when they will, each taking a subscription
    // with it, so a measurement one of them lands in is taken again.
    let mut failures = Vec::new();
    for _ in 0..3 {
        match subscriptions_come_and_go(&watch, manager.as_ref()).await {
            Ok(()) => break,
            Err(failure) => failures.push(failure),
        }
    }
    assert!(failures.len() < 3, "every measurement failed: {failures:?}");

    drop(manager);
    streams.delete("cleanup").await.unwrap();
    assert!(matches!(
        streams.get("cleanup").await,
        Err(StreamError::NotFound(_))
    ));
    // Closing releases the publishing connection too. Measured from just
    // before, so a client of the broker's own coming or going in the
    // meantime is not counted against the provider.
    let open = watch.baseline(CONNECTED).await;
    streams.close().await.unwrap();
    watch.falls_to(CONNECTED, open - 1).await;
}

/// One measurement of what subscribing and dropping leave on the broker.
async fn subscriptions_come_and_go(watch: &SysWatch, manager: &dyn Manager) -> Result<(), String> {
    let clients = watch.baseline(CONNECTED).await;
    let subscriptions = watch.baseline(SUBSCRIPTIONS).await;

    let subscription = manager
        .subscribe(SUBJECT, &Options::default())
        .await
        .unwrap();
    watch.reaches(CONNECTED, clients + 1).await?;
    watch.reaches(SUBSCRIPTIONS, subscriptions + 1).await?;
    drop(subscription);
    watch.reaches(CONNECTED, clients).await?;
    watch.reaches(SUBSCRIPTIONS, subscriptions).await?;

    let durable = as_durable(manager).unwrap();
    let deliveries = durable
        .consume(SUBJECT, &fresh("kept"), &Options::default())
        .await
        .unwrap();
    watch.reaches(CONNECTED, clients + 1).await?;
    watch.reaches(SUBSCRIPTIONS, subscriptions + 1).await?;
    drop(deliveries);
    watch.reaches(CONNECTED, clients).await?;
    // The session, and its subscription, outlive the connection. Read as the
    // least over a few seconds, as the baseline was, so the healthcheck's own
    // passing subscription is not counted.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let kept = watch.baseline(SUBSCRIPTIONS).await;
    if kept != subscriptions + 1 {
        return Err(format!(
            "{SUBSCRIPTIONS} is {kept} after a durable consumer left, expected {}",
            subscriptions + 1
        ));
    }
    Ok(())
}

/// Restarts the broker container and waits for it to report healthy again.
fn restart_broker() {
    let name = container();
    let status = Command::new("docker")
        .args(["restart", &name])
        .status()
        .expect("run docker restart");
    assert!(status.success(), "docker restart {name} failed");
    for _ in 0..120 {
        let out = Command::new("docker")
            .args(["inspect", "--format", "{{.State.Health.Status}}", &name])
            .output()
            .expect("run docker inspect");
        if String::from_utf8_lossy(&out.stdout).trim() == "healthy" {
            return;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    panic!("{name} did not become healthy after a restart");
}

// A persistent session's queued and unacknowledged QoS 1 messages are on disk:
// the broker restarts, the consumer comes back, and gets exactly what it had
// not acknowledged.
#[tokio::test]
async fn a_session_survives_a_broker_restart() {
    if std::env::var("STREAMS_E2E_RESTART").as_deref() != Ok("1") {
        eprintln!(
            "skipping: set STREAMS_E2E_RESTART=1 to restart {}",
            container()
        );
        return;
    }
    let _serial = SERIAL.lock().await;
    // As the password user: the durable session logs in, and so does the
    // publisher when it redials the restarted broker.
    let prefix = fresh_for(APP, "restart");
    let consumer = fresh("ledger");
    let streams = connect_as(APP, &prefix).await;
    let manager = declare(&streams, "ledger", &[SUBJECT]).await;
    let durable = as_durable(manager.as_ref()).unwrap();

    let mut deliveries = durable
        .consume(SUBJECT, &consumer, &Options::default())
        .await
        .unwrap();
    let acked = manager
        .publish(SUBJECT, event("acked", ""), &Options::default())
        .await
        .unwrap();
    let unacked = manager
        .publish(SUBJECT, event("unacked", ""), &Options::default())
        .await
        .unwrap();
    let first = next_delivery(&mut deliveries).await;
    assert_eq!(first.id(), acked);
    first.ack().await.unwrap();
    let second = next_delivery(&mut deliveries).await;
    assert_eq!(second.id(), unacked);
    drop(second); // Neither acked nor nak'd: still in flight on the session.
    settle().await;
    drop(deliveries);
    settle().await;

    let queued = manager
        .publish(SUBJECT, event("queued", ""), &Options::default())
        .await
        .unwrap();
    // Past the autosave interval, though shutdown saves too.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    restart_broker();

    // The publishing connection redials on its own, over the same TLS.
    let mut after = None;
    for _ in 0..30 {
        match manager
            .publish(SUBJECT, event("after", ""), &Options::default())
            .await
        {
            Ok(id) => {
                after = Some(id);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
    let after = after.expect("the publisher never came back after the restart");
    streams.close().await.unwrap();

    // A new process: a new provider, the same declaration, the same name.
    let streams = connect_as(APP, &prefix).await;
    let manager = declare(&streams, "ledger", &[SUBJECT]).await;
    let durable = as_durable(manager.as_ref()).unwrap();
    let mut resumed = durable
        .consume(SUBJECT, &consumer, &Options::default())
        .await
        .unwrap();
    let ids = drain(&mut resumed, Duration::from_secs(3)).await;
    assert!(
        !ids.contains(&acked),
        "the acked message came back: {ids:?}"
    );
    assert_eq!(
        ids,
        vec![unacked, queued, after],
        "the restarted broker should hand back the in-flight, the queued, and the post-restart message, in order"
    );
    settle().await;
    drop(resumed);
    streams.close().await.unwrap();
}
