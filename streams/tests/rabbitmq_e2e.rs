//! The RabbitMQ provider end to end over mutual TLS, against a broker that
//! keeps its data across a restart. The client certificate is also the login:
//! the broker offers only SASL EXTERNAL and maps the certificate's CN,
//! `tpp-streams-e2e`, to the one user it has.
//!
//! ```text
//! docker compose -p tpp-streams-e2e-rabbitmq -f streams/docker/compose.rabbitmq.secure.yaml up -d --wait
//! STREAMS_E2E_RESTART=1 cargo test -p tpp-streams --features rabbitmq,tls,live-tests --test rabbitmq_e2e
//! ```
//!
//! `RABBITMQ_E2E_ADDR` (default `127.0.0.1:15671`) and `STREAMS_E2E_TLS_DIR`
//! (default `streams/docker/tls/out`) point it elsewhere;
//! `RABBITMQ_E2E_CONTAINER` names the container inspected for leftovers and
//! restarted. The restart test runs only with `STREAMS_E2E_RESTART=1`, so it
//! never bounces a broker someone else is using by accident.

use std::{
    collections::BTreeSet,
    path::PathBuf,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures::StreamExt;
use lapin::{
    Connection, ConnectionProperties, DefaultConnectionBuilder,
    options::{BasicGetOptions, QueueDeleteOptions},
    tcp::{OwnedIdentity, OwnedTLSConfig},
};
use serde::{Deserialize, Serialize};
use streams::{
    Manager, Options, Payload, Stream, StreamError, Streams, as_batch, as_durable, as_positioned,
    drivers::rabbitmq::{RabbitConfig, RabbitProvider},
    typed,
};
use tokio::sync::RwLock;

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
struct Event {
    user: String,
    action: String,
}

const SUBJECT: &str = "user.created";
const WAIT: Duration = Duration::from_secs(10);
/// How long silence must last before a test calls a message absent.
const QUIET: Duration = Duration::from_millis(700);

/// Every test holds it shared; the restart test holds it alone, so no other
/// test's connection dies under it.
static BROKER: RwLock<()> = RwLock::const_new(());

/// The AMQPS listener, `RABBITMQ_E2E_ADDR` or the compose default.
fn addr() -> String {
    std::env::var("RABBITMQ_E2E_ADDR").unwrap_or_else(|_| "127.0.0.1:15671".into())
}

/// The secure broker's container name, for `rabbitmqctl` and the restart test.
fn container() -> String {
    std::env::var("RABBITMQ_E2E_CONTAINER").unwrap_or_else(|_| "tpp-streams-rabbitmq-secure".into())
}

/// The test certificate directory, `STREAMS_E2E_TLS_DIR` or docker/tls/out.
fn tls_dir() -> PathBuf {
    std::env::var("STREAMS_E2E_TLS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("docker/tls/out"))
}

/// A test certificate file's bytes, failing with how to generate it when missing.
fn read(name: &str) -> Vec<u8> {
    let path = tls_dir().join(name);
    std::fs::read(&path).unwrap_or_else(|e| {
        panic!(
            "cannot read {}: {e}; run streams/docker/tls/generate.sh",
            path.display()
        )
    })
}

/// The user the broker derives from the client certificate's CN.
const CERT_USER: &str = "tpp-streams-e2e";

/// AMQPS with SASL EXTERNAL: no username or password, the certificate logs in.
fn amqps() -> String {
    format!("amqps://{}/%2f?auth_mechanism=external", addr())
}

/// AMQPS with SASL PLAIN as guest/guest, the broker's usual default login.
fn amqps_guest() -> String {
    format!("amqps://guest:guest@{}/%2f", addr())
}

/// Trusts the test CA and presents the client certificate.
fn mutual_tls() -> OwnedTLSConfig {
    OwnedTLSConfig {
        identity: Some(OwnedIdentity::PKCS8 {
            pem: read("client.crt"),
            key: read("client.key"),
        }),
        cert_chain: Some(String::from_utf8(read("ca.crt")).unwrap()),
    }
}

/// Connects, or says why not; a handshake that hangs counts as refused.
async fn connect_with(uri: String, tls: OwnedTLSConfig) -> Result<Connection, String> {
    connect_named(uri, tls, ConnectionProperties::default()).await
}

/// [`connect_with`] under `properties`, e.g. to name the connection on the broker.
async fn connect_named(
    uri: String,
    tls: OwnedTLSConfig,
    properties: ConnectionProperties,
) -> Result<Connection, String> {
    let builder = DefaultConnectionBuilder::new()
        .map_err(|e| e.to_string())?
        .with_uri_str(uri)
        .with_properties(properties)
        .with_tls_config(tls);
    match tokio::time::timeout(WAIT, builder.connect()).await {
        Ok(result) => result.map_err(|e| e.to_string()),
        Err(_) => Err("timed out connecting".into()),
    }
}

/// A mutual-TLS connection logged in by certificate, failing with how to start the broker.
async fn connect() -> Arc<Connection> {
    let conn = connect_with(amqps(), mutual_tls())
        .await
        .unwrap_or_else(|e| {
            panic!(
                "no RabbitMQ over mTLS at {}: {e}; see docker/compose.rabbitmq.secure.yaml",
                addr()
            )
        });
    Arc::new(conn)
}

/// A prefix unique to this process and call, so tests on one broker never share queues.
fn prefix() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("e2e{nanos}-{}", SEQ.fetch_add(1, Ordering::Relaxed))
}

/// A JSON `Event` payload; the user tells messages apart.
fn event(user: &str, action: &str) -> Payload {
    Payload::json(&Event {
        user: user.into(),
        action: action.into(),
    })
    .unwrap()
}

/// A provider on `conn` naming everything under `prefix`.
async fn provider(conn: &Arc<Connection>, prefix: &str) -> RabbitProvider {
    RabbitProvider::new(conn.clone(), RabbitConfig::default().with_prefix(prefix))
        .await
        .unwrap()
}

/// A stream declaration with a fixed `id` over `subjects`.
fn stream(id: &str, subjects: &[&str]) -> Stream {
    Stream {
        id: id.into(),
        name: "e2e".into(),
        subjects: subjects.iter().map(|s| s.to_string()).collect(),
        ..Default::default()
    }
}

/// The next item within [`WAIT`], panicking on a timeout or an ended stream.
async fn next<S: futures::Stream + Unpin>(stream: &mut S) -> S::Item {
    tokio::time::timeout(WAIT, stream.next())
        .await
        .expect("nothing was delivered in time")
        .expect("the stream ended before a message arrived")
}

/// Asserts nothing arrives within [`QUIET`]; an ended stream fails too.
async fn quiet<S: futures::Stream + Unpin>(stream: &mut S)
where
    S::Item: std::fmt::Debug,
{
    if let Ok(item) = tokio::time::timeout(QUIET, stream.next()).await {
        panic!("expected nothing more, got {item:?}");
    }
}

/// `rabbitmqctl -q <args>` inside the broker container, one line per row.
fn rabbitmqctl(args: &[&str]) -> Vec<String> {
    let out = Command::new("docker")
        .args(["exec", &container(), "rabbitmqctl", "-q"])
        .args(args)
        .output()
        .expect("docker is not on PATH");
    assert!(
        out.status.success(),
        "rabbitmqctl {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout)
        .unwrap()
        .lines()
        .map(str::trim)
        .map(str::to_owned)
        .collect()
}

/// Names of the broker's exchanges or queues starting with `prefix`.
fn on_broker(what: &str, prefix: &str) -> Vec<String> {
    rabbitmqctl(&[what, "name"])
        .into_iter()
        .filter(|name| name.starts_with(prefix))
        .collect()
}

/// Fails with `accepted` unless the connection was refused, and says how.
fn refused(attempt: Result<Connection, String>, accepted: &str) {
    match attempt {
        Ok(_) => panic!("{accepted}"),
        Err(e) => eprintln!("refused as expected: {e}"),
    }
}

#[tokio::test]
async fn plaintext_is_refused() {
    let _shared = BROKER.read().await;
    let plain = connect_with(
        format!("amqp://guest:guest@{}/%2f", addr()),
        OwnedTLSConfig::default(),
    )
    .await;
    refused(plain, "a plaintext AMQP connection was accepted");
}

#[tokio::test]
async fn plain_guest_is_refused() {
    let _shared = BROKER.read().await;
    // A trusted certificate on the wire, but asking to log in as guest/guest
    // with PLAIN: the broker offers only EXTERNAL, and has no guest.
    let guest = connect_with(amqps_guest(), mutual_tls()).await;
    refused(guest, "a PLAIN guest/guest login was accepted");
    let users: Vec<_> = rabbitmqctl(&["list_users", "--no-table-headers"])
        .into_iter()
        .filter_map(|row| row.split_whitespace().next().map(str::to_owned))
        .collect();
    assert_eq!(users, vec![CERT_USER.to_owned()], "the broker's users");
}

#[tokio::test]
async fn the_certificate_is_the_login() {
    let _shared = BROKER.read().await;
    let name = prefix();
    let conn = connect_named(
        amqps(),
        mutual_tls(),
        ConnectionProperties::default().with_connection_name(name.clone().into()),
    )
    .await
    .unwrap_or_else(|e| panic!("EXTERNAL login refused: {e}"));
    // The broker lists the connection under the user named by the CN.
    let mut user = None;
    for _ in 0..20 {
        // client_properties carries the connection name chosen above.
        user = rabbitmqctl(&[
            "list_connections",
            "--no-table-headers",
            "user",
            "auth_mechanism",
            "peer_cert_subject",
            "client_properties",
        ])
        .into_iter()
        .find(|row| row.contains(&name))
        .map(|row| {
            let cols: Vec<_> = row.split('\t').map(str::trim).collect();
            (cols[0].to_owned(), cols[1].to_owned(), cols[2].to_owned())
        });
        if user.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        user,
        Some((
            CERT_USER.to_owned(),
            "EXTERNAL".to_owned(),
            format!("CN={CERT_USER},O=tpp-streams e2e")
        )),
        "the broker's view of connection {name}"
    );
    conn.close(200, "OK".into()).await.unwrap();
}

#[tokio::test]
async fn external_without_a_client_certificate_is_refused() {
    let _shared = BROKER.read().await;
    // EXTERNAL has nothing to log in with; the TLS handshake already fails,
    // since the broker requires a client certificate.
    let anonymous = connect_with(
        amqps(),
        OwnedTLSConfig {
            identity: None,
            cert_chain: Some(String::from_utf8(read("ca.crt")).unwrap()),
        },
    )
    .await;
    refused(
        anonymous,
        "a TLS connection without a client certificate was accepted",
    );
}

#[tokio::test]
async fn an_untrusted_server_is_refused() {
    let _shared = BROKER.read().await;
    // The client certificate but not the CA: the broker's certificate cannot be
    // verified, so the client must refuse it.
    let unverified = connect_with(
        amqps(),
        OwnedTLSConfig {
            identity: mutual_tls().identity,
            cert_chain: None,
        },
    )
    .await;
    refused(unverified, "an unverifiable server was trusted");
}

#[tokio::test]
async fn lifecycle_over_mutual_tls() {
    let _shared = BROKER.read().await;
    let conn = connect().await;
    let prefix = prefix();
    let p = provider(&conn, &prefix).await;

    // Create with an explicit id and with a generated one.
    let explicit = p.create(stream("orders", &[SUBJECT])).await.unwrap();
    assert_eq!(explicit.id, "orders");
    assert!(explicit.active);
    let generated = p.create(stream("", &[SUBJECT])).await.unwrap();
    assert!(!generated.id.is_empty());
    assert_ne!(generated.id, explicit.id);
    let exchanges: BTreeSet<_> = on_broker("list_exchanges", &prefix).into_iter().collect();
    assert_eq!(
        exchanges,
        BTreeSet::from([
            format!("{prefix}.orders"),
            format!("{prefix}.{}", generated.id)
        ])
    );

    // Get, list, update.
    assert_eq!(p.get("orders").await.unwrap(), explicit);
    let ids: Vec<_> = p.list().await.unwrap().into_iter().map(|s| s.id).collect();
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&explicit.id) && ids.contains(&generated.id));
    let updated = p
        .update(
            "orders",
            Stream {
                description: "after".into(),
                ..stream("ignored", &[SUBJECT, "user.deleted"])
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.id, "orders");
    assert_eq!(p.get("orders").await.unwrap().description, "after");
    assert!(matches!(
        p.update("missing", stream("", &[SUBJECT])).await,
        Err(StreamError::NotFound(_))
    ));

    // Bind, publish, subscribe, under a chosen and a generated id.
    let m = p.bind("orders").await.unwrap();
    let mut sub = m.subscribe(SUBJECT, &Options::default()).await.unwrap();
    let chosen = m
        .publish(
            SUBJECT,
            event("ada", "created"),
            &Options::default().with_id("order-1"),
        )
        .await
        .unwrap();
    assert_eq!(chosen, "order-1");
    let got = next(&mut sub).await;
    assert_eq!(got.id(), "order-1");
    assert_eq!(got.subject(), SUBJECT);
    assert_eq!(got.json::<Event>().unwrap().user, "ada");
    // A partition key is ignored: RabbitMQ has no partitions to choose from.
    let any = m
        .publish(
            SUBJECT,
            event("grace", ""),
            &Options::default().with_partition_key("k"),
        )
        .await
        .unwrap();
    assert_eq!(next(&mut sub).await.id(), any);

    // The typed view over the same manager.
    let typed = typed::<Event>(m.clone());
    let mut events = typed.subscribe(SUBJECT, &Options::default()).await.unwrap();
    let ev = Event {
        user: "linus".into(),
        action: "typed".into(),
    };
    typed
        .publish(SUBJECT, &ev, &Options::default())
        .await
        .unwrap();
    assert_eq!(next(&mut events).await, ev);
    drop(events);

    // Batch, pipelined and confirmed, in order.
    let batch = as_batch(m.as_ref()).unwrap();
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
        // `sub` still sees everything, including the typed publish.
        let mut got = next(&mut sub).await;
        while got.json::<Event>().unwrap().action == "typed" {
            got = next(&mut sub).await;
        }
        assert_eq!(got.id(), id);
    }
    drop(sub);

    // Refusals: undeclared subjects, wildcards as a publish target, options
    // that change the meaning of a call, and the positioned capability.
    assert!(matches!(
        m.publish("typo", event("", ""), &Options::default()).await,
        Err(StreamError::UnknownSubject { .. })
    ));
    assert!(matches!(
        m.subscribe("typo", &Options::default()).await,
        Err(StreamError::UnknownSubject { .. })
    ));
    assert!(matches!(
        batch
            .publish_batch("typo", vec![event("", "")], &Options::default())
            .await,
        Err(StreamError::UnknownSubject { .. })
    ));
    assert!(matches!(
        batch
            .publish_batch(
                SUBJECT,
                vec![event("", "")],
                &Options::default().with_id("x")
            )
            .await,
        Err(StreamError::Unsupported(_))
    ));
    assert!(matches!(
        m.publish(
            SUBJECT,
            event("", ""),
            &Options::default().with_ttl(Duration::from_secs(1))
        )
        .await,
        Err(StreamError::Unsupported(_))
    ));
    assert!(matches!(
        m.subscribe(SUBJECT, &Options::default().with_group("g"))
            .await,
        Err(StreamError::Unsupported(_))
    ));
    let durable = as_durable(m.as_ref()).unwrap();
    assert!(matches!(
        durable
            .consume(SUBJECT, "billing", &Options::default().with_group("g"))
            .await,
        Err(StreamError::Unsupported(_))
    ));
    assert!(matches!(
        durable.consume(SUBJECT, "", &Options::default()).await,
        Err(StreamError::Unsupported(_))
    ));
    assert!(matches!(
        as_positioned(m.as_ref()),
        Err(StreamError::Unsupported(_))
    ));

    // Dropped subscriptions left no queues behind; only now declare durable ones.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        on_broker("list_queues", &prefix),
        Vec::<String>::new(),
        "a dropped subscription left its queue"
    );

    durable_consumers(&conn, m.as_ref(), &format!("{prefix}.orders")).await;

    // Delete both streams; NotFound afterwards, and their exchanges are gone.
    p.delete("orders").await.unwrap();
    p.delete(&generated.id).await.unwrap();
    for id in ["orders", generated.id.as_str()] {
        assert!(matches!(p.get(id).await, Err(StreamError::NotFound(_))));
        assert!(matches!(p.bind(id).await, Err(StreamError::NotFound(_))));
        assert!(matches!(p.delete(id).await, Err(StreamError::NotFound(_))));
    }
    assert!(p.list().await.unwrap().is_empty());
    assert_eq!(on_broker("list_exchanges", &prefix), Vec::<String>::new());

    // A durable consumer's queue outlives its stream by design: it may hold
    // work nobody has seen. The test owns these, so it removes them.
    let queues = on_broker("list_queues", &prefix);
    assert_eq!(
        queues,
        vec![format!("{prefix}.orders.billing.user-created")],
        "only the durable consumer's queue should remain"
    );
    let ch = conn.create_channel().await.unwrap();
    for q in queues {
        ch.queue_delete(q.as_str().into(), QueueDeleteOptions::default())
            .await
            .unwrap();
    }
    assert_eq!(on_broker("list_queues", &prefix), Vec::<String>::new());

    // A publish on the deleted stream fails, and the broker closing the
    // publishing channel for it does not take down the other streams.
    let other = p.create(stream("other", &[SUBJECT])).await.unwrap();
    let other_m = p.bind(&other.id).await.unwrap();
    assert!(
        m.publish(SUBJECT, event("", ""), &Options::default())
            .await
            .is_err()
    );
    let mut other_sub = other_m
        .subscribe(SUBJECT, &Options::default())
        .await
        .unwrap();
    let id = other_m
        .publish(SUBJECT, event("after", ""), &Options::default())
        .await
        .unwrap();
    assert_eq!(next(&mut other_sub).await.id(), id);
    drop(other_sub);
    p.delete(&other.id).await.unwrap();
    assert_eq!(on_broker("list_exchanges", &prefix), Vec::<String>::new());

    p.close().await.unwrap();
    assert!(
        conn.status().connected(),
        "close took the caller's connection"
    );
}

/// Ack, nak with an attempt count, prefetch, persistence, and one name
/// splitting work.
async fn durable_consumers(conn: &Arc<Connection>, m: &dyn Manager, exchange: &str) {
    let durable = as_durable(m).unwrap();

    // Prefetch 1: the second message waits until the first is settled.
    let mut billing = durable
        .consume(SUBJECT, "billing", &Options::default().with_prefetch(1))
        .await
        .unwrap();
    let first = m
        .publish(SUBJECT, event("one", ""), &Options::default())
        .await
        .unwrap();
    let second = m
        .publish(SUBJECT, event("two", ""), &Options::default())
        .await
        .unwrap();
    let d = next(&mut billing).await;
    assert_eq!((d.id(), d.attempt()), (first.as_str(), 1));
    quiet(&mut billing).await;

    // Nak hands it straight back, counted as a second attempt.
    d.nak().await.unwrap();
    let again = next(&mut billing).await;
    assert_eq!(again.id(), first);
    assert!(again.attempt() >= 2, "attempt {}", again.attempt());
    again.ack().await.unwrap();
    let d = next(&mut billing).await;
    assert_eq!((d.id(), d.attempt()), (second.as_str(), 1));
    d.ack().await.unwrap();
    quiet(&mut billing).await;
    drop(billing);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // What reaches the queue is persistent, in runtime-go's properties.
    let id = m
        .publish(SUBJECT, event("raw", ""), &Options::default())
        .await
        .unwrap();
    let ch = conn.create_channel().await.unwrap();
    let queue = format!("{exchange}.billing.user-created");
    let raw = ch
        .basic_get(queue.as_str().into(), BasicGetOptions { no_ack: true })
        .await
        .unwrap()
        .expect("the durable queue did not hold the message");
    assert_eq!(*raw.properties.delivery_mode(), Some(2));
    assert_eq!(
        raw.properties.message_id().as_ref().map(|s| s.as_str()),
        Some(id.as_str())
    );
    ch.close(200, "OK".into()).await.unwrap();

    // Two consumers under one name split the work.
    let a = durable
        .consume(SUBJECT, "billing", &Options::default())
        .await
        .unwrap();
    let b = durable
        .consume(SUBJECT, "billing", &Options::default())
        .await
        .unwrap();
    for i in 0..6 {
        m.publish(SUBJECT, event("w", &i.to_string()), &Options::default())
            .await
            .unwrap();
    }
    let mut both = futures::stream::select(a, b);
    let mut seen = BTreeSet::new();
    while seen.len() < 6 {
        let d = next(&mut both).await;
        assert!(seen.insert(d.id().to_owned()), "delivered twice");
        d.ack().await.unwrap();
    }
    quiet(&mut both).await;
}

/// An acked publish survives a broker restart; an unacked delivery comes back;
/// an acked one does not.
#[tokio::test]
async fn durable_across_a_broker_restart() {
    if std::env::var("STREAMS_E2E_RESTART").as_deref() != Ok("1") {
        eprintln!(
            "skipped: set STREAMS_E2E_RESTART=1 to restart {}",
            container()
        );
        return;
    }
    let _alone = BROKER.write().await;
    let prefix = prefix();

    let conn = connect().await;
    let p = provider(&conn, &prefix).await;
    p.create(ledger()).await.unwrap();
    let m = p.bind("ledger").await.unwrap();
    let durable = as_durable(m.as_ref()).unwrap();
    let mut consumer = durable
        .consume(SUBJECT, "billing", &Options::default().with_prefetch(2))
        .await
        .unwrap();
    // Each publish returns only once the broker has confirmed it.
    let acked = m
        .publish(SUBJECT, event("acked", ""), &Options::default())
        .await
        .unwrap();
    // Headers travel with the message, so a restart keeps them in the queue.
    let unacked = m
        .publish(
            SUBJECT,
            event("unacked", ""),
            &Options::default().header("traceparent", "00-unacked"),
        )
        .await
        .unwrap();
    let waiting = m
        .publish(
            SUBJECT,
            event("waiting", ""),
            &Options::default()
                .header("site", "plant-4")
                .with_expiry(Duration::from_secs(600)),
        )
        .await
        .unwrap();
    let d = next(&mut consumer).await;
    assert_eq!(d.id(), acked);
    d.ack().await.unwrap();
    let held = next(&mut consumer).await;
    assert_eq!(held.id(), unacked);
    // Let the ack reach the broker before pulling the plug.
    tokio::time::sleep(Duration::from_millis(500)).await;

    restart();

    let conn = connect().await;
    let p = provider(&conn, &prefix).await;
    // The exchange is durable; declaring the stream again, with its default
    // headers, is how a new process shares it.
    p.create(ledger()).await.unwrap();
    let m = p.bind("ledger").await.unwrap();
    let durable = as_durable(m.as_ref()).unwrap();
    let mut consumer = durable
        .consume(SUBJECT, "billing", &Options::default())
        .await
        .unwrap();
    let mut back = Vec::new();
    for _ in 0..2 {
        let d = next(&mut consumer).await;
        let want = if d.id() == unacked {
            [
                ("lang", "rust"),
                ("site", "plant-3"),
                ("traceparent", "00-unacked"),
            ]
            .as_slice()
        } else {
            [("lang", "rust"), ("site", "plant-4")].as_slice()
        };
        assert_eq!(d.headers(), &want.iter().copied().collect(), "{}", d.id());
        back.push((d.id().to_owned(), d.attempt()));
        d.ack().await.unwrap();
    }
    back.sort();
    let mut expected = vec![unacked.clone(), waiting.clone()];
    expected.sort();
    assert_eq!(
        back.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>(),
        expected
    );
    let redelivered = back.iter().find(|(id, _)| *id == unacked).unwrap().1;
    assert!(redelivered >= 2, "attempt {redelivered}");
    quiet(&mut consumer).await;

    // The exchange and binding survived too: a fresh publish routes.
    let fresh = m
        .publish(SUBJECT, event("fresh", ""), &Options::default())
        .await
        .unwrap();
    let d = next(&mut consumer).await;
    assert_eq!(d.id(), fresh);
    assert_eq!(d.headers().get("lang"), Some("rust"));
    d.ack().await.unwrap();
    drop(consumer);
    drop(held);

    p.delete("ledger").await.unwrap();
    let ch = conn.create_channel().await.unwrap();
    for q in on_broker("list_queues", &prefix) {
        ch.queue_delete(q.as_str().into(), QueueDeleteOptions::default())
            .await
            .unwrap();
    }
    assert_eq!(on_broker("list_queues", &prefix), Vec::<String>::new());
    assert_eq!(on_broker("list_exchanges", &prefix), Vec::<String>::new());
}

/// The restart test's stream, with default headers a new process declares
/// again, since stream metadata lives in the process.
fn ledger() -> Stream {
    stream("ledger", &[SUBJECT]).headers([("lang", "rust"), ("site", "plant-3")])
}

/// Headers, expiry, retain and delayed nak over mutual TLS: defaults and
/// overrides arrive on single publishes and batches, another client sees them
/// as an AMQP table of long strings with the `expiration` property, they
/// survive a nak, bad keys are refused at create and publish, an expired
/// message is not delivered, and retain and a delayed nak are refused by name.
#[tokio::test]
async fn headers_and_expiry_over_mutual_tls() {
    use lapin::types::AMQPValue;

    let _shared = BROKER.read().await;
    let conn = connect().await;
    let prefix = prefix();
    let p = provider(&conn, &prefix).await;

    for key in ["x-death", "CC", "has space"] {
        assert!(
            matches!(
                p.create(stream("bad", &[SUBJECT]).header(key, "v")).await,
                Err(StreamError::InvalidHeader(_))
            ),
            "create accepted {key:?}"
        );
    }
    p.create(stream("h", &[SUBJECT]).header("lang", "rust"))
        .await
        .unwrap();
    let m = p.bind("h").await.unwrap();
    let durable = as_durable(m.as_ref()).unwrap();
    let queue = format!("{prefix}.h.audit.user-created");

    // Declare the queue, then publish while nobody consumes.
    drop(
        durable
            .consume(SUBJECT, "audit", &Options::default())
            .await
            .unwrap(),
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    let id = m
        .publish(
            SUBJECT,
            event("ada", ""),
            &Options::default()
                .header("traceparent", "00-ab")
                .with_expiry(Duration::from_secs(60)),
        )
        .await
        .unwrap();

    // Another client reads the queue directly and sees AMQP properties.
    let ch = conn.create_channel().await.unwrap();
    let raw = ch
        .basic_get(queue.as_str().into(), BasicGetOptions { no_ack: false })
        .await
        .unwrap()
        .expect("the durable queue did not hold the message");
    let table = raw.properties.headers().clone().unwrap();
    assert_eq!(
        table.inner().get("lang"),
        Some(&AMQPValue::LongString("rust".into()))
    );
    assert_eq!(
        table.inner().get("traceparent"),
        Some(&AMQPValue::LongString("00-ab".into()))
    );
    assert_eq!(
        raw.properties.expiration().as_ref().map(|e| e.as_str()),
        Some("60000")
    );
    assert_eq!(raw.data, streams::provider::pack(&id, &event("ada", "")));
    ch.close(200, "OK".into()).await.unwrap();

    // The unsettled get went back; the consumer gets it with its headers, and
    // again after a nak.
    let mut audit = durable
        .consume(SUBJECT, "audit", &Options::default())
        .await
        .unwrap();
    let d = next(&mut audit).await;
    assert_eq!(d.id(), id);
    assert_eq!(d.headers().get("traceparent"), Some("00-ab"));
    assert!(!d.can_delay_redelivery());
    d.nak().await.unwrap();
    let d = next(&mut audit).await;
    assert_eq!(d.headers().get("lang"), Some("rust"));
    assert_eq!(d.headers().get("traceparent"), Some("00-ab"));
    // A refused delayed nak requeues at once rather than stranding it.
    assert!(matches!(
        d.nak_after(Duration::from_secs(30)).await,
        Err(StreamError::Unsupported(_))
    ));
    let d = next(&mut audit).await;
    assert_eq!(d.id(), id);
    d.ack().await.unwrap();

    // Batches carry them too.
    let ids = as_batch(m.as_ref())
        .unwrap()
        .publish_batch(
            SUBJECT,
            vec![event("a", ""), event("b", "")],
            &Options::default().header("lang", "go"),
        )
        .await
        .unwrap();
    for id in &ids {
        let d = next(&mut audit).await;
        assert_eq!(d.id(), id);
        assert_eq!(d.headers().get("lang"), Some("go"));
        d.ack().await.unwrap();
    }
    drop(audit);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // An expired message is dropped by the broker.
    m.publish(
        SUBJECT,
        event("stale", ""),
        &Options::default().with_expiry(Duration::from_millis(200)),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    let mut audit = durable
        .consume(SUBJECT, "audit", &Options::default())
        .await
        .unwrap();
    quiet(&mut audit).await;
    drop(audit);

    // Refusals at publish.
    for opts in [
        Options::default().header("x-delay", "1000"),
        Options::default().header("BCC", "user.deleted"),
        Options::default().header("lang", "two\nlines"),
    ] {
        assert!(matches!(
            m.publish(SUBJECT, event("", ""), &opts).await,
            Err(StreamError::InvalidHeader(_))
        ));
    }
    assert!(matches!(
        m.publish(SUBJECT, event("", ""), &Options::default().with_retain())
            .await,
        Err(StreamError::Unsupported(_))
    ));

    p.delete("h").await.unwrap();
    let ch = conn.create_channel().await.unwrap();
    for q in on_broker("list_queues", &prefix) {
        ch.queue_delete(q.as_str().into(), QueueDeleteOptions::default())
            .await
            .unwrap();
    }
    p.close().await.unwrap();
}

/// Restarts the broker container and waits until it reports healthy again.
fn restart() {
    let name = container();
    let status = Command::new("docker")
        .args(["restart", &name])
        .status()
        .expect("docker is not on PATH");
    assert!(status.success(), "docker restart {name} failed");
    for _ in 0..120 {
        let out = Command::new("docker")
            .args(["inspect", "-f", "{{.State.Health.Status}}", &name])
            .output()
            .unwrap();
        if String::from_utf8_lossy(&out.stdout).trim() == "healthy" {
            return;
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    panic!("{name} did not become healthy after a restart");
}
