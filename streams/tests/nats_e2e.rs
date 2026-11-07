//! Both NATS providers end to end over mutual TLS, against a server that keeps
//! JetStream on disk.
//!
//! Start it with
//! `docker compose -p tpp-streams-e2e-nats -f streams/docker/compose.nats.secure.yaml up -d --wait`.
//! `NATS_E2E_URL` overrides the default `tls://127.0.0.1:14223` and
//! `NATS_E2E_CERTS` the certificate directory (default `docker/tls/out`).
//!
//! The broker-restart test runs only with `STREAMS_E2E_RESTART=1`, because it
//! restarts the `tpp-streams-nats-secure` container. The other tests hold a
//! shared gate it takes exclusively, so a restart never lands in the middle
//! of one of them.

use std::{
    path::PathBuf,
    process::Command,
    time::{Duration, Instant},
};

use async_nats::{ConnectOptions, jetstream::stream::StorageType};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use streams::{
    Deliveries, Options, Payload, Position, Stream, StreamError, Streams, as_batch, as_durable,
    as_positioned,
    drivers::nats::{JetStreamProvider, NatsConfig, NatsProvider, safe_name},
    provider, typed,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::RwLock,
};

const CONTAINER: &str = "tpp-streams-nats-secure";
const WITHIN: Duration = Duration::from_secs(5);

/// Readers are ordinary tests; the restart test is the one writer.
static GATE: RwLock<()> = RwLock::const_new(());

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
struct Event {
    user: String,
    action: String,
}

fn event(user: &str) -> Payload {
    Payload::json(&Event {
        user: user.into(),
        action: "created".into(),
    })
    .unwrap()
}

fn url() -> String {
    std::env::var("NATS_E2E_URL").unwrap_or_else(|_| "tls://127.0.0.1:14223".into())
}

/// `host:port` from the URL, for the raw plaintext probe.
fn host_port() -> String {
    let url = url();
    url.split_once("://")
        .map_or(url.as_str(), |(_, rest)| rest)
        .to_owned()
}

fn certs() -> PathBuf {
    std::env::var("NATS_E2E_CERTS").map_or_else(
        |_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("docker/tls/out"),
        PathBuf::from,
    )
}

/// TLS that trusts the test CA, without a client certificate.
fn trusting() -> ConnectOptions {
    ConnectOptions::new()
        .add_root_certificates(certs().join("ca.crt"))
        .require_tls(true)
        .connection_timeout(WITHIN)
}

/// Mutual TLS: the test CA plus the client certificate the server requires.
async fn client() -> async_nats::Client {
    let dir = certs();
    trusting()
        .add_client_certificate(dir.join("client.crt"), dir.join("client.key"))
        .connect(url())
        .await
        .unwrap_or_else(|error| {
            panic!(
                "no NATS over mTLS at {} ({error}); see docker/compose.nats.secure.yaml",
                url()
            )
        })
}

async fn jetstream(nc: async_nats::Client) -> JetStreamProvider {
    JetStreamProvider::new(nc, NatsConfig::default())
        .await
        .expect("the server has JetStream enabled")
}

fn unique(prefix: &str) -> String {
    format!("{prefix}.{}", provider::new_id())
}

fn subjects(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| (*s).to_owned()).collect()
}

async fn next<S: futures::Stream + Unpin>(stream: &mut S) -> S::Item {
    next_within(stream, WITHIN).await
}

async fn next_within<S: futures::Stream + Unpin>(stream: &mut S, within: Duration) -> S::Item {
    tokio::time::timeout(within, stream.next())
        .await
        .expect("nothing arrived in time")
        .expect("the stream ended")
}

async fn nothing<S: futures::Stream + Unpin>(stream: &mut S, within: Duration) {
    if let Ok(Some(_)) = tokio::time::timeout(within, stream.next()).await {
        panic!("something arrived that should not have");
    }
}

fn is<T: std::fmt::Debug>(result: &streams::Result<T>, want: fn(&StreamError) -> bool) {
    match result {
        Err(error) if want(error) => {}
        other => panic!("unexpected {other:?}"),
    }
}

fn not_found(e: &StreamError) -> bool {
    matches!(e, StreamError::NotFound(_))
}

fn unsupported(e: &StreamError) -> bool {
    matches!(e, StreamError::Unsupported(_))
}

fn unknown_subject(e: &StreamError) -> bool {
    matches!(e, StreamError::UnknownSubject { .. })
}

// ---------------------------------------------------------------- transport

/// The client port speaks TLS only: the server announces it, and a plaintext
/// CONNECT is answered with an error and a closed socket, never a PONG.
#[tokio::test]
async fn plaintext_is_refused() {
    let _gate = GATE.read().await;
    let mut socket = TcpStream::connect(host_port()).await.unwrap();
    let mut buf = vec![0u8; 4096];
    let n = tokio::time::timeout(WITHIN, socket.read(&mut buf))
        .await
        .unwrap()
        .unwrap();
    let info = String::from_utf8_lossy(&buf[..n]).into_owned();
    assert!(info.starts_with("INFO "), "{info}");
    assert!(info.contains(r#""tls_required":true"#), "{info}");
    assert!(info.contains(r#""tls_verify":true"#), "{info}");

    let _ = socket
        .write_all(b"CONNECT {\"verbose\":false,\"pedantic\":false}\r\nPING\r\n")
        .await;
    let mut reply = Vec::new();
    let _ = tokio::time::timeout(WITHIN, socket.read_to_end(&mut reply)).await;
    let reply = String::from_utf8_lossy(&reply);
    assert!(
        !reply.contains("PONG"),
        "a plaintext client was served: {reply}"
    );
}

/// A client that does not trust the test CA cannot complete the handshake.
#[tokio::test]
async fn an_unknown_server_certificate_is_refused() {
    let _gate = GATE.read().await;
    let result = ConnectOptions::new()
        .require_tls(true)
        .connection_timeout(WITHIN)
        .connect(url())
        .await;
    assert!(result.is_err(), "connected without trusting the server CA");
}

/// Mutual TLS: trusting the server is not enough; the server wants a
/// certificate from the client too.
#[tokio::test]
async fn tls_without_a_client_certificate_is_refused() {
    let _gate = GATE.read().await;
    let attempt = async {
        let nc = trusting().connect(url()).await?;
        nc.flush().await?;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let result = tokio::time::timeout(Duration::from_secs(10), attempt).await;
    assert!(
        !matches!(result, Ok(Ok(()))),
        "a client without a certificate was served"
    );
}

#[tokio::test]
async fn mutual_tls_connects() {
    let _gate = GATE.read().await;
    let nc = client().await;
    nc.flush().await.unwrap();
    let info = nc.server_info();
    assert!(info.tls_required);
    assert!(info.jetstream);
}

// ---------------------------------------------------------------- core NATS

#[tokio::test]
async fn core_full_lifecycle_over_tls() {
    let _gate = GATE.read().await;
    let nc = client().await;
    let p = NatsProvider::new(nc.clone(), NatsConfig::default());
    let root = unique("orders");
    let placed = format!("{root}.placed");
    let pattern = format!("{root}.*");

    // Create with an explicit id and with a generated one.
    let explicit_id = unique("core-explicit");
    let explicit = p
        .create(Stream {
            id: explicit_id.clone(),
            name: "explicit".into(),
            subjects: subjects(&[&placed]),
            ..Stream::default()
        })
        .await
        .unwrap();
    assert_eq!(explicit.id, explicit_id);
    assert!(explicit.active);
    let generated = p
        .create(Stream {
            name: "orders".into(),
            description: "before".into(),
            subjects: subjects(&[&pattern]),
            user_id: "ada".into(),
            ..Stream::default()
        })
        .await
        .unwrap();
    assert!(generated.id.ends_with(":orders"), "{}", generated.id);

    // Get, list, update.
    assert_eq!(p.get(&generated.id).await.unwrap(), generated);
    let ids: Vec<String> = p.list().await.unwrap().into_iter().map(|s| s.id).collect();
    assert!(ids.contains(&explicit.id) && ids.contains(&generated.id));
    let updated = p
        .update(
            &generated.id,
            Stream {
                description: "after".into(),
                ..generated.clone()
            },
        )
        .await
        .unwrap();
    assert_eq!(p.get(&generated.id).await.unwrap().description, "after");
    assert_eq!(updated.id, generated.id);

    // Bind, publish, subscribe; the id option is honoured.
    let m = p.bind(&generated.id).await.unwrap();
    let mut sub = m.subscribe(&pattern, &Options::default()).await.unwrap();
    let id = m
        .publish(&placed, event("ada"), &Options::default().with_id("core-1"))
        .await
        .unwrap();
    assert_eq!(id, "core-1");
    let got = next(&mut sub).await;
    assert_eq!((got.id(), got.subject()), ("core-1", placed.as_str()));
    assert_eq!(got.json::<Event>().unwrap().user, "ada");

    // Prefetch and partition key only tune delivery here, so they are accepted.
    let tuned = Options::default()
        .with_prefetch(4)
        .with_partition_key("ada");
    let mut tuned_sub = m.subscribe(&placed, &tuned).await.unwrap();
    let id = m.publish(&placed, event("bo"), &tuned).await.unwrap();
    assert_eq!(next(&mut tuned_sub).await.id(), id);
    assert_eq!(next(&mut sub).await.id(), id);
    drop(tuned_sub);

    // Typed view.
    let orders = typed::<Event>(m.clone());
    let mut typed_sub = orders
        .subscribe(&placed, &Options::default())
        .await
        .unwrap();
    let ev = Event {
        user: "cy".into(),
        action: "placed".into(),
    };
    orders
        .publish(&placed, &ev, &Options::default())
        .await
        .unwrap();
    assert_eq!(next(&mut typed_sub).await, ev);
    next(&mut sub).await;

    // Undeclared and wildcard subjects, and the TTL it cannot honour.
    let other = unique("elsewhere");
    is(
        &m.publish(&other, event("x"), &Options::default()).await,
        unknown_subject,
    );
    is(
        &m.subscribe(&other, &Options::default()).await.map(drop),
        unknown_subject,
    );
    is(
        &m.publish(&pattern, event("x"), &Options::default()).await,
        unknown_subject,
    );
    let ttl = Options::default().with_ttl(Duration::from_secs(1));
    is(&m.publish(&placed, event("x"), &ttl).await, unsupported);

    // Capabilities: batch only.
    is(&as_durable(m.as_ref()).map(drop), unsupported);
    is(&as_positioned(m.as_ref()).map(drop), unsupported);
    let batch = as_batch(m.as_ref()).unwrap();
    let ids = batch
        .publish_batch(
            &placed,
            vec![event("a"), event("b"), event("c")],
            &Options::default(),
        )
        .await
        .unwrap();
    assert_eq!(ids.len(), 3);
    for id in ids {
        assert_eq!(next(&mut sub).await.id(), id);
    }
    is(
        &batch
            .publish_batch(&placed, vec![event("a")], &Options::default().with_id("x"))
            .await,
        unsupported,
    );

    // Dropping the subscriptions releases the server-side interest.
    drop(sub);
    drop(typed_sub);
    nc.flush().await.unwrap();
    assert!(
        nc.request(placed.clone(), "ping".into()).await.is_err(),
        "a dropped subscription still had interest"
    );

    // Delete, then NotFound everywhere.
    for s in [&explicit, &generated] {
        p.delete(&s.id).await.unwrap();
        is(&p.get(&s.id).await, not_found);
        is(&p.bind(&s.id).await.map(drop), not_found);
        is(&p.update(&s.id, Stream::default()).await, not_found);
        is(&p.delete(&s.id).await, not_found);
    }
    let ids: Vec<String> = p.list().await.unwrap().into_iter().map(|s| s.id).collect();
    assert!(!ids.contains(&explicit.id) && !ids.contains(&generated.id));
}

#[tokio::test]
async fn core_queue_groups_share_and_fan_out() {
    let _gate = GATE.read().await;
    let nc = client().await;
    let subject = unique("jobs");

    // The configured queue group: two members share twenty messages.
    let p = NatsProvider::new(
        nc.clone(),
        NatsConfig::default().with_queue_group("workers"),
    );
    let s = p
        .create(Stream {
            subjects: subjects(&[&subject]),
            ..Stream::default()
        })
        .await
        .unwrap();
    let m = p.bind(&s.id).await.unwrap();
    let first = m.subscribe(&subject, &Options::default()).await.unwrap();
    let second = m.subscribe(&subject, &Options::default()).await.unwrap();
    // A different group, named per call, receives its own copy of each.
    let mut audit = m
        .subscribe(&subject, &Options::default().with_group("audit"))
        .await
        .unwrap();
    for i in 0..20 {
        m.publish(&subject, event(&i.to_string()), &Options::default())
            .await
            .unwrap();
    }
    let mut shared = futures::stream::select(first, second);
    for _ in 0..20 {
        next(&mut shared).await;
        next(&mut audit).await;
    }
    nothing(&mut shared, Duration::from_millis(300)).await;
    nothing(&mut audit, Duration::from_millis(100)).await;
    p.delete(&s.id).await.unwrap();
}

// ---------------------------------------------------------------- JetStream

#[tokio::test]
async fn jetstream_full_lifecycle_over_tls() {
    let _gate = GATE.read().await;
    let nc = client().await;
    let js = async_nats::jetstream::new(nc.clone());
    let p = jetstream(nc.clone()).await;
    let subject = unique("user.created");
    let other = unique("user.deleted");

    // Create with an explicit id and with a generated one.
    let explicit_id = unique("js-explicit");
    let explicit = p
        .create(Stream {
            id: explicit_id.clone(),
            name: "explicit".into(),
            subjects: subjects(&[&other]),
            ..Stream::default()
        })
        .await
        .unwrap();
    assert_eq!(explicit.id, safe_name(&explicit_id));
    let created = p
        .create(Stream {
            name: "users.v1".into(),
            description: "before".into(),
            subjects: subjects(&[&subject]),
            user_id: "ada".into(),
            ..Stream::default()
        })
        .await
        .unwrap();
    assert!(created.id.ends_with(":users-v1"), "{}", created.id);

    // Stored on disk, not in memory.
    let mut raw = js.get_stream(&created.id).await.unwrap();
    assert_eq!(raw.info().await.unwrap().config.storage, StorageType::File);

    // Get, list, update.
    assert_eq!(p.get(&created.id).await.unwrap(), created);
    let updated = p
        .update(
            &created.id,
            Stream {
                description: "after".into(),
                ..created.clone()
            },
        )
        .await
        .unwrap();
    assert_eq!(p.get(&created.id).await.unwrap(), updated);
    let listed = p.list().await.unwrap();
    assert!(listed.contains(&updated));
    assert!(listed.iter().any(|s| s.id == explicit.id));
    let info = raw.info().await.unwrap();
    assert_eq!(
        info.config.storage,
        StorageType::File,
        "update moved storage"
    );
    assert_eq!(info.config.description.as_deref(), Some("after"));

    // Bind; publish waits for the stored ack, so the message is already there.
    let m = p.bind(&created.id).await.unwrap();
    let mut sub = m.subscribe(&subject, &Options::default()).await.unwrap();
    let id = m
        .publish(&subject, event("ada"), &Options::default().with_id("js-1"))
        .await
        .unwrap();
    assert_eq!(id, "js-1");
    assert_eq!(raw.info().await.unwrap().state.messages, 1);
    let got = next(&mut sub).await;
    assert_eq!(got.id(), "js-1");
    assert_eq!(got.json::<Event>().unwrap().user, "ada");

    // A retried id is collapsed by the server.
    m.publish(&subject, event("ada"), &Options::default().with_id("js-1"))
        .await
        .unwrap();
    assert_eq!(raw.info().await.unwrap().state.messages, 1);

    // Group on subscribe is ignored (as in Go); partition key is ignored.
    let tuned = Options::default()
        .with_group("ignored")
        .with_partition_key("ada")
        .with_prefetch(2);
    let mut tuned_sub = m.subscribe(&subject, &tuned).await.unwrap();
    let id = m.publish(&subject, event("bo"), &tuned).await.unwrap();
    assert_eq!(next(&mut tuned_sub).await.id(), id);
    assert_eq!(next(&mut sub).await.id(), id);
    drop(tuned_sub);

    // Typed view.
    let users = typed::<Event>(m.clone());
    let mut typed_sub = users
        .subscribe(&subject, &Options::default())
        .await
        .unwrap();
    let ev = Event {
        user: "cy".into(),
        action: "created".into(),
    };
    users
        .publish(&subject, &ev, &Options::default())
        .await
        .unwrap();
    assert_eq!(next(&mut typed_sub).await, ev);
    next(&mut sub).await;
    drop(typed_sub);
    drop(sub);

    // Refusals.
    is(
        &m.publish(&other, event("x"), &Options::default()).await,
        unknown_subject,
    );
    is(
        &m.subscribe(&other, &Options::default()).await.map(drop),
        unknown_subject,
    );
    let ttl = Options::default().with_ttl(Duration::from_secs(1));
    is(&m.publish(&subject, event("x"), &ttl).await, unsupported);
    let durable = as_durable(m.as_ref()).unwrap();
    is(
        &durable
            .consume(&subject, "", &Options::default())
            .await
            .map(drop),
        unsupported,
    );
    is(
        &durable
            .consume(&subject, "w", &Options::default().with_group("g"))
            .await
            .map(drop),
        unsupported,
    );
    is(
        &durable
            .consume(&other, "w", &Options::default())
            .await
            .map(drop),
        unknown_subject,
    );

    // Durable consume: nak redelivers with a counted attempt, ack settles.
    let mut deliveries: Deliveries = durable
        .consume(&subject, "workers", &Options::default().with_prefetch(1))
        .await
        .unwrap();
    let id = m
        .publish(&subject, event("dee"), &Options::default())
        .await
        .unwrap();
    let first = next(&mut deliveries).await;
    assert_eq!((first.id(), first.attempt()), (id.as_str(), 1));
    first.nak().await.unwrap();
    let second = next(&mut deliveries).await;
    assert_eq!((second.id(), second.attempt()), (id.as_str(), 2));
    second.ack().await.unwrap();
    nothing(&mut deliveries, Duration::from_millis(500)).await;
    drop(deliveries);
    let consumer = raw.consumer_info("workers").await.unwrap();
    assert_eq!(consumer.config.durable_name.as_deref(), Some("workers"));
    assert_eq!(consumer.num_ack_pending, 0);
    assert_eq!(consumer.num_pending, 0);

    // Batch, then replay from the earliest stored message with prefetch 1.
    let ids = as_batch(m.as_ref())
        .unwrap()
        .publish_batch(&subject, vec![event("e"), event("f")], &Options::default())
        .await
        .unwrap();
    is(
        &as_batch(m.as_ref())
            .unwrap()
            .publish_batch(&subject, vec![event("x")], &Options::default().with_id("x"))
            .await,
        unsupported,
    );
    let stored = raw.info().await.unwrap().state.messages;
    assert_eq!(stored, 6, "every acked publish is stored");
    let positioned = as_positioned(m.as_ref()).unwrap();
    let mut replay = positioned
        .consume_from(
            &subject,
            "replayer",
            Position::Earliest,
            &Options::default().with_prefetch(1),
        )
        .await
        .unwrap();
    let mut replayed = Vec::new();
    for _ in 0..stored {
        let d = next(&mut replay).await;
        replayed.push(d.id().to_owned());
        d.ack().await.unwrap();
    }
    assert_eq!(replayed[0], "js-1");
    assert_eq!(&replayed[replayed.len() - 2..], ids.as_slice());
    nothing(&mut replay, Duration::from_millis(300)).await;
    drop(replay);

    // Position::New skips what is already stored.
    let mut fresh = positioned
        .consume_from(&subject, "fresh", Position::New, &Options::default())
        .await
        .unwrap();
    let id = m
        .publish(&subject, event("g"), &Options::default())
        .await
        .unwrap();
    let d = next(&mut fresh).await;
    assert_eq!(d.id(), id);
    d.ack().await.unwrap();
    drop(fresh);

    // Delete: the stream and every consumer on it are gone from the server.
    for s in [&explicit, &updated] {
        p.delete(&s.id).await.unwrap();
        assert!(js.get_stream(&s.id).await.is_err(), "{} survived", s.id);
        is(&p.get(&s.id).await, not_found);
        is(&p.bind(&s.id).await.map(drop), not_found);
        is(&p.update(&s.id, s.clone()).await, not_found);
        is(&p.delete(&s.id).await, not_found);
    }
    let listed = p.list().await.unwrap();
    assert!(
        !listed
            .iter()
            .any(|s| s.id == explicit.id || s.id == updated.id)
    );
    let names: Vec<String> = js
        .stream_names()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect();
    assert!(!names.contains(&updated.id) && !names.contains(&explicit.id));
}

// ---------------------------------------------------------------- restart

fn docker(args: &[&str]) -> String {
    let out = Command::new("docker")
        .args(args)
        .output()
        .expect("docker is on PATH");
    assert!(
        out.status.success(),
        "docker {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_owned()
}

async fn wait_healthy() {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let status = docker(&["inspect", "-f", "{{.State.Health.Status}}", CONTAINER]);
        if status == "healthy" {
            return;
        }
        assert!(Instant::now() < deadline, "{CONTAINER} is {status}");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Acked work stays done and unacked work comes back after the broker
/// restarts: the stream, the durable consumer, and its position live on the
/// named volume.
#[tokio::test]
async fn jetstream_survives_a_broker_restart() {
    if std::env::var("STREAMS_E2E_RESTART").as_deref() != Ok("1") {
        eprintln!("skipped: set STREAMS_E2E_RESTART=1 to restart {CONTAINER}");
        return;
    }
    let _gate = GATE.write().await;

    let subject = unique("durable.orders");
    let consumer = "restart-workers";
    let (stream_id, acked, inflight, queued) = {
        let nc = client().await;
        let p = jetstream(nc.clone()).await;
        let s = p
            .create(Stream {
                name: "durable".into(),
                subjects: subjects(&[&subject]),
                ..Stream::default()
            })
            .await
            .unwrap();
        let m = p.bind(&s.id).await.unwrap();
        let durable = as_durable(m.as_ref()).unwrap();
        let mut deliveries = durable
            .consume(&subject, consumer, &Options::default().with_prefetch(1))
            .await
            .unwrap();

        let acked = m
            .publish(&subject, event("acked"), &Options::default())
            .await
            .unwrap();
        let d = next(&mut deliveries).await;
        assert_eq!(d.id(), acked);
        d.ack().await.unwrap();

        // Delivered and left unsettled when the broker goes down.
        let inflight = m
            .publish(&subject, event("inflight"), &Options::default())
            .await
            .unwrap();
        let d = next(&mut deliveries).await;
        assert_eq!((d.id(), d.attempt()), (inflight.as_str(), 1));
        drop(d);
        drop(deliveries);

        // Stored while nobody pulls, never delivered.
        let queued = m
            .publish(&subject, event("queued"), &Options::default())
            .await
            .unwrap();
        let js = async_nats::jetstream::new(nc.clone());
        let info = js.get_stream(&s.id).await.unwrap().cached_info().clone();
        assert_eq!(info.config.storage, StorageType::File);
        assert_eq!(info.state.messages, 3);
        (s.id, acked, inflight, queued)
    };

    docker(&["restart", CONTAINER]);
    wait_healthy().await;

    let nc = client().await;
    let p = jetstream(nc.clone()).await;
    let s = p.get(&stream_id).await.expect("the stream survived");
    assert_eq!(s.name, "durable");
    let m = p.bind(&stream_id).await.unwrap();
    let mut deliveries = as_durable(m.as_ref())
        .unwrap()
        .consume(&subject, consumer, &Options::default())
        .await
        .unwrap();

    // The never-delivered one comes at once; the in-flight one once its ack
    // wait (JetStream's default 30s) runs out, as a second attempt. The acked
    // one never comes back.
    let mut seen = Vec::new();
    while seen.len() < 2 {
        let d = next_within(&mut deliveries, Duration::from_secs(45)).await;
        assert_ne!(d.id(), acked, "an acked message was redelivered");
        if d.id() == inflight {
            assert!(d.attempt() >= 2, "{d:?}");
        } else {
            assert_eq!(d.id(), queued);
        }
        seen.push(d.id().to_owned());
        d.ack().await.unwrap();
    }
    seen.sort();
    let mut want = vec![inflight, queued];
    want.sort();
    assert_eq!(seen, want);
    nothing(&mut deliveries, Duration::from_secs(1)).await;
    drop(deliveries);

    p.delete(&stream_id).await.unwrap();
    is(&p.get(&stream_id).await, not_found);
}
