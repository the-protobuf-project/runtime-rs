//! The NATS providers against a real server, core and JetStream on the same one.
//!
//! Start it with `docker compose -f streams/docker/compose.nats.yaml up -d
//! --wait`; `NATS_URL` overrides the default `nats://127.0.0.1:14222`. Every
//! test declares streams under fresh ids, so runs do not see each other's
//! messages and need no cleanup to be repeatable.

use std::{sync::Arc, time::Duration};

use futures::StreamExt;
use serde::{Deserialize, Serialize};
use streams::{
    Deliveries, Manager, Options, Payload, Position, Stream, StreamError, Streams, Subscription,
    as_batch, as_durable, as_positioned,
    drivers::nats::{JetStreamProvider, NatsConfig, NatsProvider, safe_name},
    provider,
};

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
struct Event {
    user: String,
    action: String,
}

const SUBJECT: &str = "user.created";
const WITHIN: Duration = Duration::from_secs(5);

fn event(user: &str) -> Payload {
    Payload::json(&Event {
        user: user.into(),
        action: "created".into(),
    })
    .unwrap()
}

async fn client() -> async_nats::Client {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:14222".into());
    async_nats::connect(&url)
        .await
        .unwrap_or_else(|error| panic!("no NATS at {url} ({error}); see docker/compose.nats.yaml"))
}

/// A subject unique to this run, so concurrent tests on one server stay apart.
fn unique(prefix: &str) -> String {
    format!("{prefix}.{}", provider::new_id())
}

async fn declare(streams: &dyn Streams, subjects: &[&str]) -> (Stream, Arc<dyn Manager>) {
    let stream = streams
        .create(Stream {
            name: "live".into(),
            subjects: subjects.iter().map(|s| (*s).to_owned()).collect(),
            ..Stream::default()
        })
        .await
        .unwrap();
    let manager = streams.bind(&stream.id).await.unwrap();
    (stream, manager)
}

async fn next<S: futures::Stream + Unpin>(stream: &mut S) -> S::Item {
    tokio::time::timeout(WITHIN, stream.next())
        .await
        .expect("nothing arrived in time")
        .expect("the stream ended")
}

async fn nothing<S: futures::Stream + Unpin>(stream: &mut S, within: Duration) {
    if let Ok(Some(_)) = tokio::time::timeout(within, stream.next()).await {
        panic!("something arrived that should not have");
    }
}

// ---------------------------------------------------------------- core NATS

#[tokio::test]
async fn plain_publish_and_subscribe() {
    let provider = NatsProvider::new(client().await, NatsConfig::default());
    let subject = unique(SUBJECT);
    let (_, m) = declare(&provider, &[&subject]).await;

    let mut sub = m.subscribe(&subject, &Options::default()).await.unwrap();
    let id = m
        .publish(&subject, event("ada"), &Options::default())
        .await
        .unwrap();

    let message = next(&mut sub).await;
    assert_eq!(message.id(), id);
    assert_eq!(message.subject(), subject);
    assert_eq!(message.json::<Event>().unwrap().user, "ada");
}

#[tokio::test]
async fn plain_wildcard_subjects() {
    let provider = NatsProvider::new(client().await, NatsConfig::default());
    let root = unique("orders");
    let pattern = format!("{root}.*");
    let placed = format!("{root}.placed");
    let (_, m) = declare(&provider, &[&pattern]).await;

    let mut sub = m.subscribe(&pattern, &Options::default()).await.unwrap();
    m.publish(&placed, event("ada"), &Options::default())
        .await
        .unwrap();
    assert_eq!(next(&mut sub).await.subject(), placed);

    let refused = m.publish(&pattern, event("ada"), &Options::default()).await;
    assert!(
        matches!(refused, Err(StreamError::UnknownSubject { .. })),
        "{refused:?}"
    );
}

#[tokio::test]
async fn plain_queue_group_shares_the_subject() {
    let provider = NatsProvider::new(
        client().await,
        NatsConfig::default().with_queue_group("workers"),
    );
    let subject = unique(SUBJECT);
    let (_, m) = declare(&provider, &[&subject]).await;

    let first = m.subscribe(&subject, &Options::default()).await.unwrap();
    let second = m.subscribe(&subject, &Options::default()).await.unwrap();
    for i in 0..20 {
        m.publish(&subject, event(&i.to_string()), &Options::default())
            .await
            .unwrap();
    }

    let mut both = futures::stream::select(first, second);
    for _ in 0..20 {
        next(&mut both).await;
    }
    // Shared, not fanned out: twenty published, twenty delivered in total.
    nothing(&mut both, Duration::from_millis(300)).await;
}

#[tokio::test]
async fn plain_without_a_group_fans_out() {
    let provider = NatsProvider::new(client().await, NatsConfig::default());
    let subject = unique(SUBJECT);
    let (_, m) = declare(&provider, &[&subject]).await;

    let mut first = m.subscribe(&subject, &Options::default()).await.unwrap();
    let mut second = m.subscribe(&subject, &Options::default()).await.unwrap();
    let id = m
        .publish(&subject, event("ada"), &Options::default())
        .await
        .unwrap();
    assert_eq!(next(&mut first).await.id(), id);
    assert_eq!(next(&mut second).await.id(), id);
}

#[tokio::test]
async fn plain_drops_a_foreign_payload_and_keeps_going() {
    let nc = client().await;
    let provider = NatsProvider::new(nc.clone(), NatsConfig::default());
    let subject = unique(SUBJECT);
    let (_, m) = declare(&provider, &[&subject]).await;

    let mut sub = m.subscribe(&subject, &Options::default()).await.unwrap();
    nc.publish(subject.clone(), "not a frame".into())
        .await
        .unwrap();
    let id = m
        .publish(&subject, event("ada"), &Options::default())
        .await
        .unwrap();
    assert_eq!(next(&mut sub).await.id(), id);
}

#[tokio::test]
async fn plain_drop_unsubscribes() {
    let nc = client().await;
    let provider = NatsProvider::new(nc.clone(), NatsConfig::default());
    let subject = unique(SUBJECT);
    let (_, m) = declare(&provider, &[&subject]).await;

    let sub: Subscription = m.subscribe(&subject, &Options::default()).await.unwrap();
    drop(sub);
    nc.flush().await.unwrap();

    // With the interest gone the server has no one to route to, which a
    // request with no responders reports at once.
    let reply = nc.request(subject.clone(), "ping".into()).await;
    assert!(
        reply.is_err(),
        "the dropped subscription still had interest"
    );
}

#[tokio::test]
async fn plain_batch_and_capabilities() {
    let provider = NatsProvider::new(client().await, NatsConfig::default());
    let subject = unique(SUBJECT);
    let (_, m) = declare(&provider, &[&subject]).await;
    assert!(as_durable(m.as_ref()).is_err());
    assert!(as_positioned(m.as_ref()).is_err());

    let mut sub = m.subscribe(&subject, &Options::default()).await.unwrap();
    let ids = as_batch(m.as_ref())
        .unwrap()
        .publish_batch(&subject, vec![event("a"), event("b")], &Options::default())
        .await
        .unwrap();
    for id in ids {
        assert_eq!(next(&mut sub).await.id(), id);
    }
}

// ---------------------------------------------------------------- JetStream

async fn jetstream() -> JetStreamProvider {
    JetStreamProvider::new(client().await, NatsConfig::default())
        .await
        .expect("the server has JetStream enabled")
}

#[tokio::test]
async fn jetstream_lifecycle() {
    let provider = jetstream().await;
    let subject = unique(SUBJECT);
    let created = provider
        .create(Stream {
            name: "users.v1".into(),
            description: "before".into(),
            subjects: vec![subject.clone()],
            user_id: "ada".into(),
            ..Stream::default()
        })
        .await
        .unwrap();
    // The generated id carries the name, made safe for a JetStream name.
    assert!(created.id.ends_with(":users-v1"), "{}", created.id);

    let got = provider.get(&created.id).await.unwrap();
    assert_eq!(got, created);

    let updated = provider
        .update(
            &created.id,
            Stream {
                name: "users.v1".into(),
                description: "after".into(),
                subjects: vec![subject.clone()],
                user_id: "ada".into(),
                ..Stream::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(provider.get(&created.id).await.unwrap(), updated);
    assert!(provider.list().await.unwrap().contains(&updated));

    provider.delete(&created.id).await.unwrap();
    for result in [
        provider.get(&created.id).await.map(drop),
        provider.bind(&created.id).await.map(drop),
        provider.delete(&created.id).await,
        provider
            .update(&created.id, updated.clone())
            .await
            .map(drop),
    ] {
        assert!(
            matches!(result, Err(StreamError::NotFound(_))),
            "{result:?}"
        );
    }
}

#[tokio::test]
async fn jetstream_refuses_what_it_cannot_mean() {
    let provider = jetstream().await;
    let subject = unique(SUBJECT);
    let (stream, m) = declare(&provider, &[&subject]).await;

    let ttl = Options::default().with_ttl(Duration::from_secs(1));
    let delayed = m.publish(&subject, event("ada"), &ttl).await;
    assert!(
        matches!(delayed, Err(StreamError::Unsupported(_))),
        "{delayed:?}"
    );

    let durable = as_durable(m.as_ref()).unwrap();
    for (consumer, opts) in [
        ("", Options::default()),
        ("workers", Options::default().with_group("g")),
    ] {
        let refused = durable.consume(&subject, consumer, &opts).await.map(drop);
        assert!(
            matches!(refused, Err(StreamError::Unsupported(_))),
            "{refused:?}"
        );
    }
    provider.delete(&stream.id).await.unwrap();
}

#[tokio::test]
async fn jetstream_subscribe_starts_at_new() {
    let provider = jetstream().await;
    let subject = unique(SUBJECT);
    let (stream, m) = declare(&provider, &[&subject]).await;

    m.publish(&subject, event("old"), &Options::default())
        .await
        .unwrap();
    let mut sub = m.subscribe(&subject, &Options::default()).await.unwrap();
    let id = m
        .publish(&subject, event("new"), &Options::default())
        .await
        .unwrap();
    assert_eq!(next(&mut sub).await.id(), id);

    drop(sub);
    provider.delete(&stream.id).await.unwrap();
}

#[tokio::test]
async fn jetstream_durable_redelivers_a_nak() {
    let provider = jetstream().await;
    let subject = unique(SUBJECT);
    let (stream, m) = declare(&provider, &[&subject]).await;

    let mut deliveries: Deliveries = as_durable(m.as_ref())
        .unwrap()
        .consume(&subject, "workers", &Options::default())
        .await
        .unwrap();
    let id = m
        .publish(&subject, event("ada"), &Options::default())
        .await
        .unwrap();

    let first = next(&mut deliveries).await;
    assert_eq!(first.id(), id);
    assert_eq!(first.attempt(), 1);
    first.nak().await.unwrap();

    let second = next(&mut deliveries).await;
    assert_eq!(second.id(), id);
    assert_eq!(second.attempt(), 2);
    second.ack().await.unwrap();

    nothing(&mut deliveries, Duration::from_millis(500)).await;
    drop(deliveries);
    provider.delete(&stream.id).await.unwrap();
}

#[tokio::test]
async fn jetstream_durable_resumes_under_its_name() {
    let provider = jetstream().await;
    let subject = unique(SUBJECT);
    let (stream, m) = declare(&provider, &[&subject]).await;
    let durable = as_durable(m.as_ref()).unwrap();

    let mut deliveries = durable
        .consume(&subject, "resumer", &Options::default())
        .await
        .unwrap();
    m.publish(&subject, event("one"), &Options::default())
        .await
        .unwrap();
    next(&mut deliveries).await.ack().await.unwrap();
    drop(deliveries);

    // Published while nobody is attached: the name, not the process, holds the
    // position, so it arrives on re-attach.
    let id = m
        .publish(&subject, event("two"), &Options::default())
        .await
        .unwrap();
    let mut deliveries = durable
        .consume(&subject, "resumer", &Options::default())
        .await
        .unwrap();
    let delivery = next(&mut deliveries).await;
    assert_eq!(delivery.id(), id);
    delivery.ack().await.unwrap();

    drop(deliveries);
    provider.delete(&stream.id).await.unwrap();
}

#[tokio::test]
async fn jetstream_positioned_replays_from_earliest() {
    let provider = jetstream().await;
    let subject = unique(SUBJECT);
    let (stream, m) = declare(&provider, &[&subject]).await;

    let ids = as_batch(m.as_ref())
        .unwrap()
        .publish_batch(&subject, vec![event("a"), event("b")], &Options::default())
        .await
        .unwrap();
    let mut deliveries = as_positioned(m.as_ref())
        .unwrap()
        .consume_from(
            &subject,
            "replayer",
            Position::Earliest,
            &Options::default().with_prefetch(1),
        )
        .await
        .unwrap();
    for id in ids {
        let delivery = next(&mut deliveries).await;
        assert_eq!(delivery.id(), id);
        delivery.ack().await.unwrap();
    }

    drop(deliveries);
    provider.delete(&stream.id).await.unwrap();
}

/// A pipelined batch several times its in-flight window, and larger than
/// async-nats' 5,000 outstanding acks, finishes and is stored in order.
#[tokio::test]
async fn jetstream_a_large_batch_lands_in_order() {
    let provider = jetstream().await;
    let subject = unique(SUBJECT);
    let (stream, m) = declare(&provider, &[&subject]).await;

    let payloads = (0..6_000).map(|i| event(&i.to_string())).collect();
    let ids = tokio::time::timeout(
        Duration::from_secs(30),
        as_batch(m.as_ref())
            .unwrap()
            .publish_batch(&subject, payloads, &Options::default()),
    )
    .await
    .expect("the batch finished")
    .unwrap();
    assert_eq!(ids.len(), 6_000);

    let mut deliveries = as_positioned(m.as_ref())
        .unwrap()
        .consume_from(
            &subject,
            "in-order",
            Position::Earliest,
            &Options::default(),
        )
        .await
        .unwrap();
    for id in ids {
        let delivery = next(&mut deliveries).await;
        assert_eq!(delivery.id(), id);
        delivery.ack().await.unwrap();
    }

    drop(deliveries);
    provider.delete(&stream.id).await.unwrap();
}

#[tokio::test]
async fn jetstream_terminates_a_foreign_payload() {
    let nc = client().await;
    let provider = JetStreamProvider::new(nc.clone(), NatsConfig::default())
        .await
        .unwrap();
    let subject = unique(SUBJECT);
    let (stream, m) = declare(&provider, &[&subject]).await;

    let mut deliveries = as_durable(m.as_ref())
        .unwrap()
        .consume(&subject, "strict", &Options::default())
        .await
        .unwrap();
    let js = async_nats::jetstream::new(nc);
    js.publish(subject.clone(), "not a frame".into())
        .await
        .unwrap()
        .await
        .unwrap();
    let id = m
        .publish(&subject, event("ada"), &Options::default())
        .await
        .unwrap();

    let delivery = next(&mut deliveries).await;
    assert_eq!(delivery.id(), id);
    delivery.ack().await.unwrap();
    nothing(&mut deliveries, Duration::from_millis(500)).await;

    drop(deliveries);
    provider.delete(&stream.id).await.unwrap();
}

/// What a Go peer relies on: the id in `Nats-Msg-Id`, the frame as the body,
/// the declaration in runtime-go's metadata keys, and the server collapsing a
/// retried publish.
#[tokio::test]
async fn jetstream_is_wire_compatible_with_go() {
    let nc = client().await;
    let provider = JetStreamProvider::new(nc.clone(), NatsConfig::default())
        .await
        .unwrap();
    let subject = unique(SUBJECT);
    let (stream, m) = declare(&provider, &[&subject]).await;

    let opts = Options::default().with_id("retried-1");
    m.publish(&subject, event("ada"), &opts).await.unwrap();
    m.publish(&subject, event("ada"), &opts).await.unwrap();

    let js = async_nats::jetstream::new(nc);
    let mut handle = js.get_stream(safe_name(&stream.id)).await.unwrap();
    let info = handle.info().await.unwrap().clone();
    assert_eq!(info.state.messages, 1, "the duplicate was appended");
    assert_eq!(info.config.metadata["runtime-go.name"], "live");
    assert_eq!(info.config.metadata["runtime-go.active"], "true");

    let raw = handle
        .get_raw_message(info.state.first_sequence)
        .await
        .unwrap();
    assert_eq!(
        raw.headers.get("Nats-Msg-Id").map(|v| v.as_str()),
        Some("retried-1")
    );
    assert_eq!(
        raw.headers.iter().count(),
        1,
        "only Nats-Msg-Id without headers"
    );
    assert!(!info.config.metadata.contains_key("runtime-go.headers"));
    let message = provider::unpack(&streams::Registry::new(), &subject, raw.payload).unwrap();
    assert_eq!(message.id(), "retried-1");
    assert!(message.headers().is_empty());

    provider.delete(&stream.id).await.unwrap();
}

// ---------------------------------------------------------------- headers

fn headed(subject: &str) -> Stream {
    Stream::named("headed")
        .subjects([subject])
        .header("lang", "rust")
        .header("site", "plant-3")
}

fn invalid_header<T: std::fmt::Debug>(result: streams::Result<T>) {
    assert!(
        matches!(result, Err(StreamError::InvalidHeader(_))),
        "{result:?}"
    );
}

fn refused_naming<T: std::fmt::Debug>(result: streams::Result<T>, says: &str) {
    assert!(
        matches!(&result, Err(StreamError::Unsupported(e)) if e.contains(says)),
        "{result:?}"
    );
}

/// The stream's headers arrive, a publish's own win, and both are NATS
/// headers another client reads, outside the payload. A message without any
/// goes out as a plain PUB, as a Go publisher's does.
#[tokio::test]
async fn plain_headers_travel_as_nats_headers() {
    let nc = client().await;
    let provider = NatsProvider::new(nc.clone(), NatsConfig::default());
    let subject = unique(SUBJECT);
    let stream = provider.create(headed(&subject)).await.unwrap();
    assert_eq!(
        provider.get(&stream.id).await.unwrap().headers,
        stream.headers
    );
    let m = provider.bind(&stream.id).await.unwrap();
    let mut raw = nc.subscribe(subject.clone()).await.unwrap();
    let mut sub = m.subscribe(&subject, &Options::default()).await.unwrap();

    let opts = Options::default()
        .header("site", "plant-4")
        .header("trace", "00-ab");
    let id = m.publish(&subject, event("ada"), &opts).await.unwrap();
    let got = next(&mut sub).await;
    assert_eq!(got.id(), id);
    assert_eq!(got.headers().get("lang"), Some("rust"));
    assert_eq!(got.headers().get("site"), Some("plant-4"));
    assert_eq!(got.headers().get("trace"), Some("00-ab"));
    let native = next(&mut raw).await;
    let headers = native.headers.expect("NATS headers");
    assert_eq!(headers.get("site").map(|v| v.as_str()), Some("plant-4"));
    let framed = provider::unpack(&streams::Registry::new(), &subject, native.payload).unwrap();
    assert!(
        framed.headers().is_empty(),
        "headers stayed out of the frame"
    );

    let bare = provider
        .create(Stream::named("bare").subjects([subject.as_str()]))
        .await
        .unwrap();
    let bare = provider.bind(&bare.id).await.unwrap();
    bare.publish(&subject, event("go"), &Options::default())
        .await
        .unwrap();
    assert!(next(&mut sub).await.headers().is_empty());
    assert!(
        next(&mut raw).await.headers.is_none(),
        "a header-less publish is a plain PUB"
    );

    let ids = as_batch(m.as_ref())
        .unwrap()
        .publish_batch(&subject, vec![event("a"), event("b")], &Options::default())
        .await
        .unwrap();
    for id in ids {
        let got = next(&mut sub).await;
        assert_eq!(
            (got.id(), got.headers().get("lang")),
            (id.as_str(), Some("rust"))
        );
    }
}

#[tokio::test]
async fn plain_refuses_bad_headers_expiry_and_retain() {
    let provider = NatsProvider::new(client().await, NatsConfig::default());
    let subject = unique(SUBJECT);
    invalid_header(
        provider
            .create(headed(&subject).header("Nats-Msg-Id", "x"))
            .await,
    );
    let (_, m) = declare(&provider, &[&subject]).await;
    invalid_header(
        m.publish(
            &subject,
            event("a"),
            &Options::default().header("Nats-TTL", "1"),
        )
        .await,
    );
    invalid_header(
        m.publish(&subject, event("a"), &Options::default().header("a b", "1"))
            .await,
    );
    let expiring = Options::default().with_expiry(Duration::from_secs(5));
    refused_naming(m.publish(&subject, event("a"), &expiring).await, "expire");
    let retained = Options::default().with_retain();
    refused_naming(m.publish(&subject, event("a"), &retained).await, "retain");
}

/// The stream's headers live in its metadata, so another provider binding it
/// stamps the same ones; they arrive on deliveries, redeliveries included,
/// and the stored message carries them as NATS headers.
#[tokio::test]
async fn jetstream_headers_are_stored_and_redelivered() {
    let nc = client().await;
    let provider = jetstream().await;
    let subject = unique(SUBJECT);
    let stream = provider.create(headed(&subject)).await.unwrap();
    let other = JetStreamProvider::new(nc.clone(), NatsConfig::default())
        .await
        .unwrap();
    assert_eq!(other.get(&stream.id).await.unwrap().headers, stream.headers);
    let m = other.bind(&stream.id).await.unwrap();

    let mut deliveries = as_durable(m.as_ref())
        .unwrap()
        .consume(&subject, "headed", &Options::default())
        .await
        .unwrap();
    let opts = Options::default().header("site", "plant-4");
    let id = m.publish(&subject, event("ada"), &opts).await.unwrap();
    let first = next(&mut deliveries).await;
    assert_eq!(first.id(), id);
    assert_eq!(first.headers().get("lang"), Some("rust"));
    assert_eq!(first.headers().get("site"), Some("plant-4"));
    assert!(
        !first.headers().contains_key("Nats-Msg-Id"),
        "{:?}",
        first.headers()
    );
    first.nak().await.unwrap();
    let second = next(&mut deliveries).await;
    assert_eq!((second.id(), second.attempt()), (id.as_str(), 2));
    assert_eq!(second.headers().get("site"), Some("plant-4"));
    second.ack().await.unwrap();

    let ids = as_batch(m.as_ref())
        .unwrap()
        .publish_batch(&subject, vec![event("a"), event("b")], &opts)
        .await
        .unwrap();
    for id in &ids {
        let d = next(&mut deliveries).await;
        assert_eq!(d.id(), id);
        assert_eq!(d.headers().get("site"), Some("plant-4"));
        d.ack().await.unwrap();
    }

    let js = async_nats::jetstream::new(nc);
    let handle = js.get_stream(safe_name(&stream.id)).await.unwrap();
    let raw = handle.get_raw_message(1).await.unwrap();
    assert_eq!(raw.headers.get("lang").map(|v| v.as_str()), Some("rust"));
    assert_eq!(
        raw.headers.get("Nats-Msg-Id").map(|v| v.as_str()),
        Some(id.as_str())
    );

    // An update replaces the set; one without headers leaves none behind.
    let updated = provider
        .update(
            &stream.id,
            Stream::named("headed").subjects([subject.as_str()]),
        )
        .await
        .unwrap();
    assert!(updated.headers.is_empty());
    assert!(provider.get(&stream.id).await.unwrap().headers.is_empty());
    invalid_header(
        provider
            .update(
                &stream.id,
                headed(&subject).header("Nats-Expected-Stream", "x"),
            )
            .await,
    );
    drop(deliveries);
    provider.delete(&stream.id).await.unwrap();
}

#[tokio::test]
async fn jetstream_refuses_bad_headers_and_retain() {
    let provider = jetstream().await;
    let subject = unique(SUBJECT);
    invalid_header(
        provider
            .create(headed(&subject).header("Nats-TTL", "1"))
            .await,
    );
    invalid_header(provider.create(headed(&subject).header("", "1")).await);
    let (stream, m) = declare(&provider, &[&subject]).await;
    let reserved = Options::default().header("Nats-Msg-Id", "x");
    invalid_header(m.publish(&subject, event("a"), &reserved).await);
    invalid_header(
        as_batch(m.as_ref())
            .unwrap()
            .publish_batch(&subject, vec![event("a")], &reserved)
            .await,
    );
    let retained = Options::default().with_retain();
    refused_naming(m.publish(&subject, event("a"), &retained).await, "retain");
    let short = Options::default().with_expiry(Duration::from_millis(500));
    refused_naming(
        m.publish(&subject, event("a"), &short).await,
        "whole seconds",
    );
    provider.delete(&stream.id).await.unwrap();
}

/// `Nats-TTL`: the server removes the message once its expiry passes, so a
/// consumer that arrives later never sees it, while one without an expiry
/// stays.
#[tokio::test]
async fn jetstream_expiry_removes_the_message() {
    let nc = client().await;
    let provider = jetstream().await;
    let subject = unique(SUBJECT);
    let (stream, m) = declare(&provider, &[&subject]).await;
    let expiring = Options::default().with_expiry(Duration::from_secs(1));
    let gone = m.publish(&subject, event("late"), &expiring).await.unwrap();
    let kept = m
        .publish(&subject, event("kept"), &Options::default())
        .await
        .unwrap();

    let js = async_nats::jetstream::new(nc);
    let raw = js
        .get_stream(safe_name(&stream.id))
        .await
        .unwrap()
        .get_raw_message(1)
        .await
        .unwrap();
    assert_eq!(raw.headers.get("Nats-TTL").map(|v| v.as_str()), Some("1"));

    let deadline = std::time::Instant::now() + WITHIN;
    loop {
        let mut handle = js.get_stream(safe_name(&stream.id)).await.unwrap();
        if handle.info().await.unwrap().state.messages == 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the message did not expire"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let mut deliveries = as_positioned(m.as_ref())
        .unwrap()
        .consume_from(&subject, "late", Position::Earliest, &Options::default())
        .await
        .unwrap();
    let d = next(&mut deliveries).await;
    assert_ne!(d.id(), gone);
    assert_eq!(d.id(), kept);
    d.ack().await.unwrap();
    drop(deliveries);
    provider.delete(&stream.id).await.unwrap();
}

/// A delayed nak: the server holds the redelivery back for the delay, rather
/// than the client sleeping.
#[tokio::test]
async fn jetstream_nak_after_holds_the_redelivery_back() {
    let provider = jetstream().await;
    let subject = unique(SUBJECT);
    let (stream, m) = declare(&provider, &[&subject]).await;
    let mut deliveries = as_durable(m.as_ref())
        .unwrap()
        .consume(&subject, "patient", &Options::default())
        .await
        .unwrap();
    let id = m
        .publish(&subject, event("ada"), &Options::default())
        .await
        .unwrap();

    let first = next(&mut deliveries).await;
    assert!(first.can_delay_redelivery());
    let naked = std::time::Instant::now();
    first.nak_after(Duration::from_secs(2)).await.unwrap();
    nothing(&mut deliveries, Duration::from_millis(1500)).await;
    let second = next(&mut deliveries).await;
    assert!(
        naked.elapsed() >= Duration::from_secs(2),
        "{:?}",
        naked.elapsed()
    );
    assert_eq!((second.id(), second.attempt()), (id.as_str(), 2));
    second.ack().await.unwrap();
    drop(deliveries);
    provider.delete(&stream.id).await.unwrap();
}

/// A stream some other tool created without `allow_msg_ttl` refuses an
/// expiry by name, rather than as the server's error, until it is updated
/// through this provider.
#[tokio::test]
async fn jetstream_expiry_needs_allow_msg_ttl() {
    let nc = client().await;
    let provider = jetstream().await;
    let subject = unique(SUBJECT);
    let id = safe_name(&provider::new_stream_id("foreign"));
    async_nats::jetstream::new(nc)
        .create_stream(async_nats::jetstream::stream::Config {
            name: id.clone(),
            subjects: vec![subject.clone()],
            ..Default::default()
        })
        .await
        .unwrap();
    let m = provider.bind(&id).await.unwrap();
    let expiring = Options::default().with_expiry(Duration::from_secs(5));
    refused_naming(
        m.publish(&subject, event("a"), &expiring).await,
        "allow_msg_ttl",
    );
    assert!(
        m.publish(&subject, event("a"), &Options::default())
            .await
            .is_ok()
    );

    provider
        .update(&id, Stream::named("foreign").subjects([subject.as_str()]))
        .await
        .unwrap();
    let m = provider.bind(&id).await.unwrap();
    m.publish(&subject, event("a"), &expiring).await.unwrap();
    provider.delete(&id).await.unwrap();
}
