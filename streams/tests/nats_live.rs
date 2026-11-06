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
    let message = provider::unpack(&streams::Registry::new(), &subject, raw.payload).unwrap();
    assert_eq!(message.id(), "retried-1");

    provider.delete(&stream.id).await.unwrap();
}
