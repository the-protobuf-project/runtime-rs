//! The ZeroMQ provider against real sockets.
//!
//! ZeroMQ is brokerless, so these need nothing running: the publisher binds a
//! socket in this process and the subscriber connects to it. The `zeromq` crate
//! has no inproc transport, so where runtime-go's tests use inproc these bind
//! `tcp://127.0.0.1:0` and read the port back, which cannot collide.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use streams::{
    Headers, Manager, Options, Payload, Stream, StreamError, Streams, Subscription,
    drivers::zeromq::{ZmqConfig, ZmqProvider},
    provider,
};
use zeromq::{Socket, SocketRecv, SocketSend, ZmqMessage};

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
struct Event {
    user: String,
    action: String,
}

const SUBJECT: &str = "user.created";
const STREAM: &str = "s-1";

/// Long enough for a subscription to cross loopback, as in runtime-go's tests.
const SETTLE: Duration = Duration::from_millis(150);
/// How long a test waits for a delivery before calling it lost.
const WAIT: Duration = Duration::from_secs(10);

fn event(user: &str) -> Payload {
    Payload::json(&Event {
        user: user.into(),
        action: "created".into(),
    })
    .unwrap()
}

/// A publisher and a subscriber over one endpoint, each with its own
/// declaration of the same stream, which is what two processes would have.
struct Pair {
    publisher: ZmqProvider,
    subscriber: ZmqProvider,
    pm: Arc<dyn Manager>,
    sm: Arc<dyn Manager>,
}

async fn pair(subjects: &[&str]) -> Pair {
    pair_on("tcp://127.0.0.1:0").await.declare(subjects).await
}

struct Unbound {
    publisher: ZmqProvider,
    subscriber: ZmqProvider,
}

async fn pair_on(endpoint: &str) -> Unbound {
    let publisher = ZmqProvider::publisher(ZmqConfig::new(endpoint).with_settle(SETTLE))
        .await
        .unwrap();
    let subscriber =
        ZmqProvider::subscriber(ZmqConfig::new(publisher.endpoint()).with_settle(SETTLE)).unwrap();
    Unbound {
        publisher,
        subscriber,
    }
}

impl Unbound {
    async fn declare(self, subjects: &[&str]) -> Pair {
        // The same id on both sides, because the id is part of the wire topic
        // and two processes would have agreed on it.
        let declaration = Stream {
            id: STREAM.into(),
            name: "test".into(),
            subjects: subjects.iter().map(|s| s.to_string()).collect(),
            ..Stream::default()
        };
        self.publisher.create(declaration.clone()).await.unwrap();
        self.subscriber.create(declaration).await.unwrap();
        Pair {
            pm: self.publisher.bind(STREAM).await.unwrap(),
            sm: self.subscriber.bind(STREAM).await.unwrap(),
            publisher: self.publisher,
            subscriber: self.subscriber,
        }
    }
}

async fn next(subscription: &mut Subscription) -> streams::Message {
    tokio::time::timeout(WAIT, subscription.next())
        .await
        .expect("nothing was delivered")
        .expect("the subscription ended")
}

#[tokio::test(flavor = "multi_thread")]
async fn publishes_and_subscribes() {
    let p = pair(&[SUBJECT]).await;
    let mut sub = p.sm.subscribe(SUBJECT, &Options::default()).await.unwrap();

    let id =
        p.pm.publish(SUBJECT, event("ada"), &Options::default())
            .await
            .unwrap();

    let message = next(&mut sub).await;
    assert_eq!(message.id(), id);
    assert_eq!(message.subject(), SUBJECT);
    assert_eq!(message.json::<Event>().unwrap().user, "ada");
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn works_over_ipc() {
    let path = std::env::temp_dir().join(format!("tpp-streams-zmq-{}.sock", provider::new_id()));
    let endpoint = format!("ipc://{}", path.display());
    let p = pair_on(&endpoint).await.declare(&[SUBJECT]).await;
    let mut sub = p.sm.subscribe(SUBJECT, &Options::default()).await.unwrap();

    let id =
        p.pm.publish(SUBJECT, event("ada"), &Options::default())
            .await
            .unwrap();
    assert_eq!(next(&mut sub).await.id(), id);

    drop(sub);
    p.publisher.close().await.unwrap();
    let _ = std::fs::remove_file(path);
}

// The topic frame is what a SUB socket filters on, so a subscriber must not see
// another subject's messages.
#[tokio::test(flavor = "multi_thread")]
async fn subjects_are_isolated() {
    let p = pair(&["a", "b"]).await;
    let mut only_a = p.sm.subscribe("a", &Options::default()).await.unwrap();

    p.pm.publish("b", event("for b"), &Options::default())
        .await
        .unwrap();
    p.pm.publish("a", event("for a"), &Options::default())
        .await
        .unwrap();

    assert_eq!(
        next(&mut only_a).await.json::<Event>().unwrap().user,
        "for a"
    );
}

// ZeroMQ filters by prefix; a subject that extends another must still not leak
// into it. runtime-go relies on the filter alone and would deliver it.
#[tokio::test(flavor = "multi_thread")]
async fn a_longer_subject_is_not_a_match() {
    let p = pair(&["user", "users"]).await;
    let mut user = p.sm.subscribe("user", &Options::default()).await.unwrap();

    p.pm.publish("users", event("plural"), &Options::default())
        .await
        .unwrap();
    p.pm.publish("user", event("singular"), &Options::default())
        .await
        .unwrap();

    assert_eq!(
        next(&mut user).await.json::<Event>().unwrap().user,
        "singular"
    );
}

// PUB/SUB fans out: two subscribers each see every message, which is the
// behavior a group would have to change and cannot.
#[tokio::test(flavor = "multi_thread")]
async fn every_subscriber_sees_every_message() {
    let p = pair(&[SUBJECT]).await;
    let mut first = p.sm.subscribe(SUBJECT, &Options::default()).await.unwrap();
    let mut second = p.sm.subscribe(SUBJECT, &Options::default()).await.unwrap();

    let id =
        p.pm.publish(SUBJECT, event("ada"), &Options::default())
            .await
            .unwrap();

    assert_eq!(next(&mut first).await.id(), id);
    assert_eq!(next(&mut second).await.id(), id);
}

#[tokio::test(flavor = "multi_thread")]
async fn batches_publish_each_in_order() {
    let p = pair(&[SUBJECT]).await;
    let mut sub = p.sm.subscribe(SUBJECT, &Options::default()).await.unwrap();

    let batch = streams::as_batch(p.pm.as_ref()).unwrap();
    let ids = batch
        .publish_batch(
            SUBJECT,
            vec![event("a"), event("b"), event("c")],
            &Options::default(),
        )
        .await
        .unwrap();

    for id in &ids {
        assert_eq!(next(&mut sub).await.id(), id);
    }
    let refused = batch
        .publish_batch(
            SUBJECT,
            vec![event("a")],
            &Options::default().with_id("one"),
        )
        .await;
    assert!(
        matches!(refused, Err(StreamError::Unsupported(_))),
        "{refused:?}"
    );
}

// ZeroMQ keeps nothing, so it must refuse the durable half by name.
#[tokio::test(flavor = "multi_thread")]
async fn is_neither_durable_nor_positioned() {
    let p = pair(&[SUBJECT]).await;
    assert!(matches!(
        streams::as_durable(p.sm.as_ref()),
        Err(StreamError::Unsupported(_))
    ));
    assert!(matches!(
        streams::as_positioned(p.sm.as_ref()),
        Err(StreamError::Unsupported(_))
    ));
}

// A SUB socket cannot send, and refusing beats accepting a message that would
// go nowhere.
#[tokio::test(flavor = "multi_thread")]
async fn a_subscriber_cannot_publish() {
    let p = pair(&[SUBJECT]).await;
    let result = p.sm.publish(SUBJECT, event("x"), &Options::default()).await;
    assert!(
        matches!(result, Err(StreamError::Unsupported(_))),
        "{result:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_an_undeclared_subject() {
    let p = pair(&[SUBJECT]).await;
    let result = p.pm.publish("typo", event("x"), &Options::default()).await;
    assert!(
        matches!(result, Err(StreamError::UnknownSubject { .. })),
        "{result:?}"
    );
    let result = p.sm.subscribe("typo", &Options::default()).await;
    assert!(matches!(result, Err(StreamError::UnknownSubject { .. })));
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_a_ttl() {
    let p = pair(&[SUBJECT]).await;
    let opts = Options::default().with_ttl(Duration::from_secs(1));
    let result = p.pm.publish(SUBJECT, event("x"), &opts).await;
    assert!(
        matches!(result, Err(StreamError::Unsupported(_))),
        "{result:?}"
    );
}

// Fan-out is the topology, so sharing a subject is not something an option can
// ask for here, on either side.
#[tokio::test(flavor = "multi_thread")]
async fn rejects_a_group() {
    let p = pair(&[SUBJECT]).await;
    let opts = Options::default().with_group("workers");
    let result = p.pm.publish(SUBJECT, event("x"), &opts).await;
    assert!(
        matches!(result, Err(StreamError::Unsupported(_))),
        "{result:?}"
    );
    let result = p.sm.subscribe(SUBJECT, &opts).await;
    assert!(matches!(result, Err(StreamError::Unsupported(_))));
}

#[tokio::test(flavor = "multi_thread")]
async fn lifecycle() {
    let s = ZmqProvider::publisher(ZmqConfig::new("tcp://127.0.0.1:0"))
        .await
        .unwrap();
    let created = s
        .create(Stream {
            name: "users".into(),
            description: "before".into(),
            subjects: vec![SUBJECT.into()],
            ..Stream::default()
        })
        .await
        .unwrap();
    assert!(created.id.ends_with(":users"), "{}", created.id);
    assert!(created.active);

    s.update(
        &created.id,
        Stream {
            name: "users".into(),
            description: "after".into(),
            subjects: vec![SUBJECT.into()],
            ..Stream::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(s.get(&created.id).await.unwrap().description, "after");
    assert_eq!(s.list().await.unwrap().len(), 1);

    s.delete(&created.id).await.unwrap();
    assert!(matches!(
        s.get(&created.id).await,
        Err(StreamError::NotFound(_))
    ));
    assert!(matches!(
        s.delete(&created.id).await,
        Err(StreamError::NotFound(_))
    ));
    assert!(matches!(
        s.update(&created.id, Stream::default()).await,
        Err(StreamError::NotFound(_))
    ));
    s.close().await.unwrap();
    s.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_an_empty_endpoint() {
    assert!(ZmqProvider::publisher(ZmqConfig::default()).await.is_err());
    assert!(ZmqProvider::subscriber(ZmqConfig::default()).is_err());
}

// Close is how a provider releases the subscriptions it opened, so a stream
// the caller still holds must end rather than wait forever.
#[tokio::test(flavor = "multi_thread")]
async fn closing_ends_live_subscriptions() {
    let p = pair(&[SUBJECT]).await;
    let mut sub = p.sm.subscribe(SUBJECT, &Options::default()).await.unwrap();

    p.subscriber.close().await.unwrap();
    let ended = tokio::time::timeout(WAIT, sub.next())
        .await
        .expect("never ended");
    assert!(ended.is_none());
    assert!(p.sm.subscribe(SUBJECT, &Options::default()).await.is_err());

    p.publisher.close().await.unwrap();
    let result = p.pm.publish(SUBJECT, event("x"), &Options::default()).await;
    assert!(
        matches!(result, Err(StreamError::Internal(_))),
        "{result:?}"
    );
}

// Dropping a subscription closes its socket, and the publisher carries on
// serving the subscriptions that remain.
#[tokio::test(flavor = "multi_thread")]
async fn dropping_a_subscription_leaves_the_others() {
    let p = pair(&[SUBJECT]).await;
    let dropped = p.sm.subscribe(SUBJECT, &Options::default()).await.unwrap();
    let mut kept = p.sm.subscribe(SUBJECT, &Options::default()).await.unwrap();
    drop(dropped);
    tokio::time::sleep(SETTLE).await;

    let id =
        p.pm.publish(SUBJECT, event("ada"), &Options::default())
            .await
            .unwrap();
    assert_eq!(next(&mut kept).await.id(), id);
}

// The frames on the wire are runtime-go's, read by a plain SUB socket.
#[tokio::test(flavor = "multi_thread")]
async fn sends_the_go_wire_layout() {
    let p = pair(&[SUBJECT]).await;
    let mut raw = zeromq::SubSocket::new();
    raw.connect(p.publisher.endpoint()).await.unwrap();
    raw.subscribe("").await.unwrap();
    tokio::time::sleep(SETTLE).await;

    let payload = event("ada");
    let opts = Options::default().with_id("msg-1");
    p.pm.publish(SUBJECT, payload.clone(), &opts).await.unwrap();

    let frames = tokio::time::timeout(WAIT, raw.recv())
        .await
        .unwrap()
        .unwrap()
        .into_vec();
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0], Bytes::from(format!("{STREAM}|{SUBJECT}")));
    assert_eq!(frames[1], Bytes::from(provider::pack("msg-1", &payload)));
}

// A runtime-go publisher sends exactly these frames; a Rust subscriber reads
// them, and drops a payload that was not framed by the contract.
#[tokio::test(flavor = "multi_thread")]
async fn reads_the_go_wire_layout() {
    let mut raw = zeromq::PubSocket::new();
    let endpoint = raw.bind("tcp://127.0.0.1:0").await.unwrap().to_string();
    let subscriber = ZmqProvider::subscriber(ZmqConfig::new(endpoint).with_settle(SETTLE)).unwrap();
    subscriber
        .create(Stream {
            id: STREAM.into(),
            subjects: vec![SUBJECT.into()],
            ..Stream::default()
        })
        .await
        .unwrap();
    let mut sub = subscriber
        .bind(STREAM)
        .await
        .unwrap()
        .subscribe(SUBJECT, &Options::default())
        .await
        .unwrap();

    let topic = Bytes::from(format!("{STREAM}|{SUBJECT}"));
    let foreign = ZmqMessage::try_from(vec![topic.clone(), Bytes::from_static(b"not a frame")]);
    raw.send(foreign.unwrap()).await.unwrap();
    // Pack(streams.JSON, "id", "x") in runtime-go.
    let go = ZmqMessage::try_from(vec![
        topic,
        Bytes::from_static(b"\xB7\x01\x04json\x02id\"x\""),
    ]);
    raw.send(go.unwrap()).await.unwrap();

    let message = next(&mut sub).await;
    assert_eq!(message.id(), "id");
    assert_eq!(message.json::<String>().unwrap(), "x");
}

// --- headers, expiry, retain ---------------------------------------------------

fn headers<const N: usize>(pairs: [(&str, &str); N]) -> Headers {
    pairs.into_iter().collect()
}

/// A pair whose declarations both stamp `lang` and `site`, as two processes
/// that agreed on the stream would.
async fn headed_pair() -> Pair {
    let unbound = pair_on("tcp://127.0.0.1:0").await;
    let declaration = Stream::named("test")
        .with_id(STREAM)
        .subject(SUBJECT)
        .header("lang", "rust")
        .header("site", "plant-3");
    unbound.publisher.create(declaration.clone()).await.unwrap();
    unbound.subscriber.create(declaration).await.unwrap();
    Pair {
        pm: unbound.publisher.bind(STREAM).await.unwrap(),
        sm: unbound.subscriber.bind(STREAM).await.unwrap(),
        publisher: unbound.publisher,
        subscriber: unbound.subscriber,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_and_publish_headers_arrive() {
    let p = headed_pair().await;
    assert_eq!(
        p.publisher.get(STREAM).await.unwrap().headers,
        headers([("lang", "rust"), ("site", "plant-3")])
    );
    let mut sub = p.sm.subscribe(SUBJECT, &Options::default()).await.unwrap();

    p.pm.publish(SUBJECT, event("ada"), &Options::default())
        .await
        .unwrap();
    let got = next(&mut sub).await;
    assert_eq!(
        got.headers(),
        &headers([("lang", "rust"), ("site", "plant-3")])
    );
    assert_eq!(got.json::<Event>().unwrap().user, "ada");

    let opts = Options::default()
        .header("site", "plant-9")
        .header("trace", "t-1");
    let want = headers([("lang", "rust"), ("site", "plant-9"), ("trace", "t-1")]);
    p.pm.publish(SUBJECT, event("grace"), &opts).await.unwrap();
    assert_eq!(next(&mut sub).await.headers(), &want);

    let ids = streams::as_batch(p.pm.as_ref())
        .unwrap()
        .publish_batch(SUBJECT, vec![event("a"), event("b")], &opts)
        .await
        .unwrap();
    for id in ids {
        let got = next(&mut sub).await;
        assert_eq!(got.id(), id);
        assert_eq!(got.headers(), &want);
    }
}

/// Headers ride in a version-2 frame; without them the frame is still the
/// version-1 one runtime-go reads.
#[tokio::test(flavor = "multi_thread")]
async fn headers_ride_in_frame_version_2() {
    let p = pair(&[SUBJECT]).await;
    let mut raw = zeromq::SubSocket::new();
    raw.connect(p.publisher.endpoint()).await.unwrap();
    raw.subscribe("").await.unwrap();
    tokio::time::sleep(SETTLE).await;

    let payload = event("ada");
    let opts = Options::default().with_id("msg-1").header("lang", "rust");
    p.pm.publish(SUBJECT, payload.clone(), &opts).await.unwrap();
    let frames = tokio::time::timeout(WAIT, raw.recv())
        .await
        .unwrap()
        .unwrap()
        .into_vec();
    assert_eq!(frames[0], Bytes::from(format!("{STREAM}|{SUBJECT}")));
    assert_eq!(frames[1][1], 2);
    assert_eq!(
        frames[1],
        Bytes::from(provider::pack_with("msg-1", &payload, &opts.headers))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_headers_are_refused_where_they_are_set() {
    let invalid = |result: streams::Result<Stream>| {
        assert!(
            matches!(result, Err(StreamError::InvalidHeader(_))),
            "{result:?}"
        );
    };
    let p = pair(&[SUBJECT]).await;
    invalid(
        p.publisher
            .create(Stream::named("x").subject(SUBJECT).header("a:b", "1"))
            .await,
    );
    invalid(
        p.subscriber
            .create(Stream::named("x").subject(SUBJECT).header("", "1"))
            .await,
    );
    invalid(
        p.publisher
            .update(STREAM, Stream::named("x").header("bad key", "1"))
            .await,
    );
    let bad = Options::default().header("k", "line\nbreak");
    let publish = p.pm.publish(SUBJECT, event("ada"), &bad).await;
    assert!(
        matches!(publish, Err(StreamError::InvalidHeader(_))),
        "{publish:?}"
    );
    let batch = streams::as_batch(p.pm.as_ref())
        .unwrap()
        .publish_batch(SUBJECT, vec![event("ada")], &bad)
        .await;
    assert!(
        matches!(batch, Err(StreamError::InvalidHeader(_))),
        "{batch:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn expiry_and_retain_are_refused() {
    let p = pair(&[SUBJECT]).await;
    let batch = streams::as_batch(p.pm.as_ref()).unwrap();
    for opts in [
        Options::default().with_expiry(Duration::from_secs(5)),
        Options::default().with_retain(),
    ] {
        let one = p.pm.publish(SUBJECT, event("ada"), &opts).await;
        assert!(matches!(one, Err(StreamError::Unsupported(_))), "{one:?}");
        let many = batch
            .publish_batch(SUBJECT, vec![event("ada")], &opts)
            .await;
        assert!(matches!(many, Err(StreamError::Unsupported(_))), "{many:?}");
    }
    // A zero expiry is no expiry.
    p.pm.publish(
        SUBJECT,
        event("ada"),
        &Options::default().with_expiry(Duration::ZERO),
    )
    .await
    .unwrap();
}
