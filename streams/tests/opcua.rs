//! The OPC UA provider against an async-opcua server started inside the test
//! process, so it runs in plain `cargo test --features opcua`.
//!
//! The server exposes one writable variable per type the tests use, plus a
//! read-only one, in its own namespace. Every test starts its own server on
//! its own port, so tests run in parallel and the restart test can stop one
//! without disturbing the others.

use std::{net::TcpListener as StdListener, path::PathBuf, sync::Arc, time::Duration};

use futures::StreamExt;
use opcua::{
    client::{ClientBuilder, IdentityToken, Session},
    crypto::SecurityPolicy,
    nodes::{AccessLevel, VariableBuilder},
    server::{
        ANONYMOUS_USER_TOKEN_ID, ServerBuilder, ServerHandle,
        diagnostics::NamespaceMetadata,
        node_manager::memory::{SimpleNodeManager, simple_node_manager},
    },
    types::{
        DataTypeId, DataValue, DateTime, Identifier, MessageSecurityMode, NodeId, ObjectId,
        StatusCode, UserTokenPolicy, Variant,
    },
};
use streams::{
    Message, Options, Payload, Stream, StreamError, Streams, as_batch, as_durable, as_positioned,
    drivers::opcua::{CODEC_NAME, OpcUaConfig, OpcUaProvider, OpcUaValue},
};
use tokio::task::JoinHandle;

const NAMESPACE: &str = "urn:tpp-streams:test";
const WAIT: Duration = Duration::from_secs(10);

/// The variables the server exposes: name, data type, initial value.
fn variables() -> Vec<(&'static str, DataTypeId, Variant)> {
    vec![
        ("Temperature", DataTypeId::Double, 20.0f64.into()),
        ("Count", DataTypeId::Int32, 0i32.into()),
        ("Running", DataTypeId::Boolean, false.into()),
        ("Mode", DataTypeId::String, "idle".into()),
        ("Sequence", DataTypeId::Int64, 0i64.into()),
    ]
}

/// A server on `port`, running until its handle is cancelled.
struct TestServer {
    handle: ServerHandle,
    task: JoinHandle<()>,
    port: u16,
}

impl TestServer {
    async fn start() -> Self {
        let port = StdListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        Self::start_on(port).await
    }

    async fn start_on(port: u16) -> Self {
        let pki = scratch(&format!("server-{port}"));
        let mut builder = ServerBuilder::new()
            .application_name("tpp-streams test server")
            .application_uri("urn:tpp-streams:test-server")
            .product_uri("urn:tpp-streams:test-server")
            .host("127.0.0.1")
            .port(port)
            .discovery_urls(vec![format!("opc.tcp://127.0.0.1:{port}/")])
            .pki_dir(pki)
            // A None endpoint needs no keys, and generating RSA keys in a
            // debug build costs seconds per server.
            .create_sample_keypair(false)
            .trust_client_certs(true)
            .add_endpoint(
                "none",
                (
                    "/",
                    SecurityPolicy::None,
                    MessageSecurityMode::None,
                    &[ANONYMOUS_USER_TOKEN_ID] as &[&str],
                ),
            )
            .with_node_manager(simple_node_manager(
                NamespaceMetadata {
                    namespace_uri: NAMESPACE.to_owned(),
                    ..Default::default()
                },
                "tpp-streams",
            ));
        // async-opcua samples a monitored node by source timestamp, at no
        // less than this; at its default of 100 ms, writes closer together
        // than that are folded into one sample. Zero reports every write.
        // Its queue limits are small by default too: 10 values per item.
        let limits = &mut builder.limits_mut().subscriptions;
        limits.min_sampling_interval_ms = 0.0;
        limits.max_monitored_item_queue_size = 10_000;
        limits.max_queued_notifications = 100_000;
        let (server, handle) = builder.build().expect("build the server");
        let ns = handle.get_namespace_index(NAMESPACE).unwrap();
        let manager = handle
            .node_managers()
            .get_of_type::<SimpleNodeManager>()
            .unwrap();
        {
            let mut space = manager.address_space().write();
            for (name, data_type, value) in variables() {
                VariableBuilder::new(&NodeId::new(ns, name), name, name)
                    .data_type(data_type)
                    .value(value)
                    .writable()
                    .organized_by(ObjectId::ObjectsFolder)
                    .insert(&mut *space);
            }
            VariableBuilder::new(&NodeId::new(ns, "Serial"), "Serial", "Serial")
                .data_type(DataTypeId::String)
                .value("SN-1")
                .access_level(AccessLevel::CURRENT_READ)
                .user_access_level(AccessLevel::CURRENT_READ)
                .organized_by(ObjectId::ObjectsFolder)
                .insert(&mut *space);
        }
        let task = tokio::spawn(async move {
            server.run().await.expect("server runs");
        });
        // Wait until it accepts connections.
        for _ in 0..100 {
            if tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Self { handle, task, port }
    }

    fn url(&self) -> String {
        format!("opc.tcp://127.0.0.1:{}/", self.port)
    }

    async fn stop(self) -> u16 {
        self.handle.cancel();
        let _ = tokio::time::timeout(WAIT, self.task).await;
        self.port
    }

    /// Monitored items the server holds for `session`, across its
    /// subscriptions, and how many subscriptions that is.
    fn monitored(&self, session: &Session) -> (usize, usize) {
        let Identifier::Numeric(id) = session.server_session_id().identifier else {
            panic!("numeric session id");
        };
        let Some(subs) = self.handle.subscriptions().get_session_subscriptions(id) else {
            return (0, 0);
        };
        let subs = subs.lock();
        let ids = subs.subscription_ids();
        let items = ids
            .iter()
            .filter_map(|i| subs.get(*i))
            .map(|s| s.len())
            .sum();
        (ids.len(), items)
    }
}

/// A directory under the target dir for one test's PKI files.
fn scratch(name: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join("opcua")
        .join(name);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A connected anonymous session over SecurityMode None, with its event loop
/// running.
async fn connect(url: &str) -> (Arc<Session>, JoinHandle<StatusCode>) {
    let port = url.rsplit(':').next().unwrap().trim_end_matches('/');
    let mut client = ClientBuilder::new()
        .application_name("tpp-streams test client")
        .application_uri("urn:tpp-streams:test-client")
        .pki_dir(scratch(&format!("client-{port}")))
        .create_sample_keypair(false)
        .trust_server_certs(true)
        .session_retry_initial(Duration::from_millis(100))
        .session_retry_max(Duration::from_millis(500))
        .session_retry_limit(-1)
        .request_timeout(Duration::from_secs(5))
        .client()
        .expect("build the client");
    let endpoint = (
        url,
        SecurityPolicy::None.to_str(),
        MessageSecurityMode::None,
        UserTokenPolicy::anonymous(),
    );
    let (session, event_loop) = client
        .connect_to_matching_endpoint(endpoint, IdentityToken::Anonymous)
        .await
        .expect("connect");
    let task = event_loop.spawn();
    assert!(
        tokio::time::timeout(WAIT, session.wait_for_connection())
            .await
            .expect("connected in time")
    );
    (session, task)
}

/// A server, a session to it, and a provider over that session.
async fn setup() -> (TestServer, Arc<Session>, OpcUaProvider, u16) {
    setup_with(OpcUaConfig::default()).await
}

async fn setup_with(config: OpcUaConfig) -> (TestServer, Arc<Session>, OpcUaProvider, u16) {
    let server = TestServer::start().await;
    let (session, _) = connect(&server.url()).await;
    let ns = server.handle.get_namespace_index(NAMESPACE).unwrap();
    let provider = OpcUaProvider::new(session.clone(), config);
    (server, session, provider, ns)
}

fn node(ns: u16, name: &str) -> String {
    format!("ns={ns};s={name}")
}

/// Declares a stream over every variable and binds it.
async fn bound(provider: &OpcUaProvider, ns: u16) -> (Stream, Arc<dyn streams::Manager>) {
    let mut subjects: Vec<String> = variables().iter().map(|(n, ..)| node(ns, n)).collect();
    subjects.push(node(ns, "Serial"));
    let stream = provider
        .create(Stream {
            name: "plant".into(),
            subjects,
            ..Stream::default()
        })
        .await
        .unwrap();
    let manager = provider.bind(&stream.id).await.unwrap();
    (stream, manager)
}

async fn next(sub: &mut streams::Subscription) -> Message {
    tokio::time::timeout(WAIT, sub.next())
        .await
        .expect("a notification in time")
        .expect("the stream is open")
}

async fn eventually(what: &str, mut check: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + WAIT;
    while !check() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn lifecycle() {
    let (_server, _session, provider, ns) = setup().await;
    let temp = node(ns, "Temperature");

    let created = provider
        .create(Stream {
            name: "plant".into(),
            description: "line 1".into(),
            subjects: vec![temp.clone()],
            ..Stream::default()
        })
        .await
        .unwrap();
    assert!(created.id.ends_with(":plant"), "{}", created.id);
    assert!(created.active);
    assert_eq!(provider.get(&created.id).await.unwrap(), created);

    let named = provider
        .create(Stream {
            id: "line-2".into(),
            subjects: vec![node(ns, "Count")],
            ..Stream::default()
        })
        .await
        .unwrap();
    let ids: Vec<String> = provider
        .list()
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.id)
        .collect();
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&created.id) && ids.contains(&named.id));

    let updated = provider
        .update(
            &named.id,
            Stream {
                subjects: vec![node(ns, "Count"), node(ns, "Running")],
                ..named.clone()
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.subjects.len(), 2);
    assert_eq!(provider.get(&named.id).await.unwrap(), updated);

    // A subject that is not a node id fails at declaration, not at first use.
    let error = provider
        .create(Stream {
            subjects: vec!["orders.placed".into()],
            ..Stream::default()
        })
        .await
        .unwrap_err();
    assert!(matches!(error, StreamError::Unsupported(_)), "{error}");
    let error = provider
        .update(
            &named.id,
            Stream {
                subjects: vec!["nope".into()],
                ..Stream::default()
            },
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("node id"), "{error}");

    let manager = provider.bind(&created.id).await.unwrap();
    let error = manager
        .publish(
            &node(ns, "Count"),
            OpcUaValue::payload(1i32),
            &Options::default(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, StreamError::UnknownSubject { .. }),
        "{error}"
    );
    let error = manager
        .subscribe("ns=9;s=Nowhere", &Options::default())
        .await
        .err()
        .unwrap();
    assert!(
        matches!(error, StreamError::UnknownSubject { .. }),
        "{error}"
    );

    provider.delete(&created.id).await.unwrap();
    assert!(matches!(
        provider.get(&created.id).await,
        Err(StreamError::NotFound(_))
    ));
    assert!(matches!(
        provider.delete(&created.id).await,
        Err(StreamError::NotFound(_))
    ));
    assert!(matches!(
        provider.bind(&created.id).await.err().unwrap(),
        StreamError::NotFound(_)
    ));
    assert_eq!(provider.list().await.unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_values_round_trip() {
    let (_server, _session, provider, ns) = setup().await;
    let (_, manager) = bound(&provider, ns).await;
    let cases: Vec<(&str, Variant)> = vec![
        ("Temperature", 21.5f64.into()),
        ("Count", (-42i32).into()),
        ("Running", true.into()),
        ("Mode", "cooling".into()),
        ("Sequence", i64::MAX.into()),
    ];
    let mut subs = Vec::new();
    for (name, _) in &cases {
        subs.push(
            manager
                .subscribe(&node(ns, name), &Options::default())
                .await
                .unwrap(),
        );
    }
    for (name, value) in &cases {
        let id = manager
            .publish(
                &node(ns, name),
                OpcUaValue::payload(value.clone()),
                &Options::default(),
            )
            .await
            .unwrap();
        assert_eq!(id.len(), 26, "a ULID: {id}");
    }
    for ((name, value), sub) in cases.iter().zip(&mut subs) {
        let message = next(sub).await;
        assert_eq!(message.subject(), node(ns, name));
        assert_eq!(message.codec(), CODEC_NAME);
        assert_eq!(message.id().len(), 26);
        let got: Variant = message.decode(&OpcUaValue).unwrap();
        assert_eq!(&got, value, "{name}");
        let dv = OpcUaValue::data_value(&message).unwrap();
        assert!(dv.status().is_good());
        assert!(dv.server_timestamp.is_some() && dv.source_timestamp.is_some());
    }

    // A caller's id is the receipt; OPC UA carries no id, so it does not travel.
    let id = manager
        .publish(
            &node(ns, "Count"),
            OpcUaValue::payload(7i32),
            &Options::default().with_id("mine"),
        )
        .await
        .unwrap();
    assert_eq!(id, "mine");
    let message = next(&mut subs[1]).await;
    assert_ne!(message.id(), "mine");

    // Writing the same value again is still a message.
    manager
        .publish(
            &node(ns, "Count"),
            OpcUaValue::payload(7i32),
            &Options::default(),
        )
        .await
        .unwrap();
    let again: Variant = next(&mut subs[1]).await.decode(&OpcUaValue).unwrap();
    assert_eq!(again, Variant::from(7i32));
}

#[tokio::test(flavor = "multi_thread")]
async fn status_and_timestamps_survive() {
    let (_server, _session, provider, ns) = setup().await;
    let (_, manager) = bound(&provider, ns).await;
    let temp = node(ns, "Temperature");
    let mut sub = manager.subscribe(&temp, &Options::default()).await.unwrap();
    let source = DateTime::ymd_hms(2026, 3, 14, 15, 9, 26);
    let written = DataValue {
        value: Some(19.25f64.into()),
        status: Some(StatusCode::UncertainLastUsableValue),
        source_timestamp: Some(source),
        ..DataValue::default()
    };
    manager
        .publish(&temp, OpcUaValue::payload_of(&written), &Options::default())
        .await
        .unwrap();
    let got = OpcUaValue::data_value(&next(&mut sub).await).unwrap();
    assert_eq!(got.value, written.value);
    assert_eq!(got.status, Some(StatusCode::UncertainLastUsableValue));
    assert_eq!(got.source_timestamp, Some(source));
    let server_ts = got.server_timestamp.expect("the server stamps it");
    assert!(server_ts > source);

    // A Bad quality written by a device reaches the subscriber as Bad.
    let failed = DataValue {
        value: Some(0.0f64.into()),
        status: Some(StatusCode::BadSensorFailure),
        ..DataValue::default()
    };
    manager
        .publish(&temp, OpcUaValue::payload_of(&failed), &Options::default())
        .await
        .unwrap();
    let got = OpcUaValue::data_value(&next(&mut sub).await).unwrap();
    assert_eq!(got.status, Some(StatusCode::BadSensorFailure));
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_writes_name_the_status() {
    let (_server, _session, provider, ns) = setup().await;
    let (_, manager) = bound(&provider, ns).await;
    let error = manager
        .publish(
            &node(ns, "Count"),
            OpcUaValue::payload(vec!["not", "a", "number"]),
            &Options::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, StreamError::Codec(_)), "{error}");
    assert!(error.to_string().contains("BadTypeMismatch"), "{error}");

    let error = manager
        .publish(
            &node(ns, "Serial"),
            OpcUaValue::payload("SN-2"),
            &Options::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, StreamError::Unsupported(_)), "{error}");
    assert!(error.to_string().contains("BadUserAccessDenied"), "{error}");
    assert!(error.is_settled());
}

#[tokio::test(flavor = "multi_thread")]
async fn batch_is_one_write() {
    let (_server, _session, provider, ns) = setup().await;
    let (_, manager) = bound(&provider, ns).await;
    let seq = node(ns, "Sequence");
    let mut sub = manager
        .subscribe(&seq, &Options::default().with_prefetch(100))
        .await
        .unwrap();
    let batch = as_batch(manager.as_ref()).unwrap();
    let payloads: Vec<Payload> = (1..=50i64).map(OpcUaValue::payload).collect();
    let ids = batch
        .publish_batch(&seq, payloads, &Options::default())
        .await
        .unwrap();
    assert_eq!(ids.len(), 50);
    for want in 1..=50i64 {
        let got: Variant = next(&mut sub).await.decode(&OpcUaValue).unwrap();
        assert_eq!(got, Variant::from(want));
    }

    // One bad entry fails alone, and every id is still returned.
    let payloads = vec![
        OpcUaValue::payload(1i64),
        OpcUaValue::payload(vec!["not", "a", "number"]),
        Payload::json(&3).unwrap(),
        OpcUaValue::payload(4i64),
    ];
    match batch
        .publish_batch(&seq, payloads, &Options::default())
        .await
    {
        Err(StreamError::PartialBatch { ids, failures, .. }) => {
            assert_eq!(ids.len(), 4);
            let failed: Vec<usize> = failures.iter().map(|(i, _)| *i).collect();
            assert_eq!(failed, [1, 2]);
            assert!(failures[0].1.to_string().contains("BadTypeMismatch"));
        }
        other => panic!("{other:?}"),
    }
    let error = batch
        .publish_batch(&seq, vec![], &Options::default().with_id("x"))
        .await
        .unwrap_err();
    assert!(matches!(error, StreamError::Unsupported(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn refusals() {
    let (_server, _session, provider, ns) = setup().await;
    let (_, manager) = bound(&provider, ns).await;
    let temp = node(ns, "Temperature");
    let unsupported = |r: Result<(), StreamError>, what: &str| match r {
        Err(StreamError::Unsupported(text)) => text,
        other => panic!("{what}: {other:?}"),
    };
    let text = unsupported(
        manager
            .subscribe(&temp, &Options::default().with_group("g"))
            .await
            .map(|_| ()),
        "group",
    );
    assert!(text.contains("consumer groups"), "{text}");
    let text = unsupported(
        manager
            .publish(
                &temp,
                OpcUaValue::payload(1.0f64),
                &Options::default().with_ttl(Duration::from_secs(5)),
            )
            .await
            .map(|_| ()),
        "ttl",
    );
    assert!(text.contains("schedule"), "{text}");
    let text = unsupported(
        manager
            .publish(&temp, Payload::json(&1.0).unwrap(), &Options::default())
            .await
            .map(|_| ()),
        "json payload",
    );
    assert!(text.contains("\"json\""), "{text}");
    assert!(matches!(
        as_durable(manager.as_ref()),
        Err(StreamError::Unsupported(_))
    ));
    assert!(matches!(
        as_positioned(manager.as_ref()),
        Err(StreamError::Unsupported(_))
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_a_subscription_deletes_the_monitored_item() {
    let (server, session, provider, ns) = setup().await;
    let (_, manager) = bound(&provider, ns).await;
    let a = manager
        .subscribe(&node(ns, "Temperature"), &Options::default())
        .await
        .unwrap();
    let b = manager
        .subscribe(&node(ns, "Count"), &Options::default())
        .await
        .unwrap();
    assert_eq!(provider.monitored_items(), 2);
    assert_eq!(
        server.monitored(&session),
        (1, 2),
        "one subscription, two items"
    );

    drop(a);
    eventually("the first item to go", || {
        server.monitored(&session) == (1, 1)
    })
    .await;
    assert_eq!(provider.monitored_items(), 1);

    drop(b);
    eventually("the subscription to go", || {
        server.monitored(&session) == (0, 0)
    })
    .await;
    assert_eq!(provider.monitored_items(), 0);
    assert!(
        session
            .subscription_state
            .lock()
            .subscription_ids()
            .is_none_or(|ids| ids.is_empty())
    );

    // And a new subscribe makes a new one.
    let mut c = manager
        .subscribe(&node(ns, "Count"), &Options::default())
        .await
        .unwrap();
    assert_eq!(server.monitored(&session), (1, 1));
    manager
        .publish(
            &node(ns, "Count"),
            OpcUaValue::payload(5i32),
            &Options::default(),
        )
        .await
        .unwrap();
    let got: Variant = next(&mut c).await.decode(&OpcUaValue).unwrap();
    assert_eq!(got, Variant::from(5i32));

    // Close ends the streams and deletes the subscription; the session stays.
    provider.close().await.unwrap();
    assert!(
        tokio::time::timeout(WAIT, c.next())
            .await
            .unwrap()
            .is_none()
    );
    eventually("close to delete it", || {
        server.monitored(&session) == (0, 0)
    })
    .await;
    assert!(session.read_namespace_array().await.is_ok());
}

#[tokio::test(flavor = "multi_thread")]
async fn initial_value_on_request() {
    let (_server, _session, provider, ns) = setup_with(OpcUaConfig {
        initial_value: true,
        ..OpcUaConfig::default()
    })
    .await;
    let (_, manager) = bound(&provider, ns).await;
    let mut sub = manager
        .subscribe(&node(ns, "Mode"), &Options::default())
        .await
        .unwrap();
    let got: Variant = next(&mut sub).await.decode(&OpcUaValue).unwrap();
    assert_eq!(got, Variant::from("idle"));
}

#[tokio::test(flavor = "multi_thread")]
async fn namespace_uris_resolve_at_bind() {
    let (_server, _session, provider, _ns) = setup().await;
    let subject = format!("nsu={NAMESPACE};s=Count");
    let stream = provider
        .create(Stream {
            subjects: vec![subject.clone(), "nsu=urn:nobody;s=x".into()],
            ..Stream::default()
        })
        .await
        .unwrap();
    // The server has no urn:nobody, so binding says so.
    assert!(provider.bind(&stream.id).await.is_err());
    provider
        .update(
            &stream.id,
            Stream {
                subjects: vec![subject.clone()],
                ..stream.clone()
            },
        )
        .await
        .unwrap();
    let manager = provider.bind(&stream.id).await.unwrap();
    let mut sub = manager
        .subscribe(&subject, &Options::default())
        .await
        .unwrap();
    manager
        .publish(&subject, OpcUaValue::payload(9i32), &Options::default())
        .await
        .unwrap();
    let message = next(&mut sub).await;
    assert_eq!(message.subject(), subject);
    assert_eq!(
        message.decode::<Variant, _>(&OpcUaValue).unwrap(),
        Variant::from(9i32)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn subscriptions_survive_a_server_restart() {
    let (server, session, provider, ns) = setup().await;
    let (_, manager) = bound(&provider, ns).await;
    let count = node(ns, "Count");
    let mut sub = manager
        .subscribe(&count, &Options::default())
        .await
        .unwrap();
    manager
        .publish(&count, OpcUaValue::payload(1i32), &Options::default())
        .await
        .unwrap();
    assert_eq!(
        next(&mut sub)
            .await
            .decode::<Variant, _>(&OpcUaValue)
            .unwrap(),
        Variant::from(1i32)
    );

    // The server goes away with every session and subscription it had.
    let port = server.stop().await;
    let error = manager
        .publish(&count, OpcUaValue::payload(2i32), &Options::default())
        .await;
    assert!(
        error.is_err(),
        "a write with no server fails rather than waits"
    );

    let server = TestServer::start_on(port).await;
    assert_eq!(server.handle.get_namespace_index(NAMESPACE), Some(ns));
    // The session reconnects and recreates the subscription and its item.
    eventually("the subscription to be recreated", || {
        server.monitored(&session) == (1, 1)
    })
    .await;
    // The new server reports the item's value, and after a recreate that is
    // delivered: it is the state written while the session was gone.
    let initial: Variant = next(&mut sub).await.decode(&OpcUaValue).unwrap();
    assert_eq!(initial, Variant::from(0i32));

    manager
        .publish(&count, OpcUaValue::payload(3i32), &Options::default())
        .await
        .unwrap();
    let got: Variant = next(&mut sub).await.decode(&OpcUaValue).unwrap();
    assert_eq!(got, Variant::from(3i32));

    // The item recreated on the new server is still the one a drop deletes.
    drop(sub);
    eventually("the recreated subscription to go", || {
        server.monitored(&session) == (0, 0)
    })
    .await;
}
