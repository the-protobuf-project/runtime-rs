//! The OPC UA provider against Microsoft's OPC PLC simulator, the OPC
//! Foundation's .NET stack, from docker/compose.opcua.yaml:
//!
//! ```text
//! docker compose -f streams/docker/compose.opcua.yaml up -d --wait
//! cargo test -p tpp-streams --features opcua,live-tests --test opcua_live
//! STREAMS_E2E_RESTART=1 cargo test -p tpp-streams --features opcua,live-tests --test opcua_live
//! ```
//!
//! SecurityPolicy None and an anonymous user; tests/opcua_e2e.rs is the
//! secured equivalent. The nodes written are compose.opcua.nodes.json's, in
//! opc-plc's namespace, named here by URI. `OPCUA_URL` (default
//! `opc.tcp://127.0.0.1:14840`) and `OPCUA_CONTAINER` (default
//! `tpp-streams-opcua`) point it elsewhere. The restart test runs only with
//! `STREAMS_E2E_RESTART=1`, because it restarts that container.
//!
//! Every test holds one lock: they write the same nodes, and the restart test
//! takes the server away from everything else.

use std::{process::Command, sync::Arc, time::Duration};

use futures::StreamExt;
use opcua::{
    client::{ClientBuilder, IdentityToken, Session, UARequest, services::DeleteMonitoredItems},
    crypto::SecurityPolicy,
    types::{DataValue, DateTime, MessageSecurityMode, StatusCode, UserTokenPolicy, Variant},
};
use streams::{
    Manager, Message, Options, Payload, Stream, StreamError, Streams, Subscription, as_batch,
    as_durable, as_positioned,
    drivers::opcua::{CODEC_NAME, OpcUaConfig, OpcUaProvider, OpcUaValue},
};
use tokio::{sync::Mutex, time::timeout};

const NAMESPACE: &str = "http://microsoft.com/Opc/OpcPlc/";
const WAIT: Duration = Duration::from_secs(15);

static SERIAL: Mutex<()> = Mutex::const_new(());

/// The simulator's endpoint, `OPCUA_URL` or the compose default.
fn url() -> String {
    std::env::var("OPCUA_URL").unwrap_or_else(|_| "opc.tcp://127.0.0.1:14840".into())
}

/// The simulator's container name, for tests that restart it.
fn container() -> String {
    std::env::var("OPCUA_CONTAINER").unwrap_or_else(|_| "tpp-streams-opcua".into())
}

/// A node id by namespace URI, which stays stable when the server renumbers namespaces.
fn node(name: &str) -> String {
    format!("nsu={NAMESPACE};s={name}")
}

/// An anonymous session with no security that retries for ever, failing
/// if it is not connected within [`WAIT`].
async fn connect() -> Arc<Session> {
    let mut client = ClientBuilder::new()
        .application_name("tpp-streams live tests")
        .application_uri("urn:tpp-streams:live-tests")
        .pki_dir(std::env::temp_dir().join("tpp-streams-opcua-live"))
        .create_sample_keypair(false)
        .trust_server_certs(true)
        .session_retry_initial(Duration::from_millis(200))
        .session_retry_max(Duration::from_secs(1))
        .session_retry_limit(-1)
        .request_timeout(Duration::from_secs(5))
        .client()
        .expect("build the client");
    let (session, event_loop) = client
        .connect_to_matching_endpoint(
            (
                url().as_str(),
                SecurityPolicy::None.to_str(),
                MessageSecurityMode::None,
                UserTokenPolicy::anonymous(),
            ),
            IdentityToken::Anonymous,
        )
        .await
        .unwrap_or_else(|e| panic!("connect to {}: {e}; is compose.opcua.yaml up?", url()));
    event_loop.spawn();
    assert!(timeout(WAIT, session.wait_for_connection()).await.unwrap());
    session
}

const NODES: [&str; 6] = [
    "Temperature",
    "Count",
    "Running",
    "Mode",
    "Sequence",
    "Serial",
];

/// A session, a provider on it, and a manager bound to a stream over [`NODES`].
async fn setup() -> (Arc<Session>, OpcUaProvider, Arc<dyn Manager>) {
    let session = connect().await;
    let provider = OpcUaProvider::new(session.clone(), OpcUaConfig::default());
    let stream = provider
        .create(Stream {
            name: "plc".into(),
            subjects: NODES.iter().map(|n| node(n)).collect(),
            ..Stream::default()
        })
        .await
        .unwrap();
    let manager = provider.bind(&stream.id).await.unwrap();
    (session, provider, manager)
}

/// The next notification within [`WAIT`].
async fn next(sub: &mut Subscription) -> Message {
    timeout(WAIT, sub.next())
        .await
        .expect("a notification in time")
        .expect("the stream is open")
}

/// The next notification's value within [`WAIT`].
async fn value(sub: &mut Subscription) -> Variant {
    next(sub).await.decode(&OpcUaValue).unwrap()
}

/// Whether the server still has monitored item `item` in `subscription`,
/// asked of the server itself rather than of the client's bookkeeping.
async fn server_has(session: &Session, subscription: u32, item: u32) -> StatusCode {
    let response = DeleteMonitoredItems::new(subscription, session)
        .items_to_delete(vec![item])
        .send(session.channel())
        .await;
    match response {
        Ok(r) => r
            .results
            .unwrap_or_default()
            .first()
            .copied()
            .unwrap_or(StatusCode::Good),
        Err(e) => e.status(),
    }
}

/// The server ids of every monitored item the session's client state holds.
fn items(session: &Session) -> Vec<(u32, u32)> {
    let state = session.subscription_state.lock();
    state
        .subscription_ids()
        .unwrap_or_default()
        .into_iter()
        .flat_map(|s| {
            state
                .get(s)
                .map(|sub| {
                    sub.monitored_items()
                        .map(|m| (s, m.id()))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn lifecycle() {
    let _serial = SERIAL.lock().await;
    let session = connect().await;
    let provider = OpcUaProvider::new(session, OpcUaConfig::default());
    let stream = provider
        .create(Stream {
            name: "plc".into(),
            subjects: vec![node("Count")],
            ..Stream::default()
        })
        .await
        .unwrap();
    assert_eq!(provider.get(&stream.id).await.unwrap(), stream);
    assert_eq!(provider.list().await.unwrap(), vec![stream.clone()]);
    let updated = provider
        .update(
            &stream.id,
            Stream {
                subjects: vec![node("Count"), node("Mode")],
                ..stream.clone()
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.subjects.len(), 2);
    let manager = provider.bind(&stream.id).await.unwrap();
    assert!(matches!(
        manager
            .publish(
                &node("Running"),
                OpcUaValue::payload(true),
                &Options::default()
            )
            .await,
        Err(StreamError::UnknownSubject { .. })
    ));
    // A namespace the server does not have fails the bind, by name.
    let stranger = provider
        .create(Stream {
            subjects: vec!["nsu=urn:nobody;s=x".into()],
            ..Stream::default()
        })
        .await
        .unwrap();
    let error = provider.bind(&stranger.id).await.err().unwrap();
    assert!(error.to_string().contains("urn:nobody"), "{error}");
    provider.delete(&stream.id).await.unwrap();
    provider.delete(&stranger.id).await.unwrap();
    assert!(matches!(
        provider.get(&stream.id).await,
        Err(StreamError::NotFound(_))
    ));
    assert!(provider.list().await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_values_round_trip() {
    let _serial = SERIAL.lock().await;
    let (_session, _provider, manager) = setup().await;
    let cases: Vec<(&str, Variant)> = vec![
        ("Temperature", 21.5f64.into()),
        ("Count", (-42i32).into()),
        ("Running", true.into()),
        ("Mode", "cooling".into()),
        ("Sequence", i64::MIN.into()),
    ];
    let mut subs = Vec::new();
    for (name, _) in &cases {
        subs.push(
            manager
                .subscribe(&node(name), &Options::default())
                .await
                .unwrap(),
        );
    }
    let before = DateTime::now();
    for (name, v) in &cases {
        manager
            .publish(
                &node(name),
                OpcUaValue::payload(v.clone()),
                &Options::default(),
            )
            .await
            .unwrap();
    }
    for ((name, v), sub) in cases.iter().zip(&mut subs) {
        let message = next(sub).await;
        assert_eq!(message.subject(), node(name));
        assert_eq!(message.codec(), CODEC_NAME);
        let dv = OpcUaValue::data_value(&message).unwrap();
        assert_eq!(dv.value.as_ref(), Some(v), "{name}");
        assert!(dv.status().is_good(), "{name}: {}", dv.status());
        // The .NET stack stamps a write with both times when it carries none.
        let source = dv.source_timestamp.expect("a source timestamp");
        let server = dv.server_timestamp.expect("a server timestamp");
        let ms = |t: DateTime| t.as_chrono().timestamp_millis();
        // A second's allowance for the container's clock.
        assert!(ms(source) + 1000 >= ms(before), "{name}: {source:?}");
        assert!(ms(server) + 1000 >= ms(before), "{name}: {server:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_writes_name_the_status() {
    let _serial = SERIAL.lock().await;
    let (_session, _provider, manager) = setup().await;
    let error = manager
        .publish(
            &node("Count"),
            OpcUaValue::payload("forty-two"),
            &Options::default(),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, StreamError::Codec(_)), "{error}");
    assert!(error.to_string().contains("BadTypeMismatch"), "{error}");

    let error = manager
        .publish(
            &node("Serial"),
            OpcUaValue::payload("SN-2"),
            &Options::default(),
        )
        .await
        .unwrap_err();
    assert!(error.is_settled(), "{error}");
    let text = error.to_string();
    assert!(
        text.contains("BadNotWritable") || text.contains("BadUserAccessDenied"),
        "{text}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn status_and_timestamps_survive() {
    let _serial = SERIAL.lock().await;
    let (_session, _provider, manager) = setup().await;
    let temp = node("Temperature");
    let mut sub = manager.subscribe(&temp, &Options::default()).await.unwrap();
    // opc-plc's user nodes accept a write that carries its own quality and
    // source time, as a gateway forwarding a device's reading would send.
    let source = DateTime::ymd_hms(2026, 3, 14, 15, 9, 26);
    // A value of its own, since the node keeps the last run's: writing the
    // very same DataValue again is, rightly, no change to report.
    let reading = 19.0 + f64::from(std::process::id() % 1000) / 1000.0;
    let stamped = DataValue {
        value: Some(reading.into()),
        status: Some(StatusCode::UncertainLastUsableValue),
        source_timestamp: Some(source),
        ..DataValue::default()
    };
    manager
        .publish(&temp, OpcUaValue::payload_of(&stamped), &Options::default())
        .await
        .unwrap();
    let got = OpcUaValue::data_value(&next(&mut sub).await).unwrap();
    assert_eq!(got.value, stamped.value);
    assert_eq!(got.status, Some(StatusCode::UncertainLastUsableValue));
    assert_eq!(got.source_timestamp, Some(source));
    assert!(got.server_timestamp.is_some_and(|t| t > source));
}

#[tokio::test(flavor = "multi_thread")]
async fn simulated_bad_quality_reaches_the_subscriber() {
    let _serial = SERIAL.lock().await;
    let session = connect().await;
    let provider = OpcUaProvider::new(session, OpcUaConfig::default());
    // opc-plc's BadFastUInt1 changes every second and is periodically Bad.
    let subject = format!("nsu={NAMESPACE};s=BadFastUInt1");
    let stream = provider
        .create(Stream {
            subjects: vec![subject.clone()],
            ..Stream::default()
        })
        .await
        .unwrap();
    let manager = provider.bind(&stream.id).await.unwrap();
    let mut sub = manager
        .subscribe(&subject, &Options::default())
        .await
        .unwrap();
    let mut statuses = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(40);
    while tokio::time::Instant::now() < deadline {
        let Ok(Some(message)) = timeout(Duration::from_secs(5), sub.next()).await else {
            continue;
        };
        let dv = OpcUaValue::data_value(&message).unwrap();
        statuses.push(dv.status());
        if statuses.iter().any(|s| !s.is_good()) && statuses.iter().any(|s| s.is_good()) {
            return;
        }
    }
    panic!("no non-Good status in {statuses:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn batch_writes_in_order() {
    let _serial = SERIAL.lock().await;
    let (_session, _provider, manager) = setup().await;
    let seq = node("Sequence");
    let mut sub = manager
        .subscribe(&seq, &Options::default().with_prefetch(500))
        .await
        .unwrap();
    let batch = as_batch(manager.as_ref()).unwrap();
    let payloads: Vec<Payload> = (1..=200i64).map(OpcUaValue::payload).collect();
    let ids = batch
        .publish_batch(&seq, payloads, &Options::default())
        .await
        .unwrap();
    assert_eq!(ids.len(), 200);
    for want in 1..=200i64 {
        assert_eq!(value(&mut sub).await, Variant::from(want));
    }
    let payloads = vec![
        OpcUaValue::payload(1i64),
        OpcUaValue::payload("x"),
        OpcUaValue::payload(3i64),
    ];
    match batch
        .publish_batch(&seq, payloads, &Options::default())
        .await
    {
        Err(StreamError::PartialBatch { ids, failures, .. }) => {
            assert_eq!(ids.len(), 3);
            assert_eq!(failures.len(), 1);
            assert_eq!(failures[0].0, 1);
            assert!(failures[0].1.to_string().contains("BadTypeMismatch"));
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn refusals() {
    let _serial = SERIAL.lock().await;
    let (_session, _provider, manager) = setup().await;
    let temp = node("Temperature");
    assert!(matches!(
        manager
            .subscribe(&temp, &Options::default().with_group("g"))
            .await
            .err(),
        Some(StreamError::Unsupported(_))
    ));
    assert!(matches!(
        manager
            .publish(
                &temp,
                OpcUaValue::payload(1.0f64),
                &Options::default().with_ttl(Duration::from_secs(1))
            )
            .await,
        Err(StreamError::Unsupported(_))
    ));
    assert!(matches!(
        manager
            .publish(&temp, Payload::json(&1.0).unwrap(), &Options::default())
            .await,
        Err(StreamError::Unsupported(_))
    ));
    // A Write has nowhere to carry headers, nothing to expire, no retain flag.
    for opts in [
        Options::default().header("lang", "rust"),
        Options::default().with_expiry(Duration::from_secs(5)),
        Options::default().with_retain(),
    ] {
        assert!(matches!(
            manager
                .publish(&temp, OpcUaValue::payload(1.0f64), &opts)
                .await,
            Err(StreamError::Unsupported(_))
        ));
    }
    assert!(as_durable(manager.as_ref()).is_err());
    assert!(as_positioned(manager.as_ref()).is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_a_subscription_deletes_the_monitored_item() {
    let _serial = SERIAL.lock().await;
    let (session, provider, manager) = setup().await;
    let a = manager
        .subscribe(&node("Temperature"), &Options::default())
        .await
        .unwrap();
    let b = manager
        .subscribe(&node("Count"), &Options::default())
        .await
        .unwrap();
    let held = items(&session);
    assert_eq!(held.len(), 2, "{held:?}");
    assert_eq!(held[0].0, held[1].0, "one subscription holds both");
    let (subscription, first) = (held[0].0, held[0].1);
    let ours = |s: &Session| items(s).len();

    drop(a);
    let deadline = tokio::time::Instant::now() + WAIT;
    while ours(&session) != 1 {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let gone = items(&session);
    let dropped = held.iter().find(|h| !gone.contains(h)).unwrap().1;
    // The server agrees: that item is gone and the other is not.
    assert_eq!(
        server_has(&session, subscription, dropped).await,
        StatusCode::BadMonitoredItemIdInvalid
    );
    let _ = first;

    drop(b);
    let deadline = tokio::time::Instant::now() + WAIT;
    while !items(&session).is_empty() || provider.monitored_items() != 0 {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // And with the last item, the subscription itself.
    assert_eq!(
        server_has(&session, subscription, 1).await,
        StatusCode::BadSubscriptionIdInvalid
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn subscriptions_survive_a_server_restart() {
    if std::env::var("STREAMS_E2E_RESTART").as_deref() != Ok("1") {
        eprintln!(
            "skipping: set STREAMS_E2E_RESTART=1 to restart {}",
            container()
        );
        return;
    }
    let _serial = SERIAL.lock().await;
    let (_session, _provider, manager) = setup().await;
    let count = node("Count");
    let mut sub = manager
        .subscribe(&count, &Options::default())
        .await
        .unwrap();
    manager
        .publish(&count, OpcUaValue::payload(1i32), &Options::default())
        .await
        .unwrap();
    assert_eq!(value(&mut sub).await, Variant::from(1i32));

    restart(&container());

    // Writes fail while the session is down and succeed once it is back.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        match manager
            .publish(&count, OpcUaValue::payload(7i32), &Options::default())
            .await
        {
            Ok(_) => break,
            Err(e) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "never came back: {e}"
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
    // The recreated item reports the node's value, then the write; a value
    // the restarted server reports before the write may come first.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let got = timeout(Duration::from_secs(30), sub.next())
            .await
            .unwrap()
            .unwrap();
        if got.decode::<Variant, _>(&OpcUaValue).unwrap() == Variant::from(7i32) {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
    }
    manager
        .publish(&count, OpcUaValue::payload(8i32), &Options::default())
        .await
        .unwrap();
    assert_eq!(value(&mut sub).await, Variant::from(8i32));
}

/// Restarts a container and waits until it reports healthy.
fn restart(name: &str) {
    let ok = Command::new("docker")
        .args(["restart", name])
        .status()
        .unwrap()
        .success();
    assert!(ok, "docker restart {name}");
    for _ in 0..120 {
        let out = Command::new("docker")
            .args(["inspect", "-f", "{{.State.Health.Status}}", name])
            .output()
            .unwrap();
        if String::from_utf8_lossy(&out.stdout).trim() == "healthy" {
            return;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    panic!("{name} did not come back healthy");
}
