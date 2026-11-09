//! The OPC UA provider over a secured channel, against the OPC PLC simulator
//! from docker/compose.opcua.secure.yaml:
//!
//! ```text
//! streams/docker/compose.opcua.secure.sh
//! docker compose -f streams/docker/compose.opcua.secure.yaml up -d --wait
//! cargo test -p tpp-streams --features opcua,tls,live-tests --test opcua_e2e
//! STREAMS_E2E_RESTART=1 cargo test -p tpp-streams --features opcua,tls,live-tests --test opcua_e2e
//! ```
//!
//! Every session here is Basic256Sha256 (or Aes256Sha256RsaPss) with
//! SignAndEncrypt, presents the client application certificate the server
//! was told to trust, pins the server's own certificate instead of accepting
//! whatever it presents, and logs in as the test user with a password. The
//! suite then proves the server refuses each thing it should: SecurityPolicy
//! None, anonymous and wrong-password users, and a client certificate it was
//! never given; and that the client refuses a server it has not pinned.
//!
//! OPC UA's security is its own, not TLS: the secure channel signs and
//! encrypts every message with the application certificates' keys, which is
//! why this suite sits beside the TLS ones and runs under the `tls` feature.
//!
//! `OPCUA_E2E_URL` (default `opc.tcp://127.0.0.1:14841`),
//! `STREAMS_E2E_TLS_DIR` (default `docker/tls/out`, which holds `opcua/`),
//! and `OPCUA_E2E_CONTAINER` (default `tpp-streams-opcua-secure`) point it
//! elsewhere. The restart test runs only with `STREAMS_E2E_RESTART=1`.

use std::{
    path::PathBuf,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use futures::StreamExt;
use opcua::{
    client::{ClientBuilder, IdentityToken, Session},
    crypto::{CertificateStore, SecurityPolicy, X509},
    types::{MessageSecurityMode, StatusCode, UserTokenPolicy, UserTokenType, Variant},
};
use streams::{
    Manager, Options, Payload, Stream, StreamError, Streams, Subscription, as_batch,
    drivers::opcua::{OpcUaConfig, OpcUaProvider, OpcUaValue},
};
use tokio::{sync::Mutex, task::JoinHandle, time::timeout};

const NAMESPACE: &str = "http://microsoft.com/Opc/OpcPlc/";
const USER: (&str, &str) = ("e2e-operator", "e2e-operator-secret");
const WAIT: Duration = Duration::from_secs(20);

static SERIAL: Mutex<()> = Mutex::const_new(());
static NEXT: AtomicU32 = AtomicU32::new(0);

fn url() -> String {
    std::env::var("OPCUA_E2E_URL").unwrap_or_else(|_| "opc.tcp://127.0.0.1:14841".into())
}

fn container() -> String {
    std::env::var("OPCUA_E2E_CONTAINER").unwrap_or_else(|_| "tpp-streams-opcua-secure".into())
}

fn certs() -> PathBuf {
    std::env::var("STREAMS_E2E_TLS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/docker/tls/out")))
        .join("opcua")
}

fn node(name: &str) -> String {
    format!("nsu={NAMESPACE};s={name}")
}

/// Who the client is and what it trusts.
#[derive(Clone, Copy)]
struct As {
    /// The application certificate: `client` (trusted by the server) or
    /// `stranger` (not).
    cert: &'static str,
    /// Whether the server's certificate is pinned in the client's store.
    pin_server: bool,
    identity: Identity,
    policy: SecurityPolicy,
    mode: MessageSecurityMode,
}

#[derive(Clone, Copy)]
enum Identity {
    Anonymous,
    User(&'static str, &'static str),
}

impl Default for As {
    fn default() -> Self {
        Self {
            cert: "client",
            pin_server: true,
            identity: Identity::User(USER.0, USER.1),
            policy: SecurityPolicy::Basic256Sha256,
            mode: MessageSecurityMode::SignAndEncrypt,
        }
    }
}

/// A fresh client PKI directory holding the server's certificate in its
/// trusted store when `pin` says so: trust is the file being there.
fn pki(pin: bool) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join("opcua-e2e")
        .join(format!(
            "{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
    let trusted = dir.join("trusted");
    std::fs::create_dir_all(&trusted).unwrap();
    if pin {
        let der = std::fs::read(certs().join("server.der"))
            .expect("run streams/docker/compose.opcua.secure.sh first");
        let cert = X509::from_der(&der).unwrap();
        std::fs::write(trusted.join(CertificateStore::cert_file_name(&cert)), der).unwrap();
    }
    dir
}

/// Starts a session as `who`, returning it and its event loop's task. With
/// `retry` false the event loop gives up on the first refusal and its task
/// ends with the status the server gave.
async fn session(who: As, retry: bool) -> (Arc<Session>, JoinHandle<StatusCode>) {
    let certs = certs();
    let mut client = ClientBuilder::new()
        .application_name("tpp-streams e2e client")
        .application_uri("urn:tpp-streams:e2e-client")
        .pki_dir(pki(who.pin_server))
        .certificate_path(certs.join(format!("{}.der", who.cert)))
        .private_key_path(certs.join(format!("{}.pem", who.cert)))
        .create_sample_keypair(false)
        // Real trust: only a pinned server certificate is accepted.
        .trust_server_certs(false)
        .verify_server_certs(true)
        .session_retry_initial(Duration::from_millis(250))
        .session_retry_max(Duration::from_secs(1))
        .session_retry_limit(if retry { -1 } else { 0 })
        .request_timeout(Duration::from_secs(5))
        .client()
        .expect("build the client");
    let identity = match who.identity {
        Identity::Anonymous => IdentityToken::Anonymous,
        Identity::User(user, password) => IdentityToken::UserName(user.into(), password.into()),
    };
    let url = url();
    let endpoint = (
        url.as_str(),
        who.policy.to_str(),
        who.mode,
        UserTokenPolicy::anonymous(),
    );
    let (session, event_loop) = match client
        .connect_to_matching_endpoint(endpoint.clone(), identity.clone())
        .await
    {
        Ok(found) => found,
        // No such endpoint: try it directly, so the refusal is the server's.
        Err(_) => client
            .connect_to_endpoint_directly(endpoint, identity)
            .expect("an endpoint description"),
    };
    (session, event_loop.spawn())
}

async fn connected(who: As) -> Arc<Session> {
    let (session, _) = session(who, true).await;
    assert!(
        timeout(WAIT, session.wait_for_connection())
            .await
            .unwrap_or(false),
        "a secured session to {}; is compose.opcua.secure.yaml up?",
        url()
    );
    session
}

/// The status a session that should be refused ends with.
async fn refused(who: As) -> StatusCode {
    let (session, task) = session(who, false).await;
    let status = timeout(WAIT, task)
        .await
        .expect("the refusal in time")
        .expect("the event loop ran");
    assert!(
        !timeout(Duration::from_millis(100), session.wait_for_connection())
            .await
            .unwrap_or(false)
    );
    status
}

async fn bound(session: Arc<Session>) -> (OpcUaProvider, Arc<dyn Manager>) {
    let provider = OpcUaProvider::new(session, OpcUaConfig::default());
    let stream = provider
        .create(Stream {
            name: "plc".into(),
            subjects: ["Temperature", "Count", "Running", "Mode", "Sequence"]
                .iter()
                .map(|n| node(n))
                .collect(),
            ..Stream::default()
        })
        .await
        .unwrap();
    let manager = provider.bind(&stream.id).await.unwrap();
    (provider, manager)
}

async fn value(sub: &mut Subscription) -> Variant {
    timeout(WAIT, sub.next())
        .await
        .expect("a notification in time")
        .expect("the stream is open")
        .decode(&OpcUaValue)
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn signed_and_encrypted_round_trip() {
    let _serial = SERIAL.lock().await;
    let session = connected(As::default()).await;
    let info = session.endpoint_info();
    assert_eq!(
        info.endpoint.security_mode,
        MessageSecurityMode::SignAndEncrypt
    );
    assert_eq!(
        SecurityPolicy::from_uri(info.endpoint.security_policy_uri.as_ref()),
        SecurityPolicy::Basic256Sha256
    );
    let (provider, manager) = bound(session).await;
    let cases: Vec<(&str, Variant)> = vec![
        ("Temperature", 36.6f64.into()),
        ("Count", 7i32.into()),
        ("Running", false.into()),
        ("Mode", "heating".into()),
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
        assert_eq!(&value(sub).await, v, "{name}");
    }

    let seq = node("Sequence");
    let mut sub = manager
        .subscribe(&seq, &Options::default().with_prefetch(200))
        .await
        .unwrap();
    let payloads: Vec<Payload> = (1..=100i64).map(OpcUaValue::payload).collect();
    as_batch(manager.as_ref())
        .unwrap()
        .publish_batch(&seq, payloads, &Options::default())
        .await
        .unwrap();
    for want in 1..=100i64 {
        assert_eq!(value(&mut sub).await, Variant::from(want));
    }
    drop((subs, sub));
    let deadline = tokio::time::Instant::now() + WAIT;
    while provider.monitored_items() != 0 {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// Headers, expiry and retain are refused by name over the secure session too,
// and nothing is written.
#[tokio::test(flavor = "multi_thread")]
async fn headers_expiry_and_retain_are_refused() {
    let _serial = SERIAL.lock().await;
    let (provider, manager) = bound(connected(As::default()).await).await;
    let temp = node("Temperature");
    let error = provider
        .create(
            Stream::named("h")
                .subject(temp.clone())
                .header("lang", "rust"),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(&error, StreamError::Unsupported(why) if why.contains("headers")),
        "{error}"
    );
    for (opts, says) in [
        (Options::default().header("lang", "rust"), "headers"),
        (
            Options::default().with_expiry(Duration::from_secs(5)),
            "expire",
        ),
        (Options::default().with_retain(), "retain"),
    ] {
        match manager
            .publish(&temp, OpcUaValue::payload(1.0f64), &opts)
            .await
        {
            Err(StreamError::Unsupported(why)) => assert!(why.contains(says), "{why}"),
            other => panic!("{says}: {other:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn aes256_sha256_rsa_pss_works_too() {
    let _serial = SERIAL.lock().await;
    let session = connected(As {
        policy: SecurityPolicy::Aes256Sha256RsaPss,
        ..As::default()
    })
    .await;
    let (_provider, manager) = bound(session).await;
    let mut sub = manager
        .subscribe(&node("Count"), &Options::default())
        .await
        .unwrap();
    manager
        .publish(
            &node("Count"),
            OpcUaValue::payload(11i32),
            &Options::default(),
        )
        .await
        .unwrap();
    assert_eq!(value(&mut sub).await, Variant::from(11i32));
}

#[tokio::test(flavor = "multi_thread")]
async fn security_mode_none_is_refused() {
    let _serial = SERIAL.lock().await;
    // The server does not advertise a None endpoint at all...
    let client = ClientBuilder::new()
        .application_name("tpp-streams e2e client")
        .application_uri("urn:tpp-streams:e2e-client")
        .pki_dir(pki(true))
        .create_sample_keypair(false)
        .client()
        .unwrap();
    let endpoints = client.get_server_endpoints_from_url(url()).await.unwrap();
    assert!(!endpoints.is_empty());
    for endpoint in &endpoints {
        assert_ne!(
            endpoint.security_mode,
            MessageSecurityMode::None,
            "{endpoint:?}"
        );
        assert_ne!(
            SecurityPolicy::from_uri(endpoint.security_policy_uri.as_ref()),
            SecurityPolicy::None
        );
    }
    // ...and asking for one regardless is refused by the server.
    let status = refused(As {
        policy: SecurityPolicy::None,
        mode: MessageSecurityMode::None,
        ..As::default()
    })
    .await;
    assert!(status.is_bad(), "{status}");
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_credentials_are_refused() {
    let _serial = SERIAL.lock().await;
    // A wrong password and an unknown user reach the server, which refuses
    // to activate the session.
    for identity in [
        Identity::User(USER.0, "not-the-password"),
        Identity::User("nobody", USER.1),
    ] {
        let status = refused(As {
            identity,
            ..As::default()
        })
        .await;
        assert_eq!(status, StatusCode::BadUserAccessDenied);
    }
    // Anonymous is not offered on any endpoint, so the client has no policy
    // to log in under and gives up before asking.
    let client = ClientBuilder::new()
        .application_name("tpp-streams e2e client")
        .application_uri("urn:tpp-streams:e2e-client")
        .pki_dir(pki(true))
        .create_sample_keypair(false)
        .client()
        .unwrap();
    for endpoint in client.get_server_endpoints_from_url(url()).await.unwrap() {
        for token in endpoint.user_identity_tokens.unwrap_or_default() {
            assert_ne!(token.token_type, UserTokenType::Anonymous, "{token:?}");
        }
    }
    let status = refused(As {
        identity: Identity::Anonymous,
        ..As::default()
    })
    .await;
    assert!(status.is_bad(), "{status}");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_untrusted_client_certificate_is_refused() {
    let _serial = SERIAL.lock().await;
    let status = refused(As {
        cert: "stranger",
        ..As::default()
    })
    .await;
    assert!(
        matches!(
            status,
            StatusCode::BadSecurityChecksFailed
                | StatusCode::BadCertificateUntrusted
                | StatusCode::BadConnectionClosed
                | StatusCode::BadCommunicationError
        ),
        "{status}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unpinned_server_is_refused_by_the_client() {
    let _serial = SERIAL.lock().await;
    let status = refused(As {
        pin_server: false,
        ..As::default()
    })
    .await;
    assert!(status.is_bad(), "{status}");
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
    let session = connected(As::default()).await;
    let (_provider, manager) = bound(session.clone()).await;
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

    let before = session.server_session_id();
    restart(&container());

    // The session reconnects over a new secure channel, logs in again, and
    // recreates the subscription; writes fail until then.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        match manager
            .publish(&count, OpcUaValue::payload(7i32), &Options::default())
            .await
        {
            Ok(_) => break,
            Err(e) => {
                assert!(!matches!(e, StreamError::UnknownSubject { .. }), "{e}");
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "never came back: {e}"
                );
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
    // A new session on the restarted server, not the old one carrying on.
    assert_ne!(session.server_session_id(), before);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while value(&mut sub).await != Variant::from(7i32) {
        assert!(tokio::time::Instant::now() < deadline);
    }
    assert_eq!(
        session.endpoint_info().endpoint.security_mode,
        MessageSecurityMode::SignAndEncrypt
    );
    manager
        .publish(&count, OpcUaValue::payload(8i32), &Options::default())
        .await
        .unwrap();
    assert_eq!(value(&mut sub).await, Variant::from(8i32));
}

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
