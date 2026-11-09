//! The Redis provider end to end over mutual TLS, against a durable server.
//!
//! Start the server with
//! `docker compose -p tpp-streams-e2e-redis -f docker/compose.redis.secure.yaml up -d --wait`,
//! then `cargo test --features redis,tls,live-tests --test redis_e2e`.
//!
//! - `REDIS_E2E_ADDR` (default `127.0.0.1:16381`) is the TLS port.
//! - `REDIS_E2E_CERTS` (default `docker/tls/out`) holds `ca.crt`,
//!   `client.crt`, and `client.key`.
//! - `STREAMS_E2E_RESTART=1` enables the test that restarts the container
//!   `tpp-streams-redis-secure` (`REDIS_E2E_CONTAINER`) and proves what
//!   survives it. Without it that test returns early, so it never restarts a
//!   server somebody else is using by accident.
//!
//! Every test runs under its own key prefix in database 3. The restart test
//! takes a write lock that every other test reads, so nothing else is in
//! flight while the server is down.
//!
//! The server also requires an ACL user (`docker/compose.redis.secure.acl`):
//! every provider here connects as `streams`, which may run only the commands
//! the driver sends, on only its own keys and channels. The helpers that
//! inspect the server behind the provider's back (`command`) connect as
//! `admin`, because XINFO, CONFIG GET, PUBSUB NUMSUB, and cleanup of keys a
//! test planted are exactly what `streams` is denied.

use std::{collections::HashSet, path::PathBuf, process::Command, sync::Arc, time::Duration};

use futures::StreamExt;
use streams::{
    Deliveries, Delivery, Headers, Manager, Message, Options, Payload, Position, Stream,
    StreamError, Streams, Subscription, as_batch, as_durable, as_positioned,
    drivers::redis::{
        RedisConfig, RedisProvider,
        redis::{self, ClientTlsConfig, TlsCertificates},
    },
};
use tokio::sync::RwLock;

const SUBJECT: &str = "user.created";
const WITHIN: Duration = Duration::from_secs(10);
const DB: u8 = 3;

/// Short enough to see a redelivery inside a test, long enough that a consumer
/// which is merely slow does not have its message taken.
const QUICK_RECLAIM: Duration = Duration::from_millis(300);

/// Every ordinary test reads this; the restart test writes it.
static SERVER: RwLock<()> = RwLock::const_new(());

#[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize)]
struct Event {
    user: String,
    action: String,
}

fn event(user: &str, action: &str) -> Payload {
    Payload::json(&Event {
        user: user.into(),
        action: action.into(),
    })
    .unwrap()
}

fn user(message: &Message) -> String {
    message.json::<Event>().unwrap().user
}

fn addr() -> String {
    std::env::var("REDIS_E2E_ADDR").unwrap_or_else(|_| "127.0.0.1:16381".into())
}

fn certs() -> PathBuf {
    std::env::var_os("REDIS_E2E_CERTS")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("docker/tls/out"))
}

fn read_cert(name: &str) -> Vec<u8> {
    let path = certs().join(name);
    std::fs::read(&path).unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()))
}

fn container() -> String {
    std::env::var("REDIS_E2E_CONTAINER").unwrap_or_else(|_| "tpp-streams-redis-secure".into())
}

/// Test-only credentials, matching the hashes in the ACL file.
const STREAMS_USER: &str = "streams";
const STREAMS_PASSWORD: &str = "streams-e2e-not-a-secret";
const ADMIN_USER: &str = "admin";
const ADMIN_PASSWORD: &str = "admin-e2e-not-a-secret";

/// Who a connection authenticates as: `None` sends no AUTH at all.
type Login<'a> = Option<(&'a str, &'a str)>;

const AS_STREAMS: Login<'static> = Some((STREAMS_USER, STREAMS_PASSWORD));
const AS_ADMIN: Login<'static> = Some((ADMIN_USER, ADMIN_PASSWORD));

/// A TLS client logging in as the `streams` user, presenting the client
/// certificate when `mutual`.
fn tls_client(mutual: bool) -> redis::Client {
    tls_client_as(AS_STREAMS, mutual)
}

/// A TLS client logging in as `login`, presenting the client certificate when
/// `mutual`.
fn tls_client_as(login: Login<'_>, mutual: bool) -> redis::Client {
    let client_tls = mutual.then(|| ClientTlsConfig {
        client_cert: read_cert("client.crt"),
        client_key: read_cert("client.key"),
    });
    let userinfo = login.map_or_else(String::new, |(user, password)| {
        format!("{user}:{password}@")
    });
    redis::Client::build_with_tls(
        format!("rediss://{userinfo}{}/{DB}", addr()),
        TlsCertificates {
            client_tls,
            root_cert: Some(read_cert("ca.crt")),
        },
    )
    .unwrap()
}

/// Connection settings for the test's own connections, which tolerate a
/// loaded machine: the client's defaults give a TLS handshake one second.
fn patient() -> redis::AsyncConnectionConfig {
    redis::AsyncConnectionConfig::new()
        .set_connection_timeout(Some(Duration::from_secs(10)))
        .set_response_timeout(Some(Duration::from_secs(10)))
}

/// The mTLS client every test uses, failing loudly when no server answers:
/// with `live-tests` on, a missing server is a failure, not a skip.
async fn client() -> redis::Client {
    let client = tls_client(true);
    client
        .get_multiplexed_async_connection_with_config(&patient())
        .await
        .unwrap_or_else(|error| {
            panic!(
                "no TLS Redis at {} ({error}); start docker/compose.redis.secure.yaml",
                addr()
            )
        });
    client
}

fn config() -> RedisConfig {
    RedisConfig {
        prefix: format!("e2e{}", streams::provider::new_id()),
        reclaim_after: QUICK_RECLAIM,
        ..RedisConfig::default()
    }
}

/// Runs `cmd` as `admin`, for looking at the server behind the provider.
async fn command<T: redis::FromRedisValue>(cmd: &redis::Cmd) -> T {
    let mut conn = tls_client_as(AS_ADMIN, true)
        .get_multiplexed_async_connection_with_config(&patient())
        .await
        .unwrap();
    cmd.query_async(&mut conn).await.unwrap()
}

/// Runs `cmd` as `streams` on a fresh connection, returning the server's
/// answer, refusal included.
async fn as_streams<T: redis::FromRedisValue>(cmd: &redis::Cmd) -> redis::RedisResult<T> {
    let mut conn = tls_client(true)
        .get_multiplexed_async_connection_with_config(&patient())
        .await?;
    cmd.query_async(&mut conn).await
}

/// Asserts `result` is the ACL's refusal.
fn noperm<T: std::fmt::Debug>(what: &str, result: redis::RedisResult<T>) {
    match result {
        Err(error) if error.code() == Some("NOPERM") => {}
        other => panic!("{what}: expected NOPERM, got {other:?}"),
    }
}

async fn exists(key: &str) -> bool {
    command::<u64>(redis::cmd("EXISTS").arg(key)).await == 1
}

async fn create(provider: &RedisProvider, subjects: &[&str]) -> (Stream, Arc<dyn Manager>) {
    let stream = provider
        .create(Stream {
            name: "e2e".into(),
            subjects: subjects.iter().map(|s| (*s).to_owned()).collect(),
            ..Stream::default()
        })
        .await
        .unwrap();
    let manager = provider.bind(&stream.id).await.unwrap();
    (stream, manager)
}

async fn recv(messages: &mut Subscription) -> Message {
    tokio::time::timeout(WITHIN, messages.next())
        .await
        .expect("no message arrived in time")
        .expect("the subscription ended before a message arrived")
}

async fn recv_delivery(deliveries: &mut Deliveries) -> Delivery {
    tokio::time::timeout(WITHIN, deliveries.next())
        .await
        .expect("no delivery arrived in time")
        .expect("the consumer ended before a delivery arrived")
}

async fn silent<S: futures::Stream + Unpin>(stream: &mut S, quiet: Duration)
where
    S::Item: std::fmt::Debug,
{
    if let Ok(Some(item)) = tokio::time::timeout(quiet, stream.next()).await {
        panic!("expected nothing, received {item:?}");
    }
}

async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + WITHIN;
    while tokio::time::Instant::now() < deadline {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{what} did not happen in time");
}

fn unsupported<T: std::fmt::Debug>(result: streams::Result<T>) {
    assert!(
        matches!(result, Err(StreamError::Unsupported(_))),
        "expected Unsupported, got {result:?}"
    );
}

fn not_found<T: std::fmt::Debug>(result: streams::Result<T>) {
    assert!(
        matches!(result, Err(StreamError::NotFound(_))),
        "expected NotFound, got {result:?}"
    );
}

fn unknown_subject<T: std::fmt::Debug>(result: streams::Result<T>) {
    assert!(
        matches!(result, Err(StreamError::UnknownSubject { .. })),
        "expected UnknownSubject, got {result:?}"
    );
}

/// Subscribers on the channel a pub/sub stream's subject travels on.
async fn numsub(prefix: &str, stream: &str, subject: &str) -> u64 {
    let channel = format!("{prefix}:stream:ch:{stream}:{subject}");
    let reply: (String, u64) = command(redis::cmd("PUBSUB").arg("NUMSUB").arg(&channel)).await;
    reply.1
}

/// How many consumers a durable consumer group has, by identity.
async fn consumers(key: &str, group: &str) -> usize {
    let reply: Vec<redis::Value> =
        command(redis::cmd("XINFO").arg("CONSUMERS").arg(key).arg(group)).await;
    reply.len()
}

/// The group's pending count, from the summary form of XPENDING.
async fn pending(key: &str, group: &str) -> u64 {
    let reply: redis::streams::StreamPendingReply =
        command(redis::cmd("XPENDING").arg(key).arg(group)).await;
    match reply {
        redis::streams::StreamPendingReply::Data(data) => data.count as u64,
        _ => 0,
    }
}

// --- the transport is really TLS ------------------------------------------

#[tokio::test]
async fn plaintext_is_refused() {
    let _server = SERVER.read().await;
    let plain = redis::Client::open(format!("redis://{}/{DB}", addr())).unwrap();
    let attempt = tokio::time::timeout(Duration::from_secs(5), async {
        let mut conn = plain.get_multiplexed_async_connection().await?;
        redis::cmd("PING").query_async::<String>(&mut conn).await
    })
    .await;
    assert!(
        !matches!(attempt, Ok(Ok(_))),
        "a plaintext PING was answered on the TLS port"
    );
}

#[tokio::test]
async fn tls_without_a_client_certificate_is_refused() {
    let _server = SERVER.read().await;
    let anonymous = tls_client(false);
    let attempt = tokio::time::timeout(Duration::from_secs(5), async {
        let mut conn = anonymous.get_multiplexed_async_connection().await?;
        redis::cmd("PING").query_async::<String>(&mut conn).await
    })
    .await;
    assert!(
        !matches!(attempt, Ok(Ok(_))),
        "a TLS client without a certificate was answered"
    );
}

#[tokio::test]
async fn an_untrusted_server_is_refused() {
    let _server = SERVER.read().await;
    // No root certificate: the server's self-made CA is not in the platform
    // store, so the client must refuse it rather than talk to it.
    let client = redis::Client::build_with_tls(
        format!("rediss://{}/{DB}", addr()),
        TlsCertificates {
            client_tls: Some(ClientTlsConfig {
                client_cert: read_cert("client.crt"),
                client_key: read_cert("client.key"),
            }),
            root_cert: None,
        },
    )
    .unwrap();
    let attempt = tokio::time::timeout(Duration::from_secs(5), async {
        let mut conn = client.get_multiplexed_async_connection().await?;
        redis::cmd("PING").query_async::<String>(&mut conn).await
    })
    .await;
    assert!(
        !matches!(attempt, Ok(Ok(_))),
        "a server signed by an unknown CA was trusted"
    );
}

// --- authentication: an ACL user on top of the certificate --------------------

/// Connects as `login` and PINGs, returning whatever went wrong first.
async fn ping_as(login: Login<'_>) -> redis::RedisResult<String> {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut conn = tls_client_as(login, true)
            .get_multiplexed_async_connection_with_config(&patient())
            .await?;
        redis::cmd("PING").query_async::<String>(&mut conn).await
    })
    .await
    .expect("the server did not answer at all")
}

#[tokio::test]
async fn the_streams_user_logs_in() {
    let _server = SERVER.read().await;
    assert_eq!(ping_as(AS_STREAMS).await.unwrap(), "PONG");
    let whoami: String = command(redis::cmd("ACL").arg("WHOAMI")).await;
    assert_eq!(whoami, ADMIN_USER);
}

/// A valid certificate and no password is a connection that can run nothing:
/// the client's SELECT is the first command, and the server refuses it.
#[tokio::test]
async fn no_password_is_refused() {
    let _server = SERVER.read().await;
    let error = ping_as(None).await.unwrap_err();
    assert!(
        error.to_string().contains("Authentication required"),
        "{error}"
    );
}

#[tokio::test]
async fn a_wrong_password_is_refused() {
    let _server = SERVER.read().await;
    let error = ping_as(Some((STREAMS_USER, "not-the-password")))
        .await
        .unwrap_err();
    assert_eq!(
        error.kind(),
        redis::ErrorKind::AuthenticationFailed,
        "{error}"
    );

    // And the provider says so rather than limping on: its first command
    // fails. The client library retries a failed login for a few seconds
    // before giving up, hence the long timeout.
    let wrong = redis::Client::build_with_tls(
        format!("rediss://{STREAMS_USER}:not-the-password@{}/{DB}", addr()),
        TlsCertificates {
            client_tls: Some(ClientTlsConfig {
                client_cert: read_cert("client.crt"),
                client_key: read_cert("client.key"),
            }),
            root_cert: Some(read_cert("ca.crt")),
        },
    )
    .unwrap();
    let provider = RedisProvider::new(wrong, config());
    let created = tokio::time::timeout(
        Duration::from_secs(60),
        provider.create(Stream {
            name: "e2e".into(),
            subjects: vec![SUBJECT.into()],
            ..Stream::default()
        }),
    )
    .await
    .expect("a provider with a wrong password hung");
    assert!(
        matches!(&created, Err(StreamError::Internal(_))),
        "{created:?}"
    );
}

/// `default` is off: it cannot log in with any password, and is not the user
/// an unauthenticated connection runs as.
#[tokio::test]
async fn the_default_user_is_disabled() {
    let _server = SERVER.read().await;
    for password in ["nopass", "default", STREAMS_PASSWORD, ADMIN_PASSWORD] {
        let error = ping_as(Some(("default", password))).await.unwrap_err();
        assert_eq!(
            error.kind(),
            redis::ErrorKind::AuthenticationFailed,
            "default/{password:?}: {error}"
        );
    }
    let user: Vec<redis::Value> = command(redis::cmd("ACL").arg("GETUSER").arg("default")).await;
    let flags = format!("{user:?}");
    assert!(
        flags.contains("\"off\"") && !flags.contains("\"on\""),
        "{flags}"
    );
    let users: Vec<String> = command(redis::cmd("ACL").arg("USERS")).await;
    assert_eq!(users, [ADMIN_USER, "default", STREAMS_USER]);
}

/// The ACL is a real boundary: inside it the driver works (every other test),
/// and one step outside, by command, key, or channel, is NOPERM.
#[tokio::test]
async fn the_acl_confines_the_streams_user() {
    let _server = SERVER.read().await;
    let mine = format!("e2e{}:durable:s:x:y", streams::provider::new_id());

    // Commands no mode needs, even on its own keys.
    noperm(
        "CONFIG GET",
        as_streams::<redis::Value>(redis::cmd("CONFIG").arg("GET").arg("port")).await,
    );
    noperm("FLUSHDB", as_streams::<()>(&redis::cmd("FLUSHDB")).await);
    noperm(
        "KEYS",
        as_streams::<Vec<String>>(redis::cmd("KEYS").arg("*")).await,
    );
    noperm(
        "EVAL",
        as_streams::<redis::Value>(redis::cmd("EVAL").arg("return 1").arg(0)).await,
    );
    noperm(
        "GET",
        as_streams::<Option<String>>(redis::cmd("GET").arg(&mine)).await,
    );
    noperm(
        "XINFO",
        as_streams::<redis::Value>(redis::cmd("XINFO").arg("STREAM").arg(&mine)).await,
    );
    noperm(
        "XGROUP DESTROY",
        as_streams::<redis::Value>(redis::cmd("XGROUP").arg("DESTROY").arg(&mine).arg("g")).await,
    );
    noperm(
        "ACL WHOAMI",
        as_streams::<String>(redis::cmd("ACL").arg("WHOAMI")).await,
    );

    // A command it has, on a key outside its namespaces.
    noperm(
        "SET outside",
        as_streams::<()>(redis::cmd("SET").arg("outside:key").arg("x")).await,
    );
    noperm(
        "XADD another prefix",
        as_streams::<String>(
            redis::cmd("XADD")
                .arg("orders:durable:s:x:y")
                .arg("*")
                .arg("msg")
                .arg("x"),
        )
        .await,
    );

    // Channels: another prefix's subjects, and another database's expiries.
    for channel in ["orders:stream:ch:x:y", "__keyevent@0__:expired"] {
        noperm(
            &format!("PUBLISH {channel}"),
            as_streams::<u64>(redis::cmd("PUBLISH").arg(channel).arg("x")).await,
        );
        let mut pubsub = tls_client(true).get_async_pubsub().await.unwrap();
        noperm(
            &format!("SUBSCRIBE {channel}"),
            pubsub.subscribe(channel).await,
        );
    }

    // Through the provider: a prefix outside the ACL is refused at the first
    // write, and the refusal reaches the caller.
    let outside = RedisProvider::durable(
        client().await,
        RedisConfig {
            prefix: "orders".into(),
            ..RedisConfig::default()
        },
    );
    let refused = outside
        .create(Stream {
            name: "e2e".into(),
            subjects: vec![SUBJECT.into()],
            ..Stream::default()
        })
        .await;
    assert!(
        matches!(&refused, Err(StreamError::Internal(message))
            if message.to_ascii_uppercase().contains("NOPERM")),
        "{refused:?}"
    );
    // Nothing the refusals touched exists.
    assert!(!exists("outside:key").await);
    assert!(!exists("orders:durable:s:x:y").await);
}

/// The server is configured the way the compose file says: TLS only, client
/// certificates required, AOF fsynced on every write, expiry events on.
#[tokio::test]
async fn the_server_is_tls_only_and_durable() {
    let _server = SERVER.read().await;
    let get = |name: &'static str| async move {
        let reply: Vec<String> = command(redis::cmd("CONFIG").arg("GET").arg(name)).await;
        reply.get(1).cloned().unwrap_or_default()
    };
    assert_eq!(get("port").await, "0");
    assert_eq!(get("tls-port").await, "6380");
    assert_eq!(get("tls-auth-clients").await, "yes");
    assert_eq!(get("appendonly").await, "yes");
    assert_eq!(get("appendfsync").await, "always");
    assert_eq!(get("aclfile").await, "/etc/redis/users.acl");
    let events = get("notify-keyspace-events").await;
    assert!(events.contains('E') && events.contains('x'), "{events}");
}

// --- lifecycle --------------------------------------------------------------

/// Create (explicit and generated ids), get, list, update, bind, delete, and
/// NotFound after, in each of the three modes, with nothing left behind.
#[tokio::test]
async fn the_full_lifecycle_in_every_mode() {
    let _server = SERVER.read().await;
    let client = client().await;
    let config = config();
    let modes = [
        ("stream", RedisProvider::new(client.clone(), config.clone())),
        (
            "notify",
            RedisProvider::scheduled(client.clone(), config.clone()),
        ),
        ("durable", RedisProvider::durable(client, config.clone())),
    ];
    for (kind, provider) in &modes {
        let explicit = provider
            .create(Stream {
                id: "orders".into(),
                name: "orders".into(),
                description: "before".into(),
                subjects: vec![SUBJECT.into()],
                ..Stream::default()
            })
            .await
            .unwrap();
        assert_eq!(explicit.id, "orders");
        assert!(explicit.active);

        let generated = provider
            .create(Stream {
                name: "users".into(),
                subjects: vec![SUBJECT.into(), "user.deleted".into()],
                ..Stream::default()
            })
            .await
            .unwrap();
        assert!(!generated.id.is_empty());
        assert_ne!(generated.id, explicit.id);

        let got = provider.get(&generated.id).await.unwrap();
        assert_eq!(got, generated);

        let listed: Vec<String> = provider
            .list()
            .await
            .unwrap()
            .into_iter()
            .map(|s| s.id)
            .collect();
        let mut want = vec![explicit.id.clone(), generated.id.clone()];
        want.sort();
        assert_eq!(listed, want, "{kind}");

        let updated = provider
            .update(
                &explicit.id,
                Stream {
                    name: "orders".into(),
                    description: "after".into(),
                    subjects: vec![SUBJECT.into(), "order.shipped".into()],
                    ..Stream::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(updated.id, explicit.id);
        let reread = provider.get(&explicit.id).await.unwrap();
        assert_eq!(reread.description, "after");
        assert_eq!(reread.subjects.len(), 2);
        let meta = format!("{}:{kind}:meta:{}", config.prefix, explicit.id);
        assert_eq!(
            command::<u64>(redis::cmd("XLEN").arg(&meta)).await,
            1,
            "an update must replace the metadata entry, not accumulate"
        );
        not_found(
            provider
                .update("absent", Stream::default())
                .await
                .map(|_| ()),
        );

        // Bind sees the update: the added subject is now declared.
        let manager = provider.bind(&explicit.id).await.unwrap();
        let opts = if *kind == "notify" {
            Options::default().with_ttl(Duration::from_secs(60))
        } else {
            Options::default()
        };
        manager
            .publish("order.shipped", event("bind", ""), &opts)
            .await
            .unwrap();

        for stream in [&explicit, &generated] {
            provider.delete(&stream.id).await.unwrap();
            not_found(provider.get(&stream.id).await);
            not_found(provider.bind(&stream.id).await.map(|_| ()));
            not_found(provider.delete(&stream.id).await);
            assert!(
                !exists(&format!("{}:{kind}:meta:{}", config.prefix, stream.id)).await,
                "{kind}: the metadata key outlived delete"
            );
        }
        assert!(provider.list().await.unwrap().is_empty(), "{kind}");
    }
}

// --- immediate (pub/sub) ------------------------------------------------------

#[tokio::test]
async fn pubsub_over_tls() {
    let _server = SERVER.read().await;
    let config = config();
    let provider = RedisProvider::new(client().await, config.clone());
    let (stream, m) = create(&provider, &[SUBJECT, "other"]).await;

    let mut messages = m.subscribe(SUBJECT, &Options::default()).await.unwrap();
    assert_eq!(numsub(&config.prefix, &stream.id, SUBJECT).await, 1);

    // A generated id, then a chosen one, then a partition key, which a
    // partitionless backend accepts and ignores.
    let generated = m
        .publish(SUBJECT, event("ada", "created"), &Options::default())
        .await
        .unwrap();
    let got = recv(&mut messages).await;
    assert_eq!(got.id(), generated);
    assert_eq!(got.subject(), SUBJECT);
    assert_eq!(got.codec(), "json");
    assert_eq!(user(&got), "ada");

    let chosen = m
        .publish(
            SUBJECT,
            event("grace", ""),
            &Options::default().with_id("order-42"),
        )
        .await
        .unwrap();
    assert_eq!(chosen, "order-42");
    assert_eq!(recv(&mut messages).await.id(), "order-42");

    m.publish(
        SUBJECT,
        event("keyed", ""),
        &Options::default().with_partition_key("tenant-1"),
    )
    .await
    .unwrap();
    assert_eq!(user(&recv(&mut messages).await), "keyed");

    // Exactly once, and other subjects stay on their own channel.
    m.publish("other", event("elsewhere", ""), &Options::default())
        .await
        .unwrap();
    silent(&mut messages, Duration::from_millis(500)).await;

    // Undeclared subjects and the options that would change the meaning.
    unknown_subject(m.publish("typo", event("", ""), &Options::default()).await);
    unknown_subject(m.subscribe("typo", &Options::default()).await.map(|_| ()));
    unsupported(
        m.publish(
            SUBJECT,
            event("", ""),
            &Options::default().with_ttl(Duration::from_secs(1)),
        )
        .await,
    );
    unsupported(
        m.subscribe(SUBJECT, &Options::default().with_group("workers"))
            .await
            .map(|_| ()),
    );
    unsupported(as_durable(m.as_ref()).map(|_| ()));
    unsupported(as_positioned(m.as_ref()).map(|_| ()));

    // Prefetch is a request, and asking for none still delivers.
    for prefetch in [0, 1, 128] {
        let mut sized = m
            .subscribe(SUBJECT, &Options::default().with_prefetch(prefetch))
            .await
            .unwrap();
        m.publish(SUBJECT, event("sized", ""), &Options::default())
            .await
            .unwrap();
        assert_eq!(user(&recv(&mut sized).await), "sized");
        assert_eq!(user(&recv(&mut messages).await), "sized");
    }

    // Batch: one pipeline, ids in order; a single id for several is refused.
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
        assert_eq!(recv(&mut messages).await.id(), id);
    }
    unsupported(
        batch
            .publish_batch(
                SUBJECT,
                vec![event("a", "")],
                &Options::default().with_id("x"),
            )
            .await,
    );
    assert!(
        batch
            .publish_batch(SUBJECT, Vec::new(), &Options::default())
            .await
            .unwrap()
            .is_empty()
    );

    // Dropping the subscription releases the server-side subscription.
    drop(messages);
    eventually("the server-side unsubscribe", || async {
        numsub(&config.prefix, &stream.id, SUBJECT).await == 0
    })
    .await;
    provider.delete(&stream.id).await.unwrap();
}

#[tokio::test]
async fn the_typed_view_over_tls() {
    let _server = SERVER.read().await;
    let provider = RedisProvider::new(client().await, config());
    let (stream, m) = create(&provider, &[SUBJECT]).await;
    let events = streams::typed::<Event>(m);
    let mut received = events
        .subscribe(SUBJECT, &Options::default())
        .await
        .unwrap();
    let grace = Event {
        user: "grace".into(),
        action: "created".into(),
    };
    events
        .publish(SUBJECT, &grace, &Options::default())
        .await
        .unwrap();
    let got = tokio::time::timeout(WITHIN, received.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got, grace);
    provider.delete(&stream.id).await.unwrap();
}

// --- scheduled (TTL expiry) ---------------------------------------------------

fn after(ms: u64) -> Options {
    Options::default().with_ttl(Duration::from_millis(ms))
}

#[tokio::test]
async fn scheduled_over_tls() {
    let _server = SERVER.read().await;
    let config = config();
    let provider = RedisProvider::scheduled(client().await, config.clone());
    let (stream, m) = create(&provider, &["reminder", "unwanted"]).await;
    let mut a = m.subscribe("reminder", &Options::default()).await.unwrap();
    let mut b = m.subscribe("reminder", &Options::default()).await.unwrap();

    // Fires after its TTL, under the id it was given, to exactly one of two
    // racing subscribers, and the payload key is claimed away.
    let published = tokio::time::Instant::now();
    let id = m
        .publish(
            "reminder",
            event("alice", "take a pill"),
            &after(1500).with_id("pill-1"),
        )
        .await
        .unwrap();
    assert_eq!(id, "pill-1");
    let payload = format!("{}:notify:payload:{id}", config.prefix);
    assert!(exists(&payload).await);
    let fired = {
        let mut merged = futures::stream::select(&mut a, &mut b);
        let fired = tokio::time::timeout(WITHIN, merged.next())
            .await
            .unwrap()
            .unwrap();
        silent(&mut merged, Duration::from_millis(700)).await;
        fired
    };
    assert!(published.elapsed() >= Duration::from_millis(1500));
    assert_eq!(fired.id(), "pill-1");
    assert_eq!(fired.json::<Event>().unwrap().action, "take a pill");
    assert!(
        !exists(&payload).await,
        "a delivered payload key was left behind"
    );

    // Other subjects do not leak in.
    m.publish("unwanted", event("nope", ""), &after(200))
        .await
        .unwrap();
    silent(&mut a, Duration::from_millis(1000)).await;

    // A scheduled batch fires whole.
    let ids = as_batch(m.as_ref())
        .unwrap()
        .publish_batch(
            "reminder",
            vec![event("x", ""), event("y", "")],
            &after(200),
        )
        .await
        .unwrap();
    let mut seen = HashSet::new();
    let mut merged = futures::stream::select(&mut a, &mut b);
    while seen.len() < 2 {
        let message = tokio::time::timeout(WITHIN, merged.next())
            .await
            .unwrap()
            .unwrap();
        seen.insert(message.id().to_owned());
    }
    assert_eq!(seen, ids.into_iter().collect());
    drop(merged);

    // A missing or zero TTL, a group, and an undeclared subject are refused.
    unsupported(
        m.publish("reminder", event("", ""), &Options::default())
            .await,
    );
    unsupported(
        m.publish(
            "reminder",
            event("", ""),
            &Options::default().with_ttl(Duration::ZERO),
        )
        .await,
    );
    unsupported(
        m.subscribe("reminder", &Options::default().with_group("g"))
            .await
            .map(|_| ()),
    );
    unknown_subject(m.publish("typo", event("", ""), &after(100)).await);
    unsupported(as_durable(m.as_ref()).map(|_| ()));

    drop(a);
    drop(b);
    // The unwanted subject's payload was never claimed: nobody subscribed to
    // it. It is the leftover a missed expiry leaves, and why a subscriber has
    // to be attached when a scheduled message fires.
    provider.delete(&stream.id).await.unwrap();
}

// --- durable (Redis Streams) --------------------------------------------------

#[tokio::test]
async fn durable_over_tls() {
    let _server = SERVER.read().await;
    let config = config();
    let provider = RedisProvider::durable(client().await, config.clone());
    let (stream, m) = create(&provider, &[SUBJECT, "other"]).await;
    let key = format!("{}:durable:s:{}:{SUBJECT}", config.prefix, stream.id);
    let d = as_durable(m.as_ref()).unwrap();
    let p = as_positioned(m.as_ref()).unwrap();

    // A history entry before any consumer exists.
    let history = m
        .publish(SUBJECT, event("history", ""), &Options::default())
        .await
        .unwrap();

    // Consume (Position::New) with a prefetch: history is skipped, new work
    // arrives at attempt 1 under its chosen id, and an ack clears it.
    let mut workers = d
        .consume(SUBJECT, "workers", &Options::default().with_prefetch(4))
        .await
        .unwrap();
    let chosen = m
        .publish(
            SUBJECT,
            event("ada", "created"),
            &Options::default()
                .with_id("evt-1")
                .with_partition_key("tenant-1"),
        )
        .await
        .unwrap();
    assert_eq!(chosen, "evt-1");
    let got = recv_delivery(&mut workers).await;
    assert_eq!(got.id(), "evt-1");
    assert_eq!(got.attempt(), 1);
    assert_eq!(user(&got), "ada");
    got.ack().await.unwrap();
    eventually("the ack to clear the pending list", || async {
        pending(&key, "workers").await == 0
    })
    .await;

    // Nak: the message stays pending and comes back with a rising attempt.
    let retry = m
        .publish(SUBJECT, event("retry", ""), &Options::default())
        .await
        .unwrap();
    let first = recv_delivery(&mut workers).await;
    assert_eq!(first.id(), retry);
    first.nak().await.unwrap();
    assert_eq!(pending(&key, "workers").await, 1);
    let second = recv_delivery(&mut workers).await;
    assert_eq!(second.id(), retry);
    assert!(second.attempt() >= 2, "attempt {}", second.attempt());
    second.ack().await.unwrap();

    // An unacked delivery whose consumer goes away is redelivered to the
    // next one under the name; an acked one is not.
    let lost = m
        .publish(SUBJECT, event("lost", ""), &Options::default())
        .await
        .unwrap();
    let taken = recv_delivery(&mut workers).await;
    assert_eq!(taken.id(), lost);
    drop(taken);
    drop(workers);
    let mut resumed = d
        .consume(SUBJECT, "workers", &Options::default())
        .await
        .unwrap();
    let back = recv_delivery(&mut resumed).await;
    assert_eq!(back.id(), lost);
    assert!(back.attempt() >= 2);
    back.ack().await.unwrap();
    silent(&mut resumed, Duration::from_secs(1)).await;

    // Positioned: Earliest replays history under a new name; New skips it.
    let mut replay = p
        .consume_from(SUBJECT, "replay", Position::Earliest, &Options::default())
        .await
        .unwrap();
    let mut replayed = Vec::new();
    for _ in 0..4 {
        let delivery = recv_delivery(&mut replay).await;
        replayed.push(delivery.id().to_owned());
        delivery.ack().await.unwrap();
    }
    assert_eq!(replayed, [history, "evt-1".to_owned(), retry, lost]);
    silent(&mut replay, Duration::from_millis(500)).await;
    let mut tail = p
        .consume_from(SUBJECT, "tail", Position::New, &Options::default())
        .await
        .unwrap();
    silent(&mut tail, Duration::from_secs(1)).await;
    // Both hold nothing, so dropping them now (before more is published,
    // which they would take and hold) lets them leave their groups.
    // A consumer dropped mid-read can still be handed an entry the server
    // was already serving it, so wait for both to leave before publishing.
    drop(replay);
    drop(tail);
    for group in ["replay", "tail"] {
        eventually(&format!("{group} to leave its group"), || async {
            consumers(&key, group).await == 0
        })
        .await;
    }

    // Batch: one pipeline, in order, split across the consumers that read.
    let ids = as_batch(m.as_ref())
        .unwrap()
        .publish_batch(
            SUBJECT,
            (0..6).map(|i| event("batch", &i.to_string())).collect(),
            &Options::default(),
        )
        .await
        .unwrap();
    let mut seen = HashSet::new();
    while seen.len() < ids.len() {
        let delivery = recv_delivery(&mut resumed).await;
        seen.insert(delivery.id().to_owned());
        delivery.ack().await.unwrap();
    }
    assert_eq!(seen, ids.iter().cloned().collect());
    unsupported(
        as_batch(m.as_ref())
            .unwrap()
            .publish_batch(
                SUBJECT,
                vec![event("", "")],
                &Options::default().with_id("x"),
            )
            .await,
    );

    // The plain subscriber tails the log.
    let mut tailing = m.subscribe(SUBJECT, &Options::default()).await.unwrap();
    m.publish(SUBJECT, event("tail", ""), &Options::default())
        .await
        .unwrap();
    assert_eq!(user(&recv(&mut tailing).await), "tail");
    recv_delivery(&mut resumed).await.ack().await.unwrap();

    // Refusals: TTL, group, empty name, undeclared subject.
    unsupported(
        m.publish(
            SUBJECT,
            event("", ""),
            &Options::default().with_ttl(Duration::from_secs(1)),
        )
        .await,
    );
    unsupported(
        d.consume(SUBJECT, "workers", &Options::default().with_group("g"))
            .await
            .map(|_| ()),
    );
    unsupported(
        m.subscribe(SUBJECT, &Options::default().with_group("g"))
            .await
            .map(|_| ()),
    );
    unsupported(
        d.consume(SUBJECT, "", &Options::default())
            .await
            .map(|_| ()),
    );
    unknown_subject(
        d.consume("typo", "workers", &Options::default())
            .await
            .map(|_| ()),
    );
    unknown_subject(m.publish("typo", event("", ""), &Options::default()).await);

    // Dropping consumers removes their identities from the groups once they
    // hold nothing.
    assert_eq!(pending(&key, "workers").await, 0);
    drop(resumed);
    drop(tailing);
    // The first "workers" identity was dropped while it still held a
    // message, so it stayed in the roster for that message to be reclaimed
    // from; it holds nothing now but is not removed after the fact. The
    // resumed identity, which held nothing, has left.
    eventually("workers to keep 1 consumer", || async {
        consumers(&key, "workers").await == 1
    })
    .await;

    // Delete removes the stream; as in Go, the subject logs are data and are
    // not removed with it.
    provider.delete(&stream.id).await.unwrap();
    not_found(provider.get(&stream.id).await);
    assert!(exists(&key).await);
    let _: () = command(redis::cmd("DEL").arg(&key)).await;
}

/// The retention cap trims the log, approximately, as Go's WithMaxLen does.
#[tokio::test]
async fn max_len_caps_the_log_over_tls() {
    let _server = SERVER.read().await;
    let config = RedisConfig {
        max_len: Some(10),
        ..config()
    };
    let provider = RedisProvider::durable(client().await, config.clone());
    let (stream, m) = create(&provider, &[SUBJECT]).await;
    let batch = as_batch(m.as_ref()).unwrap();
    for _ in 0..10 {
        batch
            .publish_batch(
                SUBJECT,
                (0..100).map(|i| event("u", &i.to_string())).collect(),
                &Options::default(),
            )
            .await
            .unwrap();
    }
    let key = format!("{}:durable:s:{}:{SUBJECT}", config.prefix, stream.id);
    let len: u64 = command(redis::cmd("XLEN").arg(&key)).await;
    assert!(len < 1000, "the log kept {len} entries");
    provider.delete(&stream.id).await.unwrap();
    let _: () = command(redis::cmd("DEL").arg(&key)).await;
}

// --- headers, expiry, retain, delayed nak ------------------------------------

fn headers<const N: usize>(pairs: [(&str, &str); N]) -> Headers {
    pairs.into_iter().collect()
}

/// A stream on SUBJECT stamping `lang` and `site` on everything.
fn with_defaults() -> Stream {
    Stream::named("e2e-headers")
        .subject(SUBJECT)
        .header("lang", "rust")
        .header("site", "plant-3")
}

fn defaults() -> Headers {
    headers([("lang", "rust"), ("site", "plant-3")])
}

/// What a publish with [`override_opts`] arrives with.
fn overridden() -> Headers {
    headers([("lang", "rust"), ("site", "plant-9"), ("trace", "t-1")])
}

fn override_opts() -> Options {
    Options::default()
        .header("site", "plant-9")
        .header("trace", "t-1")
}

async fn create_stream(provider: &RedisProvider, stream: Stream) -> (Stream, Arc<dyn Manager>) {
    let stream = provider.create(stream).await.unwrap();
    let manager = provider.bind(&stream.id).await.unwrap();
    (stream, manager)
}

fn invalid<T: std::fmt::Debug>(result: streams::Result<T>) {
    assert!(
        matches!(result, Err(StreamError::InvalidHeader(_))),
        "expected InvalidHeader, got {result:?}"
    );
}

/// Headers in all three modes as the ACL-confined `streams` user: stream
/// defaults arrive, a publish's own override them, durable ones are entry
/// fields any client sees, and a header-less message is still the frame
/// runtime-go reads.
#[tokio::test]
async fn headers_over_tls() {
    let _server = SERVER.read().await;
    let config = config();
    let client = client().await;

    // Pub/sub, with a raw subscriber reading the frames.
    let immediate = RedisProvider::new(client.clone(), config.clone());
    let (stream, m) = create_stream(&immediate, with_defaults()).await;
    assert_eq!(immediate.get(&stream.id).await.unwrap().headers, defaults());
    let mut messages = m.subscribe(SUBJECT, &Options::default()).await.unwrap();
    m.publish(SUBJECT, event("a", ""), &Options::default())
        .await
        .unwrap();
    assert_eq!(recv(&mut messages).await.headers(), &defaults());
    m.publish(SUBJECT, event("b", ""), &override_opts())
        .await
        .unwrap();
    let got = recv(&mut messages).await;
    assert_eq!(got.headers(), &overridden());
    assert_eq!(user(&got), "b");
    drop(messages);

    let (plain, p) = create(&immediate, &[SUBJECT]).await;
    let channel = format!("{}:stream:ch:{}:{SUBJECT}", config.prefix, plain.id);
    let mut raw = tls_client(true).get_async_pubsub().await.unwrap();
    raw.subscribe(&channel).await.unwrap();
    let mut raw = raw.into_on_message();
    let payload = event("go", "");
    let id = p
        .publish(SUBJECT, payload.clone(), &Options::default())
        .await
        .unwrap();
    let frame = tokio::time::timeout(WITHIN, raw.next())
        .await
        .unwrap()
        .unwrap()
        .get_payload_bytes()
        .to_vec();
    assert_eq!(
        frame,
        streams::provider::pack(&id, &payload),
        "not Go's frame"
    );
    drop(raw);

    // Scheduled.
    let scheduled = RedisProvider::scheduled(client.clone(), config.clone());
    let (_, s) = create_stream(&scheduled, with_defaults()).await;
    let mut fired = s.subscribe(SUBJECT, &Options::default()).await.unwrap();
    s.publish(
        SUBJECT,
        event("later", ""),
        &override_opts().with_ttl(Duration::from_millis(200)),
    )
    .await
    .unwrap();
    assert_eq!(recv(&mut fired).await.headers(), &overridden());
    drop(fired);

    // Durable: entry fields beside a version-1 `msg`, read back on a
    // redelivery.
    let durable = RedisProvider::durable(client.clone(), config.clone());
    let (dstream, d) = create_stream(&durable, with_defaults()).await;
    let key = format!("{}:durable:s:{}:{SUBJECT}", config.prefix, dstream.id);
    let mut deliveries = as_durable(d.as_ref())
        .unwrap()
        .consume(SUBJECT, "workers", &Options::default())
        .await
        .unwrap();
    let id = d
        .publish(SUBJECT, event("d", ""), &override_opts())
        .await
        .unwrap();
    let entries: redis::streams::StreamRangeReply =
        command(redis::cmd("XRANGE").arg(&key).arg("-").arg("+")).await;
    let mut fields: Vec<&str> = entries.ids[0].map.keys().map(String::as_str).collect();
    fields.sort_unstable();
    assert_eq!(fields, ["h:lang", "h:site", "h:trace", "msg"]);
    let frame: Vec<u8> = entries.ids[0].get("msg").unwrap();
    assert_eq!(frame[1], 1);
    let first = recv_delivery(&mut deliveries).await;
    assert_eq!(first.headers(), &overridden());
    first.nak().await.unwrap();
    let again = recv_delivery(&mut deliveries).await;
    assert_eq!(again.id(), id);
    assert!(again.attempt() >= 2);
    assert_eq!(again.headers(), &overridden());
    again.ack().await.unwrap();
    drop(deliveries);

    // Invalid headers fail where they are set, in every mode.
    for provider in [&immediate, &scheduled, &durable] {
        invalid(
            provider
                .create(Stream::named("x").subject(SUBJECT).header("a:b", "1"))
                .await,
        );
        invalid(
            provider
                .update(&dstream.id, Stream::named("x").header("bad key", "1"))
                .await,
        );
    }
    invalid(
        d.publish(
            SUBJECT,
            event("", ""),
            &Options::default().header("k", "a\nb"),
        )
        .await,
    );

    for (provider, id) in [
        (&immediate, &stream.id),
        (&immediate, &plain.id),
        (&durable, &dstream.id),
    ] {
        provider.delete(id).await.unwrap();
    }
    let _: () = command(redis::cmd("DEL").arg(&key)).await;
}

#[tokio::test]
async fn expiry_and_retain_are_refused_over_tls() {
    let _server = SERVER.read().await;
    let config = config();
    let client = client().await;
    let modes = [
        (
            RedisProvider::new(client.clone(), config.clone()),
            Options::default(),
        ),
        (
            RedisProvider::scheduled(client.clone(), config.clone()),
            after(200),
        ),
        (
            RedisProvider::durable(client.clone(), config.clone()),
            Options::default(),
        ),
    ];
    for (provider, base) in modes {
        let (stream, m) = create(&provider, &[SUBJECT]).await;
        let batch = as_batch(m.as_ref()).unwrap();
        for opts in [
            base.clone().with_expiry(Duration::from_secs(5)),
            base.clone().with_retain(),
        ] {
            unsupported(m.publish(SUBJECT, event("", ""), &opts).await);
            unsupported(
                batch
                    .publish_batch(SUBJECT, vec![event("", "")], &opts)
                    .await,
            );
        }
        provider.delete(&stream.id).await.unwrap();
    }
}

/// A delayed nak over TLS, as `streams` (whose ACL grants XCLAIM for it):
/// held back for the delay, well short of a plain nak's reclaim interval, and
/// counted as one redelivery.
#[tokio::test]
async fn a_delayed_nak_over_tls() {
    let _server = SERVER.read().await;
    let reclaim = Duration::from_secs(8);
    let delay = Duration::from_millis(1500);
    let config = RedisConfig {
        reclaim_after: reclaim,
        ..config()
    };
    let durable = RedisProvider::durable(client().await, config.clone());
    let (stream, m) = create_stream(&durable, with_defaults()).await;
    let mut deliveries = as_durable(m.as_ref())
        .unwrap()
        .consume(SUBJECT, "workers", &Options::default())
        .await
        .unwrap();
    let id = m
        .publish(SUBJECT, event("busy", ""), &Options::default())
        .await
        .unwrap();
    let first = recv_delivery(&mut deliveries).await;
    assert!(first.can_delay_redelivery());
    let naked_at = tokio::time::Instant::now();
    first.nak_after(delay).await.unwrap();
    silent(&mut deliveries, delay - Duration::from_millis(100)).await;
    let again = recv_delivery(&mut deliveries).await;
    let waited = naked_at.elapsed();
    assert_eq!(again.id(), id);
    assert!(waited >= delay, "redelivered after {waited:?}");
    assert!(waited < reclaim - Duration::from_secs(2), "{waited:?}");
    assert_eq!(again.attempt(), 2);
    assert_eq!(again.headers(), &defaults());

    // Longer than the reclaim interval cannot be honoured.
    unsupported(again.nak_after(reclaim * 2).await);
    drop(deliveries);
    durable.delete(&stream.id).await.unwrap();
    let _: () = command(redis::cmd("DEL").arg(format!(
        "{}:durable:s:{}:{SUBJECT}",
        config.prefix, stream.id
    )))
    .await;
}

// --- a broker restart -----------------------------------------------------------

/// Restarts the container and waits until it answers over mTLS again.
async fn restart_broker() {
    let status = Command::new("docker")
        .args(["restart", &container()])
        .status()
        .expect("docker is not runnable");
    assert!(status.success(), "docker restart failed: {status}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let health = Command::new("docker")
            .args(["inspect", "-f", "{{.State.Health.Status}}", &container()])
            .output()
            .expect("docker is not runnable");
        let healthy = String::from_utf8_lossy(&health.stdout).trim() == "healthy";
        let answers = async {
            let mut conn = tls_client(true)
                .get_multiplexed_async_connection_with_config(&patient())
                .await?;
            redis::cmd("PING").query_async::<String>(&mut conn).await
        };
        if healthy && matches!(answers.await, Ok(ref pong) if pong == "PONG") {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{} did not come back healthy",
            container()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// What survives `docker restart` of a server with an fsynced AOF.
///
/// Every provider's shared connection predates the restart, and the first
/// command each sends afterwards succeeds without a retry: a read, and on the
/// immediate stream a PUBLISH, which the driver never repeats, so it can only
/// succeed because the dead connection was replaced before it was sent.
///
/// Durable (Redis Streams): the stream's metadata, the log, the consumer
/// group's position, and its pending list all survive. A consumer that was
/// running across the restart reconnects on its own, gets back the message it
/// held unacknowledged (attempt >= 2), never sees the one it acknowledged, and
/// keeps receiving new work; a fresh process under the same name sees the
/// same group.
///
/// Scheduled (TTL expiry): the timer survives, because the AOF stores the
/// pending key's absolute expiry, so a message due after the restart fires to
/// a subscriber attached after the restart. A message that comes due while no
/// subscriber is attached, which includes the downtime itself, is never
/// delivered: Redis publishes the expiry event to nobody and does not replay
/// it. Its payload key stays behind, orphaned. That is the mode's contract,
/// not something durability settings can change.
#[tokio::test]
async fn survives_a_broker_restart() {
    if std::env::var("STREAMS_E2E_RESTART").as_deref() != Ok("1") {
        eprintln!(
            "skipped: set STREAMS_E2E_RESTART=1 to restart {}",
            container()
        );
        return;
    }
    let _server = SERVER.write().await;
    let config = config();
    let client = client().await;

    // Durable setup: one acked, one held unacked by a consumer that stays
    // attached through the restart.
    let durable = RedisProvider::durable(client.clone(), config.clone());
    let (stream, m) = create_stream(&durable, with_defaults()).await;
    let key = format!("{}:durable:s:{}:{SUBJECT}", config.prefix, stream.id);
    let d = as_durable(m.as_ref()).unwrap();
    let mut workers = d
        .consume(SUBJECT, "workers", &Options::default())
        .await
        .unwrap();
    let acked = m
        .publish(SUBJECT, event("acked", ""), &Options::default())
        .await
        .unwrap();
    let held = m
        .publish(SUBJECT, event("held", ""), &override_opts())
        .await
        .unwrap();
    let first = recv_delivery(&mut workers).await;
    assert_eq!(first.id(), acked);
    first.ack().await.unwrap();
    let second = recv_delivery(&mut workers).await;
    assert_eq!(second.id(), held);
    // Held: neither acked nor naked, the way a crashed handler leaves it.
    std::mem::forget(second);
    assert_eq!(pending(&key, "workers").await, 1);

    // Scheduled setup: no subscriber attached. One message comes due during
    // the restart, one well after it.
    let scheduled = RedisProvider::scheduled(client.clone(), config.clone());
    let (reminders, s) = create(&scheduled, &["reminder"]).await;
    let missed = s
        .publish("reminder", event("missed", ""), &after(500))
        .await
        .unwrap();
    let later = s
        .publish("reminder", event("later", ""), &after(12_000))
        .await
        .unwrap();
    let scheduled_at = tokio::time::Instant::now();

    // Immediate setup: a stream whose provider has used its shared connection.
    let immediate = RedisProvider::new(client.clone(), config.clone());
    let (live, p) = create(&immediate, &[SUBJECT]).await;

    restart_broker().await;

    // No retries from here on: each of these is the first command its
    // provider sends after the restart, on a shared connection opened before
    // it. Everything durable is still on the server.
    assert_eq!(durable.get(&stream.id).await.unwrap(), stream);
    assert_eq!(scheduled.get(&reminders.id).await.unwrap(), reminders);

    // The first command on the immediate provider is a publish, which is
    // never sent twice. The subscription opens its own connection, so the
    // publish is still the shared connection's first command. It arrives
    // exactly once.
    let mut listener = p.subscribe(SUBJECT, &Options::default()).await.unwrap();
    let once = p
        .publish(SUBJECT, event("after-restart", ""), &Options::default())
        .await
        .expect("the first publish after a restart failed");
    assert_eq!(recv(&mut listener).await.id(), once);
    silent(&mut listener, Duration::from_millis(500)).await;
    drop(listener);
    immediate.delete(&live.id).await.unwrap();
    assert_eq!(
        pending(&key, "workers").await,
        1,
        "the pending list was lost"
    );
    assert_eq!(command::<u64>(redis::cmd("XLEN").arg(&key)).await, 2);

    // The consumer that lived through the restart reconnects by itself: the
    // held message comes back, the acked one does not, and new work flows.
    let back = recv_delivery(&mut workers).await;
    assert_eq!(back.id(), held, "the unacked message was not redelivered");
    assert!(back.attempt() >= 2, "attempt {}", back.attempt());
    assert_eq!(back.headers(), &overridden(), "headers lost in the restart");
    back.ack().await.unwrap();
    let fresh = m
        .publish(SUBJECT, event("after", ""), &Options::default())
        .await
        .unwrap();
    let next = recv_delivery(&mut workers).await;
    assert_eq!(next.id(), fresh, "the acked message came back");
    assert_eq!(next.attempt(), 1);
    assert_eq!(
        next.headers(),
        &defaults(),
        "stream headers lost in the restart"
    );
    next.ack().await.unwrap();
    silent(&mut workers, Duration::from_secs(1)).await;
    drop(workers);

    // A new process with a new client under the same name: same group, same
    // position, nothing outstanding.
    let reborn = RedisProvider::durable(self::client().await, config.clone());
    let m2 = reborn.bind(&stream.id).await.unwrap();
    let mut again = as_durable(m2.as_ref())
        .unwrap()
        .consume(SUBJECT, "workers", &Options::default())
        .await
        .unwrap();
    silent(&mut again, Duration::from_secs(1)).await;
    drop(again);

    // Scheduled: the late timer survived; the one that came due while nobody
    // listened is gone for good, its payload orphaned.
    let mut subscriber = s.subscribe("reminder", &Options::default()).await.unwrap();
    let pending_later = format!(
        "{}:notify:pending:{}:reminder:{later}",
        config.prefix, reminders.id
    );
    assert!(
        exists(&pending_later).await || scheduled_at.elapsed() >= Duration::from_secs(12),
        "the scheduled timer did not survive the restart"
    );
    let fired = tokio::time::timeout(Duration::from_secs(20), subscriber.next())
        .await
        .expect("the scheduled message due after the restart never fired")
        .expect("the scheduled subscription ended");
    assert_eq!(fired.id(), later);
    assert!(scheduled_at.elapsed() >= Duration::from_secs(12));
    silent(&mut subscriber, Duration::from_secs(1)).await;
    assert!(
        exists(&format!("{}:notify:payload:{missed}", config.prefix)).await,
        "expected the missed message's payload to be left behind"
    );
    drop(subscriber);

    // Clean up what delete does not own.
    durable.delete(&stream.id).await.unwrap();
    scheduled.delete(&reminders.id).await.unwrap();
    let _: () = command(
        redis::cmd("DEL")
            .arg(&key)
            .arg(format!("{}:notify:payload:{missed}", config.prefix)),
    )
    .await;
}
