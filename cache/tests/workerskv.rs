//! Workers KV driver against a stand-in for the KV REST API.
//!
//! The fake keeps state, so these tests drive the SDK end to end: key encoding
//! in the path, the expiry query parameter, cursor paging and the 404 a missing
//! key comes back as.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use cache::{
    CacheError, Config, Provider,
    core::Options,
    drivers::workerskv::{WorkersKvClient, WorkersKvConfig, WorkersKvProvider},
};
use cloudflare::framework::{
    Environment,
    auth::Credentials,
    client::{ClientConfig, async_api::Client},
};
use serde_json::{Value, json};
use wiremock::{MockServer, Request, Respond, ResponseTemplate, matchers::any};

const ACCOUNT: &str = "acct";
const NAMESPACE: &str = "ns1";

/// Stored value and absolute expiry, in Unix seconds.
type Entry = (Vec<u8>, Option<i64>);

#[derive(Clone, Default)]
struct FakeKv {
    values: Arc<Mutex<BTreeMap<String, Entry>>>,
    ttls: Arc<Mutex<Vec<i64>>>,
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn ok(result: Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({
        "success": true, "errors": [], "messages": [], "result": result,
    }))
}

fn fail(status: u16, code: u32, message: &str) -> ResponseTemplate {
    ResponseTemplate::new(status).set_body_json(json!({
        "success": false, "messages": [], "result": null,
        "errors": [{"code": code, "message": message}],
    }))
}

/// Percent-decodes one path segment, as the API does before looking a key up.
fn decode(segment: &str) -> String {
    let bytes = segment.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let Ok(b) = u8::from_str_radix(&segment[i + 1..i + 3], 16)
        {
            out.push(b);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap()
}

impl FakeKv {
    fn live(values: &BTreeMap<String, Entry>, key: &str) -> Option<Vec<u8>> {
        match values.get(key) {
            Some((v, None)) => Some(v.clone()),
            Some((v, Some(at))) if *at > now() => Some(v.clone()),
            _ => None,
        }
    }
}

impl Respond for FakeKv {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let base = format!("/accounts/{ACCOUNT}/storage/kv/namespaces/{NAMESPACE}");
        let path = request.url.path();
        let Some(rest) = path.strip_prefix(&base) else {
            return fail(404, 10013, "namespace not found");
        };
        let mut values = self.values.lock().unwrap();
        let method = request.method.as_str();
        match (method, rest) {
            ("GET", "") => ok(json!({"id": NAMESPACE, "title": "test"})),
            ("GET", "/keys") => {
                let query: BTreeMap<String, String> =
                    request.url.query_pairs().into_owned().collect();
                let prefix = query.get("prefix").cloned().unwrap_or_default();
                let start: usize = query
                    .get("cursor")
                    .and_then(|c| c.parse().ok())
                    .unwrap_or(0);
                let names: Vec<&String> = values
                    .keys()
                    .filter(|k| k.starts_with(&prefix) && Self::live(&values, k).is_some())
                    .collect();
                // Two a page, so following the cursor is exercised.
                let end = (start + 2).min(names.len());
                let page: Vec<Value> = names[start..end]
                    .iter()
                    .map(|k| match values[*k].1 {
                        Some(at) => json!({"name": k, "expiration": at}),
                        None => json!({"name": k}),
                    })
                    .collect();
                let cursor = if end < names.len() {
                    end.to_string()
                } else {
                    String::new()
                };
                ResponseTemplate::new(200).set_body_json(json!({
                    "success": true, "errors": [], "messages": [], "result": page,
                    "result_info": {"count": page.len(), "cursor": cursor},
                }))
            }
            ("DELETE", "/bulk") => {
                let keys: Vec<String> = serde_json::from_slice(&request.body).unwrap();
                for k in &keys {
                    values.remove(k);
                }
                ok(json!({"successful_key_count": keys.len(), "unsuccessful_keys": []}))
            }
            (_, rest) if rest.starts_with("/values/") => {
                let key = decode(&rest["/values/".len()..]);
                match method {
                    "GET" => match Self::live(&values, &key) {
                        Some(v) => ResponseTemplate::new(200).set_body_bytes(v),
                        None => fail(404, 10009, "get: 'key not found'"),
                    },
                    "PUT" => {
                        let ttl: Option<i64> = request
                            .url
                            .query_pairs()
                            .find(|(k, _)| k == "expiration_ttl")
                            .and_then(|(_, v)| v.parse().ok());
                        if let Some(ttl) = ttl {
                            if ttl < 60 {
                                return fail(400, 10019, "Expiration TTL must be at least 60.");
                            }
                            self.ttls.lock().unwrap().push(ttl);
                        }
                        values.insert(key, (request.body.clone(), ttl.map(|t| now() + t)));
                        // The live API answers a write with a null result,
                        // which is what the SDK's end-to-end suite expects.
                        ok(Value::Null)
                    }
                    _ => fail(405, 10000, "method"),
                }
            }
            _ => fail(404, 10000, "no route"),
        }
    }
}

async fn provider(config: Config) -> (FakeKv, MockServer, WorkersKvProvider) {
    let fake = FakeKv::default();
    let server = MockServer::start().await;
    wiremock::Mock::given(any())
        .respond_with(fake.clone())
        .mount(&server)
        .await;
    let sdk = Client::new(
        Credentials::UserAuthToken {
            token: "test".to_owned(),
        },
        ClientConfig::default(),
        Environment::Custom(format!("{}/", server.uri())),
    )
    .unwrap();
    let client = WorkersKvClient::connect(
        Arc::new(sdk),
        WorkersKvConfig {
            account_id: ACCOUNT.to_owned(),
            namespace_id: NAMESPACE.to_owned(),
        },
    )
    .await
    .unwrap();
    (
        fake,
        server,
        WorkersKvProvider::new(Arc::new(client), config),
    )
}

fn hour() -> Options {
    Options::default().with_ttl(Duration::from_secs(3600))
}

#[tokio::test]
async fn test_volatile_round_trip_with_awkward_keys() {
    let (_fake, _server, provider) = provider(Config {
        prefix: "app".to_owned(),
        ..Config::default()
    })
    .await;
    let db = provider.set_database("orders").await.unwrap();
    for key in ["plain", "with/slash", "with?query", "100%", "spa ce"] {
        db.volatile.set(key, key.as_bytes(), &hour()).await.unwrap();
        let mut got = Vec::new();
        db.volatile.get(key, &mut got).await.unwrap();
        assert_eq!(got, key.as_bytes(), "{key}");
    }
    db.volatile.delete("with/slash").await.unwrap();
    let mut got = Vec::new();
    assert!(matches!(
        db.volatile.get("with/slash", &mut got).await,
        Err(CacheError::NotFound)
    ));
}

#[tokio::test]
async fn test_short_leases_are_lengthened_and_reported() {
    let (fake, _server, provider) = provider(Config::default()).await;
    let db = provider.set_database("orders").await.unwrap();
    let short = Options::default().with_ttl(Duration::from_secs(5));
    db.volatile.set("k", b"1", &short).await.unwrap();
    let odd = Options::default().with_ttl(Duration::from_millis(90_500));
    db.volatile.set("k2", b"1", &odd).await.unwrap();
    assert_eq!(*fake.ttls.lock().unwrap(), vec![60, 91]);

    let left = db.volatile.ttl("k2").await.unwrap();
    assert!(left > Duration::from_secs(80) && left <= Duration::from_secs(91));

    let forever = Options::default().no_expiry();
    db.volatile.set("forever", b"1", &forever).await.unwrap();
    assert_eq!(db.volatile.ttl("forever").await.unwrap(), Duration::ZERO);
    assert!(matches!(
        db.volatile.ttl("absent").await,
        Err(CacheError::NotFound)
    ));
}

#[tokio::test]
async fn test_conditional_writes_refuse() {
    let (_fake, _server, provider) = provider(Config {
        default_ttl: Duration::from_secs(3600),
        ..Config::default()
    })
    .await;
    let db = provider.set_database("orders").await.unwrap();
    let id = db
        .document
        .create(b"\"a\"", &Options::default())
        .await
        .unwrap();
    assert!(matches!(
        db.document.update(&id, b"\"b\"", &Options::default()).await,
        Err(CacheError::Unsupported)
    ));
    assert!(matches!(
        db.volatile.touch("k", Duration::from_secs(3600)).await,
        Err(CacheError::Unsupported)
    ));
    let mut got = Vec::new();
    db.document.get(&id, &mut got).await.unwrap();
    assert_eq!(got, b"\"a\"");
}

#[tokio::test]
async fn test_scan_follows_the_cursor_and_matches_globs() {
    let (_fake, _server, provider) = provider(Config {
        prefix: "app".to_owned(),
        ..Config::default()
    })
    .await;
    let db = provider.set_database("orders").await.unwrap();
    for key in ["user:1", "user:2", "user:10", "order:1", "user-x"] {
        db.volatile.set(key, b"1", &hour()).await.unwrap();
    }
    let keys = db.volatile.scan("user:?").await.unwrap();
    assert_eq!(keys.len(), 2, "{keys:?}");
    assert_eq!(db.volatile.scan("*").await.unwrap().len(), 5);
}

#[tokio::test]
async fn test_drop_database_removes_only_its_keys() {
    let (fake, _server, provider) = provider(Config {
        prefix: "app".to_owned(),
        ..Config::default()
    })
    .await;
    let orders = provider.set_database("orders").await.unwrap();
    let users = provider.set_database("users").await.unwrap();
    for key in ["a", "b", "c"] {
        orders.volatile.set(key, b"1", &hour()).await.unwrap();
    }
    users.volatile.set("a", b"1", &hour()).await.unwrap();

    assert_eq!(provider.drop_database("orders").await.unwrap(), 3);
    assert_eq!(fake.values.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn test_connect_checks_the_namespace() {
    let server = MockServer::start().await;
    wiremock::Mock::given(any())
        .respond_with(FakeKv::default())
        .mount(&server)
        .await;
    let sdk = Arc::new(
        Client::new(
            Credentials::UserAuthToken {
                token: "test".to_owned(),
            },
            ClientConfig::default(),
            Environment::Custom(format!("{}/", server.uri())),
        )
        .unwrap(),
    );
    let wrong = WorkersKvConfig {
        account_id: ACCOUNT.to_owned(),
        namespace_id: "nope".to_owned(),
    };
    assert!(WorkersKvClient::connect(sdk, wrong).await.is_err());
}
