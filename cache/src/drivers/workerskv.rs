//! Cloudflare Workers KV client, Provider, and primitive capabilities.
//!
//! KV is reached through the REST API by Cloudflare's official Rust SDK, the
//! `cloudflare` crate. It is a store replicated to every Cloudflare location and
//! read from the nearest one, which makes it very good at a value read often,
//! everywhere, and written rarely, and shapes everything else about it:
//!
//! - **Eventually consistent.** A write is visible at once where it was made
//!   and up to a minute later elsewhere, and a key takes at most one write a
//!   second.
//! - **No conditional writes.** `add` and `replace` return `Unsupported`, so
//!   Document updates refuse and Aside collapses loads within a process instead
//!   of locking across them. A read followed by a write is not a conditional
//!   write, and under eventual consistency it is not even close to one.
//! - **No lease extension.** `touch` returns `Unsupported`: a lease changes
//!   only by rewriting the value, and a rewrite racing a delete brings back
//!   what was deleted.
//! - **Sixty-second floor.** KV expires nothing sooner than sixty seconds
//!   after it was written, so a shorter lease is lengthened to sixty seconds
//!   rather than refused. For a cache an entry outliving its lease is a stale
//!   read; refusing would make every short-lived value unwritable here.
//!
//! Leases and Scanner are declared: a key listing reports each key's expiry,
//! and lists by prefix. Bulk is not, because the SDK has no bulk read, and no
//! strategy takes one on a backend without sets.
//!
//! This is the Rust twin of runtime-go's `cache/workerskv`.

use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use cloudflare::{
    endpoints::workerskv::{
        delete_bulk::DeleteBulk,
        get_namespace::GetNamespace,
        list_namespace_keys::{ListNamespaceKeys, ListNamespaceKeysParams},
        read_key::ReadKey,
        write_key::{WriteKey, WriteKeyBody, WriteKeyParams},
    },
    framework::{client::async_api::Client, response::ApiFailure},
};

use crate::{
    CacheError, Config, Result,
    core::{
        Capabilities, DB, DatabaseSpec, Driver, Keyspace, Leases, Provider, Scanner,
        build_database, check_known, check_namespace, drop_database,
    },
};

/// Stable backend identity exposed by Workers KV primitives.
const BACKEND: &str = "workerskv";
/// The shortest expiry KV accepts, in seconds.
const MIN_TTL_SECONDS: u64 = 60;
/// Keys per bulk delete, the most one request may carry.
const BULK_DELETE: usize = 10_000;
/// Keys per listing page, the most one request may return.
const LIST_LIMIT: u16 = 1000;

/// Which account and namespace a [`WorkersKvClient`] reads and writes.
#[derive(Clone, Debug, Default)]
pub struct WorkersKvConfig {
    /// The account that owns the namespace.
    pub account_id: String,
    /// The KV namespace: the store the server keeps apart from every other.
    pub namespace_id: String,
}

/// A Workers KV namespace reached through a caller-owned SDK client.
///
/// **Use when**: Constructing one long-lived client shared by a
/// [`WorkersKvProvider`]. The SDK client is the caller's; this type never
/// closes it, and holds nothing of its own that needs closing.
pub struct WorkersKvClient {
    /// The official SDK client, carrying the API token.
    sdk: Arc<Client>,
    /// The account that owns the namespace.
    account: String,
    /// The namespace every operation addresses.
    namespace: String,
}

impl WorkersKvClient {
    /// Checks the namespace is reachable and returns a client over it.
    ///
    /// **Cost**: One API request, reading the namespace itself, so a token
    /// without access or a wrong id is learned here rather than in a request
    /// handler.
    pub async fn connect(sdk: Arc<Client>, config: WorkersKvConfig) -> Result<Self> {
        if config.account_id.is_empty() || config.namespace_id.is_empty() {
            return Err(CacheError::Internal(
                "workerskv: an account id and a namespace id are required".to_owned(),
            ));
        }
        sdk.request(&GetNamespace {
            account_identifier: &config.account_id,
            namespace_identifier: &config.namespace_id,
        })
        .await
        .map_err(|e| {
            CacheError::Internal(format!(
                "workerskv: cannot reach namespace {}: {e}",
                config.namespace_id
            ))
        })?;
        Ok(Self {
            sdk,
            account: config.account_id,
            namespace: config.namespace_id,
        })
    }

    /// Returns the SDK client, for anything this crate does not do.
    pub fn sdk(&self) -> &Arc<Client> {
        &self.sdk
    }
}

/// Cache Provider over a caller-owned Workers KV client.
///
/// A database name is a key segment, exactly as on every other backend. The
/// server enforces nothing between names; a separate namespace, chosen per
/// client, is the boundary it does enforce.
pub struct WorkersKvProvider {
    /// Caller-owned root client; the Provider and selected DBs never close it.
    client: Arc<WorkersKvClient>,
    /// Shared strategy defaults and optional named-database allowlist.
    config: Config,
}

impl WorkersKvProvider {
    /// Binds cache policy to a caller-owned Workers KV client.
    ///
    /// **Cost**: Local allocation only; no API request.
    pub fn new(client: Arc<WorkersKvClient>, config: Config) -> Self {
        Self { client, config }
    }

    /// Wires Driver, Leases and Scanner over the shared client.
    fn database(&self, namespace: String, database: usize, embed_db: bool) -> DB {
        let primitives = Arc::new(KvPrimitives {
            client: self.client.clone(),
        });
        let capabilities = Capabilities::new()
            .with_leases(primitives.clone())
            .with_scanner(primitives.clone());
        build_database(
            primitives,
            capabilities,
            DatabaseSpec {
                prefix: self.config.prefix.clone(),
                namespace,
                database,
                embed_db,
                default_ttl: self.config.default_ttl,
                default_stale: self.config.default_stale,
                concurrency: self.config.concurrency,
                require_ttl: self.config.require_ttl,
                ..DatabaseSpec::default()
            },
        )
    }
}

#[async_trait]
impl Provider for WorkersKvProvider {
    /// Selects a named key namespace. [`WorkersKvClient::connect`] already
    /// reached the namespace, so nothing is asked of the server here.
    async fn set_database(&self, name: &str) -> Result<DB> {
        check_namespace(name)?;
        check_known(name, &self.config.databases)?;
        Ok(self.database(name.to_owned(), 0, false))
    }

    /// Selects an emulated numeric database: KV has no numbered databases, so
    /// the index becomes a key segment and the server enforces nothing between
    /// two of them.
    async fn select_index(&self, index: usize) -> Result<DB> {
        Ok(self.database(String::new(), index, true))
    }

    /// Lists a named database's keys by prefix and deletes them in bulk. A
    /// listing lags writes like everything else in KV, so a key written in the
    /// last minute may survive the drop.
    async fn drop_database(&self, name: &str) -> Result<usize> {
        check_namespace(name)?;
        let keyspace = Keyspace::new(&self.config.prefix, name, 0, false);
        let primitives = KvPrimitives {
            client: self.client.clone(),
        };
        drop_database(&primitives, &primitives, keyspace.head()).await
    }

    /// Returns the stable Workers KV identity without I/O.
    fn backend(&self) -> &str {
        BACKEND
    }
}

/// Workers KV implementation of Driver, Leases and Scanner.
pub(crate) struct KvPrimitives {
    /// Shared root client; this adapter never closes it.
    client: Arc<WorkersKvClient>,
}

impl KvPrimitives {
    /// Lists every key under a literal prefix, following the cursor to the end.
    async fn list(&self, prefix: &str, limit: u16) -> Result<Vec<(String, Option<i64>)>> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = self
                .client
                .sdk
                .request(&ListNamespaceKeys {
                    account_identifier: &self.client.account,
                    namespace_identifier: &self.client.namespace,
                    params: ListNamespaceKeysParams {
                        limit: Some(limit),
                        cursor: cursor.clone(),
                        prefix: (!prefix.is_empty()).then(|| prefix.to_owned()),
                    },
                })
                .await
                .map_err(failure)?;
            out.extend(
                page.result
                    .into_iter()
                    .map(|k| (k.name, k.expiration.map(|e| e.timestamp()))),
            );
            cursor = page
                .result_info
                .as_ref()
                .and_then(|info| info.get("cursor"))
                .and_then(|c| c.as_str())
                .filter(|c| !c.is_empty())
                .map(str::to_owned);
            if cursor.is_none() || limit < LIST_LIMIT {
                return Ok(out);
            }
        }
    }
}

#[async_trait]
impl Driver for KvPrimitives {
    fn name(&self) -> &str {
        BACKEND
    }

    /// Reads raw bytes. A missing or expired key is `NotFound`.
    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        self.client
            .sdk
            .request(&ReadKey {
                account_identifier: &self.client.account,
                namespace_identifier: &self.client.namespace,
                key,
            })
            .await
            .map_err(failure)
    }

    /// Writes unconditionally, lengthening a lease under sixty seconds to it.
    async fn set(&self, key: &str, value: &[u8], ttl: Duration) -> Result<()> {
        self.client
            .sdk
            .request(&WriteKey {
                account_identifier: &self.client.account,
                namespace_identifier: &self.client.namespace,
                key,
                params: WriteKeyParams {
                    expiration: None,
                    expiration_ttl: expiry(ttl),
                },
                body: WriteKeyBody::Value(value.to_vec()),
            })
            .await
            .map_err(failure)?;
        Ok(())
    }

    /// Refuses: KV has no write-if-absent.
    async fn add(&self, _key: &str, _value: &[u8], _ttl: Duration) -> Result<bool> {
        Err(CacheError::Unsupported)
    }

    /// Refuses: KV has no write-if-present.
    async fn replace(&self, _key: &str, _value: &[u8], _ttl: Duration) -> Result<bool> {
        Err(CacheError::Unsupported)
    }

    /// Removes keys in bulk; an absent key is not an error.
    async fn delete(&self, keys: &[&str]) -> Result<()> {
        for chunk in keys.chunks(BULK_DELETE) {
            self.client
                .sdk
                .request(&DeleteBulk {
                    account_identifier: &self.client.account,
                    namespace_identifier: &self.client.namespace,
                    bulk_keys: chunk.iter().map(|k| (*k).to_owned()).collect(),
                })
                .await
                .map_err(failure)?;
        }
        Ok(())
    }

    /// A read whose value is thrown away. The metadata endpoint looks cheaper
    /// but answers about a key's metadata rather than the key.
    async fn exists(&self, key: &str) -> Result<bool> {
        match self.get(key).await {
            Ok(_) => Ok(true),
            Err(CacheError::NotFound) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Refuses: a lease changes only with the value.
    async fn touch(&self, _key: &str, _ttl: Duration) -> Result<()> {
        Err(CacheError::Unsupported)
    }
}

#[async_trait]
impl Leases for KvPrimitives {
    /// Reads the remaining lease from a listing, the one place KV reports a
    /// key's expiry without its value. Zero is a key that never expires.
    async fn ttl(&self, key: &str) -> Result<Duration> {
        let keys = self.list(key, 10).await?;
        let Some((_, expiration)) = keys.into_iter().find(|(name, _)| name == key) else {
            return Err(CacheError::NotFound);
        };
        let Some(at) = expiration else {
            return Ok(Duration::ZERO);
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_secs() as i64);
        if at <= now {
            return Err(CacheError::NotFound);
        }
        Ok(Duration::from_secs((at - now) as u64))
    }
}

#[async_trait]
impl Scanner for KvPrimitives {
    /// Lists the keys matching a glob. KV lists by prefix only, so the listing
    /// is cut by the glob's literal head and the rest is matched here.
    async fn scan(&self, pattern: &str) -> Result<Vec<String>> {
        let glob = parse_glob(pattern)?;
        let mut seen = HashSet::new();
        Ok(self
            .list(&glob.prefix(), LIST_LIMIT)
            .await?
            .into_iter()
            .map(|(name, _)| name)
            .filter(|name| glob.matches(name) && seen.insert(name.clone()))
            .collect())
    }
}

/// One element of a parsed glob.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Token {
    /// A character that must match itself, escaped or not.
    Literal(char),
    /// `?`: any one character.
    One,
    /// `*`: any run of characters.
    Any,
}

/// A Redis-style glob, limited to what KV can serve: `*`, `?` and `\` escapes.
struct Glob(Vec<Token>);

/// Parses a glob, refusing character classes rather than guessing at them.
fn parse_glob(pattern: &str) -> Result<Glob> {
    let mut tokens = Vec::new();
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        tokens.push(match c {
            '\\' => Token::Literal(chars.next().unwrap_or('\\')),
            '*' => Token::Any,
            '?' => Token::One,
            '[' => return Err(CacheError::Unsupported),
            c => Token::Literal(c),
        });
    }
    Ok(Glob(tokens))
}

impl Glob {
    /// The literal text before the first wildcard, which KV can list by.
    fn prefix(&self) -> String {
        self.0
            .iter()
            .map_while(|t| match t {
                Token::Literal(c) => Some(*c),
                _ => None,
            })
            .collect()
    }

    /// Whether `name` matches, backtracking to the last `*` on a mismatch.
    fn matches(&self, name: &str) -> bool {
        let (p, n): (&[Token], Vec<char>) = (&self.0, name.chars().collect());
        let (mut pi, mut ni, mut star, mut mark) = (0, 0, None, 0);
        while ni < n.len() {
            match p.get(pi) {
                Some(Token::Any) => {
                    star = Some(pi);
                    mark = ni;
                    pi += 1;
                }
                Some(Token::One) => {
                    pi += 1;
                    ni += 1;
                }
                Some(Token::Literal(c)) if *c == n[ni] => {
                    pi += 1;
                    ni += 1;
                }
                _ => match star {
                    Some(s) => {
                        pi = s + 1;
                        mark += 1;
                        ni = mark;
                    }
                    None => return false,
                },
            }
        }
        p[pi..].iter().all(|t| *t == Token::Any)
    }
}

/// A lease in whole seconds, rounded up and never under the KV minimum; `None`
/// for a key that never expires.
fn expiry(ttl: Duration) -> Option<i64> {
    if ttl.is_zero() {
        return None;
    }
    let seconds = ttl.as_secs() + u64::from(ttl.subsec_nanos() > 0);
    Some(seconds.max(MIN_TTL_SECONDS) as i64)
}

/// Maps an SDK failure: a 404 is KV saying the key is not there.
fn failure(error: ApiFailure) -> CacheError {
    match error {
        ApiFailure::Error(status, _) if status.as_u16() == 404 => CacheError::NotFound,
        other => CacheError::Internal(format!("{BACKEND}: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expiry_rounds_up_and_holds_the_floor() {
        assert_eq!(expiry(Duration::ZERO), None);
        assert_eq!(expiry(Duration::from_secs(5)), Some(60));
        assert_eq!(expiry(Duration::from_millis(90_500)), Some(91));
        assert_eq!(expiry(Duration::from_secs(3600)), Some(3600));
    }

    #[test]
    fn test_glob_prefix_unescapes_and_stops_at_a_wildcard() {
        let glob = parse_glob(r"app\*:orders:cache:*").unwrap();
        assert_eq!(glob.prefix(), "app*:orders:cache:");
        assert!(glob.matches("app*:orders:cache:x"));
        assert!(!glob.matches("appx:orders:cache:x"));
    }

    #[test]
    fn test_glob_matches_like_redis() {
        let glob = parse_glob("user:?").unwrap();
        assert!(glob.matches("user:1"));
        assert!(!glob.matches("user:10"));
        let glob = parse_glob("a*b*c").unwrap();
        assert!(glob.matches("abc"));
        assert!(glob.matches("aXXbYYc"));
        assert!(!glob.matches("aXXbYY"));
        assert!(matches!(
            parse_glob("user:[12]"),
            Err(CacheError::Unsupported)
        ));
    }
}
