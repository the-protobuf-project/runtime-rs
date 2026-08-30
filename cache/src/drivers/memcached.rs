//! Async Memcached client, Go-compatible router, and primitive capabilities.
//!
//! This module owns transport setup, client-side server placement, Memcached
//! expiry encoding, direct protocol result mapping, and Provider selection.
//! Strategy policy, key qualification, and database construction remain in
//! core; the Provider only supplies backend facts and capabilities.
//!
//! Memcached 1.6 or newer is required because the selected Tokio client uses
//! the meta text protocol. One pooled client is retained per distinct server;
//! a private CRC32-IEEE modulo router preserves `gomemcache` placement across
//! Go-to-Rust migrations, including repeated server entries used as weights.

use std::{
    collections::HashMap,
    fmt::Display,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures::future::try_join_all;
use memcache::exp::{
    AsyncMetaClient, Delete, Get, GetStatus, MutationResult, MutationStatus, Op, OpResult, Set,
};
use tokio::sync::RwLock;

use crate::{
    CacheError, Config, DialTimeout, OperationTimeout, Result,
    core::{
        Bulk, Capabilities, DB, DatabaseSpec, Driver, Provider, build_database, check_known,
        check_namespace,
    },
};

/// Stable backend identity exposed by Memcached primitives.
const BACKEND: &str = "memcached";
/// Actual default used by the `gomemcache` version pinned by runtime-go.
const GO_DEFAULT_TIMEOUT: Duration = Duration::from_millis(500);
/// Actual default number of idle connections retained by `gomemcache`.
const GO_DEFAULT_MAX_IDLE: usize = 2;
/// Boundary where Memcached changes expiry from relative seconds to Unix time.
const THIRTY_DAYS_SECONDS: u64 = 60 * 60 * 24 * 30;
/// Probe key used by runtime-go to turn the first transport failure into a
/// construction error while treating an ordinary cache miss as reachability.
const PROBE_KEY: &str = "__runtime_go_probe";

/// Connection and pooling settings for an application-owned Memcached client.
///
/// Servers are routing slots. Repeating an address gives it proportional
/// weight while one physical pool is shared for identical address strings.
/// Memcached has no server-side cluster discovery, so changing slot order or
/// membership moves keys and should be treated as a cache migration.
///
/// **Trade-offs**: The meta protocol supplies genuinely asynchronous atomic
/// operations and pipelining, but requires Memcached 1.6+ and currently
/// supports TCP only in this backend.
///
/// **Scalability**: Each distinct server has an independent idle pool. Busy
/// pools open additional connections; `max_idle_connections` bounds only how
/// many are retained after use.
///
/// **Use when**: Constructing one long-lived [`MemcachedClient`] shared by a
/// later Memcached Provider.
#[derive(Clone, Debug)]
pub struct MemcachedConfig {
    /// Memcached `host:port` routing slots; at least one is required.
    pub servers: Vec<String>,
    /// Policy bounding DNS resolution and each lazy TCP dial.
    pub dial_timeout: DialTimeout,
    /// Policy bounding each command or per-server batch exchange.
    pub operation_timeout: OperationTimeout,
    /// Idle connections retained per distinct server; zero selects two.
    pub max_idle_connections: usize,
}

impl Default for MemcachedConfig {
    fn default() -> Self {
        Self {
            servers: Vec::new(),
            dial_timeout: DialTimeout::Default,
            operation_timeout: OperationTimeout::Default,
            max_idle_connections: 0,
        }
    }
}

/// Conditional behavior for one Memcached meta-set operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StoreCondition {
    /// Replace any prior value.
    Set,
    /// Store only while the key is absent.
    Add,
    /// Store only while the key exists.
    Replace,
}

/// Backend operation understood by the private transport seam.
///
/// Values and keys are owned so a router can group operations per server and
/// run those groups concurrently without borrowing a caller buffer.
#[derive(Clone, Debug, Eq, PartialEq)]
enum WireOperation {
    /// Read a value or metadata, optionally touching its expiry atomically.
    Get {
        /// Qualified backend key.
        key: String,
        /// Whether the reply must include the stored bytes.
        value: bool,
        /// New Memcached expiry when this read is also a Touch.
        touch: Option<u32>,
    },
    /// Store bytes with one native condition.
    Store {
        /// Qualified backend key.
        key: String,
        /// Raw cached bytes.
        value: Vec<u8>,
        /// Encoded Memcached expiry.
        expiry: u32,
        /// Atomic storage condition.
        condition: StoreCondition,
    },
    /// Delete one key.
    Delete {
        /// Qualified backend key.
        key: String,
    },
}

impl WireOperation {
    /// Returns the routing key without copying it.
    fn key(&self) -> &str {
        match self {
            Self::Get { key, .. } | Self::Store { key, .. } | Self::Delete { key } => key,
        }
    }
}

/// Transport-independent reply used by primitives and scripted tests.
#[derive(Clone, Debug, Eq, PartialEq)]
enum WireReply {
    /// A live read; metadata-only reads intentionally carry no bytes.
    Hit(Option<Vec<u8>>),
    /// A missing or expired item.
    Miss,
    /// A mutation was applied.
    Stored,
    /// Add found an existing item.
    AlreadyExists,
}

/// Private transport and lifecycle seam for Memcached operations.
///
/// Production uses a managed CRC32 router. Tests substitute scripted replies,
/// keeping unit tests deterministic and independent of a Memcached service.
#[async_trait]
trait CommandExecutor: Send + Sync {
    /// Executes one operation against its selected server.
    async fn execute(&self, operation: WireOperation) -> Result<WireReply>;

    /// Executes ordered operations in per-server pipelines.
    async fn execute_batch(&self, operations: Vec<WireOperation>) -> Result<Vec<WireReply>>;

    /// Stops new admission and releases the owned router locally.
    async fn close(&self);
}

/// One distinct physical server and its cloneable asynchronous connection pool.
struct ServerClient {
    /// Caller-supplied address used only for safe error context.
    address: String,
    /// Tokio meta-protocol client; clones share this server's idle pool.
    client: AsyncMetaClient,
}

/// Immutable CRC32 routing table shared by admitted operations.
struct Router {
    /// One physical client per distinct configured address string.
    servers: Vec<ServerClient>,
    /// Configured routing slots mapped to indexes in `servers`.
    slots: Vec<usize>,
}

impl Router {
    /// Chooses the physical server using Go's CRC32-IEEE modulo rule.
    fn server_index(&self, key: &str) -> usize {
        let slot = route_slot(key.as_bytes(), self.slots.len());
        self.slots[slot]
    }

    /// Executes one operation and annotates transport/protocol failures with
    /// the selected server without including values.
    async fn execute(&self, operation: WireOperation) -> Result<WireReply> {
        let server = &self.servers[self.server_index(operation.key())];
        execute_on(server, operation).await
    }

    /// Groups operations by physical server, pipelines groups concurrently,
    /// and restores replies to exact caller order.
    async fn execute_batch(&self, operations: Vec<WireOperation>) -> Result<Vec<WireReply>> {
        if operations.is_empty() {
            return Ok(Vec::new());
        }
        let operation_count = operations.len();
        let mut groups: Vec<Vec<(usize, WireOperation)>> =
            (0..self.servers.len()).map(|_| Vec::new()).collect();
        for (position, operation) in operations.into_iter().enumerate() {
            let server = self.server_index(operation.key());
            groups[server].push((position, operation));
        }

        let futures = groups
            .into_iter()
            .enumerate()
            .filter(|(_, group)| !group.is_empty())
            .map(|(server, group)| execute_group(&self.servers[server], group));
        let completed = try_join_all(futures).await?;
        let mut ordered: Vec<Option<WireReply>> = (0..operation_count).map(|_| None).collect();
        for group in completed {
            for (position, reply) in group {
                ordered[position] = Some(reply);
            }
        }
        ordered
            .into_iter()
            .enumerate()
            .map(|(position, reply)| {
                reply.ok_or_else(|| {
                    CacheError::Internal(format!(
                        "memcached: batch omitted reply at position {position}"
                    ))
                })
            })
            .collect()
    }
}

/// Lifecycle wrapper that rejects work admitted after close.
struct ManagedExecutor {
    /// Present while commands may snapshot the router; removed exactly once.
    router: RwLock<Option<Arc<Router>>>,
}

impl ManagedExecutor {
    /// Takes a short-lived router snapshot without holding the lifecycle lock
    /// during DNS, dialing, or command I/O.
    async fn snapshot(&self) -> Result<Arc<Router>> {
        self.router
            .read()
            .await
            .as_ref()
            .cloned()
            .ok_or_else(|| CacheError::Internal("memcached: client is closed".to_owned()))
    }
}

#[async_trait]
impl CommandExecutor for ManagedExecutor {
    /// Executes through a router snapshot so close never holds a lock over I/O.
    async fn execute(&self, operation: WireOperation) -> Result<WireReply> {
        self.snapshot().await?.execute(operation).await
    }

    /// Executes a grouped batch through one router snapshot.
    async fn execute_batch(&self, operations: Vec<WireOperation>) -> Result<Vec<WireReply>> {
        self.snapshot().await?.execute_batch(operations).await
    }

    /// Removes the owned router; repeated removal is harmless.
    async fn close(&self) {
        self.router.write().await.take();
    }
}

/// Caller-owned asynchronous connection to a Memcached server list.
///
/// Construction resolves all distinct server addresses and probes the server
/// selected for runtime-go's probe key. A miss is successful reachability.
/// The client owns routing and pools; primitive adapters borrow them through a
/// shared executor and never close the root client themselves.
///
/// **Trade-offs**: Explicit close makes shutdown state observable, unlike Go's
/// no-op wrapper, but an admitted operation may still finish from its router
/// snapshot. The experimental dependency is deliberately not exposed through
/// Wrap/Unwrap.
///
/// **Scalability**: Keys distribute by CRC32 across configured slots. Per-server
/// batches run concurrently and each server retains its own idle pool.
///
/// **Use when**: An application owns a stable Memcached server list and will
/// construct one or more cache databases through [`MemcachedProvider`].
pub struct MemcachedClient {
    /// Shared command admission and local close state.
    executor: Arc<dyn CommandExecutor>,
}

impl MemcachedClient {
    /// Resolves, configures, and reachability-checks a Memcached client.
    ///
    /// **Cost**: DNS resolution for every distinct address, followed by one GET
    /// round trip to the probe key's selected server. Connections to other
    /// servers remain lazy.
    /// **Concurrency**: Returned handles admit concurrent commands and batches.
    /// **Side effects**: Opens and pools the probe connection.
    /// **When to use**: Once during application startup before creating the
    /// Memcached Provider.
    pub async fn connect(config: MemcachedConfig) -> Result<Self> {
        let dial = resolve_dial_timeout(config.dial_timeout)?;
        let operation = resolve_operation_timeout(config.operation_timeout)?;
        let max_idle = if config.max_idle_connections == 0 {
            GO_DEFAULT_MAX_IDLE
        } else {
            config.max_idle_connections
        };
        let (addresses, slots) = plan_servers(&config.servers)?;
        let futures = addresses
            .into_iter()
            .map(|address| connect_server(address, dial, operation, max_idle));
        let servers = try_join_all(futures).await?;
        let executor: Arc<dyn CommandExecutor> = Arc::new(ManagedExecutor {
            router: RwLock::new(Some(Arc::new(Router { servers, slots }))),
        });
        let client = Self { executor };
        client.probe().await?;
        Ok(client)
    }

    /// Closes local command admission and releases pooled transports.
    ///
    /// **Cost**: Local handle release; no Memcached command.
    /// **Concurrency**: Already-admitted router snapshots may finish; later
    /// operations fail. Repeated calls are harmless.
    /// **When to use**: During shutdown after DBs borrowing this client stop.
    pub async fn close(&self) {
        self.executor.close().await;
    }

    /// Reads the probe key once; miss and hit both prove server reachability.
    async fn probe(&self) -> Result<()> {
        match self
            .executor
            .execute(WireOperation::Get {
                key: PROBE_KEY.to_owned(),
                value: true,
                touch: None,
            })
            .await?
        {
            WireReply::Hit(_) | WireReply::Miss => Ok(()),
            reply => Err(unexpected("probe", reply)),
        }
    }

    /// Creates a Driver/Bulk adapter sharing this client's pools and lifecycle.
    pub(crate) fn primitives(&self) -> Arc<MemcachedPrimitives> {
        Arc::new(MemcachedPrimitives {
            executor: self.executor.clone(),
        })
    }

    #[cfg(test)]
    /// Constructs a client around a deterministic executor for unit tests.
    fn with_executor(executor: Arc<dyn CommandExecutor>) -> Self {
        Self { executor }
    }
}

/// Memcached implementation of the cache [`Provider`] boundary.
///
/// Named databases are isolated by a key namespace. Numeric databases are
/// emulated by embedding the requested index in every key because Memcached has
/// no native database selection. Selected databases borrow the caller-owned
/// [`MemcachedClient`] and advertise only Driver and Bulk capabilities.
///
/// **Trade-offs**: Selection is local and inexpensive, but isolation depends on
/// every application respecting the generated keyspace. Memcached cannot scan
/// or safely drop one database, enumerate server-side sets, report remaining
/// TTL, or release a distributed claim with compare-and-delete.
///
/// **Scalability**: Every selected DB shares the root client's per-server pools
/// and Go-compatible router. Numeric selection creates no new connection.
///
/// **Best for**: Constructing cache strategies over a caller-owned Memcached
/// client when direct key access and batched reads are sufficient.
pub struct MemcachedProvider {
    /// Caller-owned root client; the Provider and selected DBs never close it.
    client: Arc<MemcachedClient>,
    /// Shared strategy defaults and optional named-database allowlist.
    config: Config,
}

impl MemcachedProvider {
    /// Binds cache policy to a caller-owned Memcached client.
    ///
    /// **Cost**: Local allocation only; no DNS lookup or Memcached command.
    /// **Concurrency**: Returned databases safely share the client's router and
    /// per-server pools.
    /// **Side effects**: Does not connect, probe, or take ownership of the
    /// client's shutdown lifecycle.
    /// **When to use**: After [`MemcachedClient::connect`] and before selecting
    /// a named or numeric cache database.
    pub fn new(client: Arc<MemcachedClient>, config: Config) -> Self {
        Self { client, config }
    }

    /// Wires Driver and Bulk over the root client's shared executor.
    ///
    /// Memcached's absent capabilities remain absent so strategies report
    /// `Unsupported` at their public operation boundary. Construction performs
    /// no backend I/O and installs no release callback because no client or
    /// connection is derived for a selected database.
    fn database(&self, namespace: String, database: usize, embed_db: bool) -> DB {
        let primitives = self.client.primitives();
        let driver: Arc<dyn Driver> = primitives.clone();
        let bulk: Arc<dyn Bulk> = primitives;
        let capabilities = Capabilities::new().with_bulk(bulk);
        build_database(driver, capabilities, DatabaseSpec {
            prefix: self.config.prefix.clone(),
            namespace,
            database,
            embed_db,
            default_ttl: self.config.default_ttl,
            default_stale: self.config.default_stale,
            concurrency: self.config.concurrency,
            require_ttl: self.config.require_ttl,
            ..DatabaseSpec::default()
        })
    }
}

#[async_trait]
impl Provider for MemcachedProvider {
    /// Selects a named key namespace over the caller-owned client.
    ///
    /// Validation and the configured allowlist are checked locally before DB
    /// construction. Selection performs no Memcached command; the client was
    /// already probed during connection. The namespace itself separates keys,
    /// so database zero is not redundantly embedded.
    async fn set_database(&self, name: &str) -> Result<DB> {
        check_namespace(name)?;
        check_known(name, &self.config.databases)?;
        Ok(self.database(name.to_owned(), 0, false))
    }

    /// Selects an emulated numeric database over the caller-owned client.
    ///
    /// Memcached has no native database selector, so the requested index is
    /// embedded into every generated key. This is a local construction with no
    /// command, connection, or independently owned resource.
    async fn select_index(&self, index: usize) -> Result<DB> {
        Ok(self.database(String::new(), index, true))
    }

    /// Rejects deletion of one named database after validating its namespace.
    ///
    /// **Cost**: Local validation only. **Side effects**: None.
    /// **Safety**: Memcached has no keyspace cursor, and `flush_all` would erase
    /// unrelated prefixes and applications, so it is never used as a fallback.
    async fn drop_database(&self, name: &str) -> Result<usize> {
        check_namespace(name)?;
        Err(CacheError::Unsupported)
    }

    /// Returns the stable Memcached identity without backend I/O.
    fn backend(&self) -> &str {
        BACKEND
    }
}

/// Memcached implementation of the required Driver and optional Bulk boundary.
///
/// This adapter owns no transport and exposes no server-specific operations.
/// It deliberately does not implement Sets, Leases, Scanner, SetScanner, or
/// Fenced; the later Provider will declare only Driver and Bulk.
///
/// **Trade-offs**: Exists and Touch use metadata-only meta gets, avoiding value
/// transfer. Multi-key Delete remains sequential to preserve Go's first-error
/// ordering; Bulk reads pipeline per server.
///
/// **Scalability**: Single-key operations touch exactly one routed server.
/// Bulk groups run concurrently across servers, while a long Delete list costs
/// one sequential exchange per key.
///
/// **Use case**: Internal adapter supplied by the Memcached Provider; callers
/// normally consume strategy traits rather than this concrete type.
pub(crate) struct MemcachedPrimitives {
    /// Shared root-client transport; this adapter never closes it.
    executor: Arc<dyn CommandExecutor>,
}

impl MemcachedPrimitives {
    /// Performs one native store and maps its semantic condition result.
    async fn write(
        &self,
        key: &str,
        value: &[u8],
        ttl: Duration,
        condition: StoreCondition,
    ) -> Result<bool> {
        let expiry = to_expiry(ttl)?;
        let reply = self
            .executor
            .execute(WireOperation::Store {
                key: key.to_owned(),
                value: value.to_vec(),
                expiry,
                condition,
            })
            .await?;
        match (condition, reply) {
            (_, WireReply::Stored) => Ok(true),
            (StoreCondition::Add, WireReply::AlreadyExists) => Ok(false),
            (StoreCondition::Replace, WireReply::Miss) => Ok(false),
            (_, reply) => Err(unexpected("store", reply)),
        }
    }
}

#[async_trait]
impl Driver for MemcachedPrimitives {
    /// Returns the stable backend identity without I/O or side effects.
    fn name(&self) -> &str {
        BACKEND
    }

    /// Reads raw bytes with one meta-get round trip.
    ///
    /// **Cost**: One exchange with the CRC32-selected server. **Side effects**:
    /// Normal Memcached read/LRU accounting only. A miss or expiry is NotFound;
    /// transport and protocol failures remain errors.
    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        match self
            .executor
            .execute(WireOperation::Get {
                key: key.to_owned(),
                value: true,
                touch: None,
            })
            .await?
        {
            WireReply::Hit(Some(value)) => Ok(value),
            WireReply::Miss => Err(CacheError::NotFound),
            reply => Err(unexpected("get", reply)),
        }
    }

    /// Stores raw bytes and expiry unconditionally with one meta-set.
    ///
    /// **Cost**: One selected-server exchange. **Side effects**: Replaces any
    /// existing value and lease. Zero TTL makes the item permanent.
    async fn set(&self, key: &str, value: &[u8], ttl: Duration) -> Result<()> {
        self.write(key, value, ttl, StoreCondition::Set).await?;
        Ok(())
    }

    /// Stores only when absent with one atomic Add-mode meta-set.
    ///
    /// **Cost**: One selected-server exchange. **Side effects**: Creates value
    /// and lease only on success; an existing live item returns false without
    /// mutation.
    async fn add(&self, key: &str, value: &[u8], ttl: Duration) -> Result<bool> {
        self.write(key, value, ttl, StoreCondition::Add).await
    }

    /// Stores only when present with one atomic Replace-mode meta-set.
    ///
    /// **Cost**: One selected-server exchange. **Side effects**: Replaces value
    /// and lease only on success; an absent item returns false.
    async fn replace(&self, key: &str, value: &[u8], ttl: Duration) -> Result<bool> {
        self.write(key, value, ttl, StoreCondition::Replace).await
    }

    /// Deletes keys sequentially, treating misses as success.
    ///
    /// **Cost**: One round trip per key, stopping on the first real failure.
    /// **Side effects**: Earlier deletions remain applied if a later exchange
    /// fails. Misses are already in the desired state. Empty input performs no
    /// I/O. Use Bulk reads—not Delete—for unordered high-fanout work.
    async fn delete(&self, keys: &[&str]) -> Result<()> {
        for key in keys {
            match self
                .executor
                .execute(WireOperation::Delete {
                    key: (*key).to_owned(),
                })
                .await?
            {
                WireReply::Stored | WireReply::Miss => {}
                reply => return Err(unexpected("delete", reply)),
            }
        }
        Ok(())
    }

    /// Checks liveness with one metadata-only meta-get.
    ///
    /// **Cost**: One selected-server exchange without value transfer. **Side
    /// effects**: Normal read/LRU accounting only. Miss returns false; backend
    /// failures do not masquerade as absence.
    async fn exists(&self, key: &str) -> Result<bool> {
        match self
            .executor
            .execute(WireOperation::Get {
                key: key.to_owned(),
                value: false,
                touch: None,
            })
            .await?
        {
            WireReply::Hit(None) => Ok(true),
            WireReply::Miss => Ok(false),
            reply => Err(unexpected("exists", reply)),
        }
    }

    /// Atomically replaces expiry with one metadata-only meta-get Touch.
    ///
    /// **Cost**: One selected-server exchange without value transfer. **Side
    /// effects**: Replaces the live item's lease; zero makes it permanent.
    /// Missing/expired items return NotFound and are never recreated.
    async fn touch(&self, key: &str, ttl: Duration) -> Result<()> {
        let expiry = to_expiry(ttl)?;
        match self
            .executor
            .execute(WireOperation::Get {
                key: key.to_owned(),
                value: false,
                touch: Some(expiry),
            })
            .await?
        {
            WireReply::Hit(None) => Ok(()),
            WireReply::Miss => Err(CacheError::NotFound),
            reply => Err(unexpected("touch", reply)),
        }
    }
}

#[async_trait]
impl Bulk for MemcachedPrimitives {
    /// Fetches ordered values in one concurrent pipeline per physical server.
    ///
    /// **Cost**: Zero I/O for empty input; otherwise one pipeline exchange per
    /// involved server, with server groups concurrent. **Side effects**: Read/
    /// LRU accounting only. Misses retain their positions as None.
    async fn get_many(&self, keys: &[String]) -> Result<Vec<Option<Vec<u8>>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let operations = keys
            .iter()
            .map(|key| WireOperation::Get {
                key: key.clone(),
                value: true,
                touch: None,
            })
            .collect();
        let replies = self.executor.execute_batch(operations).await?;
        validate_reply_count("get many", keys.len(), replies.len())?;
        replies
            .into_iter()
            .map(|reply| match reply {
                WireReply::Hit(Some(value)) => Ok(Some(value)),
                WireReply::Miss => Ok(None),
                reply => Err(unexpected("get many", reply)),
            })
            .collect()
    }

    /// Checks ordered liveness in one concurrent pipeline per physical server.
    ///
    /// **Cost**: Same grouping as GetMany but without value transfer. **Side
    /// effects**: Read/LRU accounting only. Misses retain their positions as
    /// false, and any server failure fails the whole non-transactional batch.
    async fn exists_many(&self, keys: &[String]) -> Result<Vec<bool>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let operations = keys
            .iter()
            .map(|key| WireOperation::Get {
                key: key.clone(),
                value: false,
                touch: None,
            })
            .collect();
        let replies = self.executor.execute_batch(operations).await?;
        validate_reply_count("exists many", keys.len(), replies.len())?;
        replies
            .into_iter()
            .map(|reply| match reply {
                WireReply::Hit(None) => Ok(true),
                WireReply::Miss => Ok(false),
                reply => Err(unexpected("exists many", reply)),
            })
            .collect()
    }
}

/// Connects one physical server with explicit Go-compatible timeout defaults.
async fn connect_server(
    address: String,
    dial_timeout: Option<Duration>,
    operation_timeout: Option<Duration>,
    max_idle: usize,
) -> Result<ServerClient> {
    let connect = AsyncMetaClient::connect(address.as_str());
    let client = match dial_timeout {
        Some(timeout) => tokio::time::timeout(timeout, connect)
            .await
            .map_err(|_| {
                CacheError::Internal(format!(
                    "memcached: resolve server {address}: timed out after {timeout:?}"
                ))
            })?
            .map_err(|error| internal("resolve server", &address, error))?,
        None => connect
            .await
            .map_err(|error| internal("resolve server", &address, error))?,
    }
    .with_connect_timeout(dial_timeout)
    .with_io_timeout(operation_timeout)
    .with_max_idle(max_idle);
    Ok(ServerClient { address, client })
}

/// Executes one operation using the selected async client.
async fn execute_on(server: &ServerClient, operation: WireOperation) -> Result<WireReply> {
    match operation {
        WireOperation::Get { key, value, touch } => {
            let mut request = server.client.get(key);
            if !value {
                request = request.without_value();
            }
            if let Some(expiry) = touch {
                request = request.touch(expiry);
            }
            let result = request
                .send()
                .await
                .map_err(|error| internal("get", &server.address, error))?;
            decode_get(result.status, result.value)
        }
        WireOperation::Store {
            key,
            value,
            expiry,
            condition,
        } => {
            let request = server.client.set(key, value).ttl(expiry);
            let result = match condition {
                StoreCondition::Set => request.send().await,
                StoreCondition::Add => request.add().send().await,
                StoreCondition::Replace => request.replace().send().await,
            }
            .map_err(|error| internal("store", &server.address, error))?;
            decode_mutation(result)
        }
        WireOperation::Delete { key } => {
            let result = server
                .client
                .delete(key)
                .send()
                .await
                .map_err(|error| internal("delete", &server.address, error))?;
            decode_mutation(result)
        }
    }
}

/// Executes one already-routed group as one ordered meta pipeline.
async fn execute_group(
    server: &ServerClient,
    group: Vec<(usize, WireOperation)>,
) -> Result<Vec<(usize, WireReply)>> {
    let operations: Vec<Op> = group
        .iter()
        .map(|(_, operation)| encode_operation(operation))
        .collect();
    let replies = server
        .client
        .run_batch(operations)
        .await
        .map_err(|error| internal("batch", &server.address, error))?;
    if replies.len() != group.len() {
        return Err(CacheError::Internal(format!(
            "memcached: batch from {} returned {} replies for {} operations",
            server.address,
            replies.len(),
            group.len()
        )));
    }
    group
        .into_iter()
        .zip(replies)
        .map(|((position, operation), reply)| {
            decode_batch_reply(&operation, reply).map(|reply| (position, reply))
        })
        .collect()
}

/// Encodes one private operation for the dependency's heterogeneous batch API.
fn encode_operation(operation: &WireOperation) -> Op {
    match operation {
        WireOperation::Get { key, value, touch } => {
            let mut operation = Get::new(key.as_bytes().to_vec());
            if !value {
                operation = operation.without_value();
            }
            if let Some(expiry) = touch {
                operation = operation.touch(*expiry);
            }
            operation.into()
        }
        WireOperation::Store {
            key,
            value,
            expiry,
            condition,
        } => {
            let operation = Set::new(key.as_bytes().to_vec(), value.clone()).ttl(*expiry);
            match condition {
                StoreCondition::Set => operation,
                StoreCondition::Add => operation.add(),
                StoreCondition::Replace => operation.replace(),
            }
            .into()
        }
        WireOperation::Delete { key } => Delete::new(key.as_bytes().to_vec()).into(),
    }
}

/// Maps a heterogeneous batch reply using its originating operation kind.
fn decode_batch_reply(operation: &WireOperation, reply: OpResult) -> Result<WireReply> {
    match (operation, reply) {
        (WireOperation::Get { .. }, OpResult::Get(result)) => {
            decode_get(result.status, result.value)
        }
        (
            WireOperation::Store { .. } | WireOperation::Delete { .. },
            OpResult::Mutation(result),
        ) => decode_mutation(result),
        (_, reply) => Err(CacheError::Internal(format!(
            "memcached: batch returned mismatched reply {reply:?}"
        ))),
    }
}

/// Maps typed meta-get outcomes into the private stable reply.
fn decode_get(status: GetStatus, value: Option<Vec<u8>>) -> Result<WireReply> {
    match status {
        GetStatus::Hit => Ok(WireReply::Hit(value)),
        GetStatus::Miss => Ok(WireReply::Miss),
        status => Err(CacheError::Internal(format!(
            "memcached: unexpected get status {status:?}"
        ))),
    }
}

/// Maps typed mutation outcomes without treating a condition conflict as I/O.
fn decode_mutation(result: MutationResult) -> Result<WireReply> {
    match result.status {
        MutationStatus::Stored => Ok(WireReply::Stored),
        MutationStatus::NotFound => Ok(WireReply::Miss),
        MutationStatus::AlreadyExists => Ok(WireReply::AlreadyExists),
        status => Err(CacheError::Internal(format!(
            "memcached: unexpected mutation status {status:?}"
        ))),
    }
}

/// Deduplicates physical server strings while retaining weighted routing slots.
fn plan_servers(servers: &[String]) -> Result<(Vec<String>, Vec<usize>)> {
    if servers.is_empty() {
        return Err(CacheError::Internal(
            "memcached: at least one server is required".to_owned(),
        ));
    }
    let mut indexes = HashMap::new();
    let mut unique = Vec::new();
    let mut slots = Vec::with_capacity(servers.len());
    for server in servers {
        if server.trim().is_empty() {
            return Err(CacheError::Internal(
                "memcached: server address cannot be empty".to_owned(),
            ));
        }
        let index = match indexes.get(server) {
            Some(index) => *index,
            None => {
                let index = unique.len();
                unique.push(server.clone());
                indexes.insert(server.clone(), index);
                index
            }
        };
        slots.push(index);
    }
    Ok((unique, slots))
}

/// Returns the Go-compatible configured slot for one valid Memcached key.
fn route_slot(key: &[u8], slots: usize) -> usize {
    (crc32fast::hash(key) as usize) % slots
}

/// Maps shared dial intent to this driver's explicit timeout value.
fn resolve_dial_timeout(timeout: DialTimeout) -> Result<Option<Duration>> {
    resolve_timeout("dial", timeout.into())
}

/// Maps shared operation intent to this driver's explicit timeout value.
fn resolve_operation_timeout(timeout: OperationTimeout) -> Result<Option<Duration>> {
    let normalized = match timeout {
        OperationTimeout::Default => TimeoutIntent::Default,
        OperationTimeout::Disabled => TimeoutIntent::Disabled,
        OperationTimeout::After(duration) => TimeoutIntent::After(duration),
    };
    resolve_timeout("operation", normalized)
}

/// Internal common form for the two public timeout enums.
enum TimeoutIntent {
    /// Use Go's documented driver policy.
    Default,
    /// Install no client-side timeout.
    Disabled,
    /// Install one positive duration.
    After(Duration),
}

impl From<DialTimeout> for TimeoutIntent {
    fn from(value: DialTimeout) -> Self {
        match value {
            DialTimeout::Default => Self::Default,
            DialTimeout::Disabled => Self::Disabled,
            DialTimeout::After(duration) => Self::After(duration),
        }
    }
}

/// Resolves a timeout and rejects ambiguous zero durations before I/O.
fn resolve_timeout(kind: &str, timeout: TimeoutIntent) -> Result<Option<Duration>> {
    match timeout {
        TimeoutIntent::Default => Ok(Some(GO_DEFAULT_TIMEOUT)),
        TimeoutIntent::Disabled => Ok(None),
        TimeoutIntent::After(duration) if duration.is_zero() => Err(CacheError::Internal(format!(
            "memcached: {kind} timeout must be positive"
        ))),
        TimeoutIntent::After(duration) => Ok(Some(duration)),
    }
}

/// Converts a cache TTL using the current wall clock for absolute expiries.
fn to_expiry(ttl: Duration) -> Result<u32> {
    to_expiry_at(ttl, SystemTime::now())
}

/// Converts a cache TTL into Memcached's dual relative/absolute representation.
fn to_expiry_at(ttl: Duration, now: SystemTime) -> Result<u32> {
    if ttl.is_zero() {
        return Ok(0);
    }
    let seconds = ttl.as_secs().max(1);
    if seconds <= THIRTY_DAYS_SECONDS {
        return u32::try_from(seconds).map_err(|error| {
            CacheError::Internal(format!("memcached: relative expiry overflow: {error}"))
        });
    }
    let deadline = now
        .checked_add(ttl)
        .ok_or_else(|| CacheError::Internal("memcached: absolute expiry overflow".to_owned()))?;
    let unix = deadline.duration_since(UNIX_EPOCH).map_err(|error| {
        CacheError::Internal(format!(
            "memcached: absolute expiry predates Unix epoch: {error}"
        ))
    })?;
    u32::try_from(unix.as_secs()).map_err(|error| {
        CacheError::Internal(format!(
            "memcached: absolute expiry exceeds protocol range: {error}"
        ))
    })
}

/// Adds safe operation/server context to a dependency error.
fn internal(operation: &str, server: &str, error: impl Display) -> CacheError {
    CacheError::Internal(format!(
        "memcached: {operation} on {server} failed: {error}"
    ))
}

/// Reports a semantically impossible reply without silently degrading it.
fn unexpected(operation: &str, reply: WireReply) -> CacheError {
    CacheError::Internal(format!("memcached: unexpected {operation} reply {reply:?}"))
}

/// Rejects a malformed executor batch before ordered results escape to core.
fn validate_reply_count(operation: &str, expected: usize, actual: usize) -> Result<()> {
    if expected == actual {
        Ok(())
    } else {
        Err(CacheError::Internal(format!(
            "memcached: {operation} returned {actual} replies for {expected} keys"
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

    use tokio::sync::Mutex;

    use super::*;

    /// Deterministic semantic executor for primitive and lifecycle tests.
    struct ScriptedExecutor {
        operations: Mutex<Vec<WireOperation>>,
        replies: Mutex<VecDeque<Result<WireReply>>>,
        batches: Mutex<VecDeque<Result<Vec<WireReply>>>>,
        closed: AtomicBool,
    }

    impl ScriptedExecutor {
        fn new(replies: Vec<Result<WireReply>>) -> Self {
            Self {
                operations: Mutex::new(Vec::new()),
                replies: Mutex::new(replies.into()),
                batches: Mutex::new(VecDeque::new()),
                closed: AtomicBool::new(false),
            }
        }

        fn with_batches(batches: Vec<Result<Vec<WireReply>>>) -> Self {
            Self {
                operations: Mutex::new(Vec::new()),
                replies: Mutex::new(VecDeque::new()),
                batches: Mutex::new(batches.into()),
                closed: AtomicBool::new(false),
            }
        }
    }

    #[async_trait]
    impl CommandExecutor for ScriptedExecutor {
        async fn execute(&self, operation: WireOperation) -> Result<WireReply> {
            if self.closed.load(Ordering::SeqCst) {
                return Err(CacheError::Internal("scripted client is closed".to_owned()));
            }
            self.operations.lock().await.push(operation);
            self.replies
                .lock()
                .await
                .pop_front()
                .unwrap_or_else(|| Err(CacheError::Internal("missing scripted reply".to_owned())))
        }

        async fn execute_batch(&self, operations: Vec<WireOperation>) -> Result<Vec<WireReply>> {
            if self.closed.load(Ordering::SeqCst) {
                return Err(CacheError::Internal("scripted client is closed".to_owned()));
            }
            self.operations.lock().await.extend(operations);
            self.batches.lock().await.pop_front().unwrap_or_else(|| {
                Err(CacheError::Internal(
                    "missing scripted batch reply".to_owned(),
                ))
            })
        }

        async fn close(&self) {
            self.closed.store(true, Ordering::SeqCst);
        }
    }

    fn primitives(executor: Arc<ScriptedExecutor>) -> MemcachedPrimitives {
        MemcachedPrimitives { executor }
    }

    #[test]
    fn test_memcached_config_default_requires_explicit_servers() {
        let config = MemcachedConfig::default();
        assert!(config.servers.is_empty());
        assert_eq!(config.dial_timeout, DialTimeout::Default);
        assert_eq!(config.operation_timeout, OperationTimeout::Default);
        assert_eq!(config.max_idle_connections, 0);
    }

    #[test]
    fn test_memcached_timeout_resolution_preserves_intent_and_go_default() {
        assert_eq!(
            resolve_dial_timeout(DialTimeout::Default).unwrap(),
            Some(Duration::from_millis(500))
        );
        assert_eq!(
            resolve_operation_timeout(OperationTimeout::Disabled).unwrap(),
            None
        );
        assert_eq!(
            resolve_operation_timeout(OperationTimeout::After(Duration::from_secs(2))).unwrap(),
            Some(Duration::from_secs(2))
        );
        assert!(resolve_dial_timeout(DialTimeout::After(Duration::ZERO)).is_err());
        assert!(resolve_operation_timeout(OperationTimeout::After(Duration::ZERO)).is_err());
    }

    #[test]
    fn test_memcached_server_plan_preserves_weighted_slots_and_deduplicates_pools() {
        let servers = vec!["a:1".to_owned(), "b:2".to_owned(), "a:1".to_owned()];
        let (unique, slots) = plan_servers(&servers).unwrap();
        assert_eq!(unique, vec!["a:1", "b:2"]);
        assert_eq!(slots, vec![0, 1, 0]);
        assert!(plan_servers(&[]).is_err());
        assert!(plan_servers(&[String::new()]).is_err());
    }

    #[test]
    fn test_memcached_route_slot_matches_go_crc32_modulo() {
        assert_eq!(crc32fast::hash(b"foo"), 0x8c73_6521);
        assert_eq!(route_slot(b"foo", 3), 2);
    }

    #[test]
    fn test_memcached_expiry_handles_permanent_relative_absolute_and_overflow() {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        assert_eq!(to_expiry_at(Duration::ZERO, now).unwrap(), 0);
        assert_eq!(to_expiry_at(Duration::from_nanos(1), now).unwrap(), 1);
        assert_eq!(to_expiry_at(Duration::from_millis(1_999), now).unwrap(), 1);
        assert_eq!(
            to_expiry_at(Duration::from_secs(THIRTY_DAYS_SECONDS), now).unwrap(),
            THIRTY_DAYS_SECONDS as u32
        );
        assert_eq!(
            to_expiry_at(Duration::from_secs(THIRTY_DAYS_SECONDS + 1), now).unwrap(),
            1_702_592_001
        );
        let too_late = UNIX_EPOCH + Duration::from_secs(u32::MAX as u64);
        assert!(to_expiry_at(Duration::from_secs(THIRTY_DAYS_SECONDS + 1), too_late).is_err());
    }

    #[tokio::test]
    async fn test_memcached_driver_get_maps_hit_and_miss() {
        let executor = Arc::new(ScriptedExecutor::new(vec![
            Ok(WireReply::Hit(Some(b"value".to_vec()))),
            Ok(WireReply::Miss),
        ]));
        let driver = primitives(executor.clone());
        assert_eq!(driver.get("hit").await.unwrap(), b"value");
        assert!(matches!(
            driver.get("miss").await,
            Err(CacheError::NotFound)
        ));
        assert_eq!(executor.operations.lock().await.len(), 2);
    }

    #[tokio::test]
    async fn test_memcached_driver_writes_map_conditions_and_expiry() {
        let executor = Arc::new(ScriptedExecutor::new(vec![
            Ok(WireReply::Stored),
            Ok(WireReply::AlreadyExists),
            Ok(WireReply::Miss),
        ]));
        let driver = primitives(executor.clone());
        driver
            .set("set", b"value", Duration::from_secs(5))
            .await
            .unwrap();
        assert!(!driver.add("add", b"value", Duration::ZERO).await.unwrap());
        assert!(
            !driver
                .replace("replace", b"value", Duration::ZERO)
                .await
                .unwrap()
        );
        let operations = executor.operations.lock().await;
        assert!(matches!(&operations[0], WireOperation::Store {
            expiry: 5,
            condition: StoreCondition::Set,
            ..
        }));
    }

    #[tokio::test]
    async fn test_memcached_driver_delete_is_sequential_and_miss_is_success() {
        let executor = Arc::new(ScriptedExecutor::new(vec![
            Ok(WireReply::Stored),
            Ok(WireReply::Miss),
        ]));
        let driver = primitives(executor.clone());
        driver.delete(&["a", "b"]).await.unwrap();
        driver.delete(&[]).await.unwrap();
        assert_eq!(executor.operations.lock().await.len(), 2);
    }

    #[tokio::test]
    async fn test_memcached_driver_exists_and_touch_use_metadata_only_get() {
        let executor = Arc::new(ScriptedExecutor::new(vec![
            Ok(WireReply::Hit(None)),
            Ok(WireReply::Hit(None)),
            Ok(WireReply::Miss),
        ]));
        let driver = primitives(executor.clone());
        assert!(driver.exists("key").await.unwrap());
        driver.touch("key", Duration::ZERO).await.unwrap();
        assert!(matches!(
            driver.touch("missing", Duration::from_secs(2)).await,
            Err(CacheError::NotFound)
        ));
        let operations = executor.operations.lock().await;
        assert!(matches!(&operations[0], WireOperation::Get {
            value: false,
            touch: None,
            ..
        }));
        assert!(matches!(&operations[1], WireOperation::Get {
            value: false,
            touch: Some(0),
            ..
        }));
    }

    #[tokio::test]
    async fn test_memcached_bulk_preserves_order_duplicates_and_misses() {
        let executor = Arc::new(ScriptedExecutor::with_batches(vec![
            Ok(vec![
                WireReply::Hit(Some(b"a".to_vec())),
                WireReply::Miss,
                WireReply::Hit(Some(b"a".to_vec())),
            ]),
            Ok(vec![WireReply::Hit(None), WireReply::Miss]),
        ]));
        let driver = primitives(executor);
        assert_eq!(
            driver
                .get_many(&["a".to_owned(), "b".to_owned(), "a".to_owned()])
                .await
                .unwrap(),
            vec![Some(b"a".to_vec()), None, Some(b"a".to_vec())]
        );
        assert_eq!(
            driver
                .exists_many(&["a".to_owned(), "b".to_owned()])
                .await
                .unwrap(),
            vec![true, false]
        );
    }

    #[tokio::test]
    async fn test_memcached_client_close_is_idempotent_and_rejects_later_work() {
        let executor = Arc::new(ScriptedExecutor::new(Vec::new()));
        let client = MemcachedClient::with_executor(executor.clone());
        let primitives = client.primitives();
        client.close().await;
        client.close().await;
        assert!(primitives.get("key").await.is_err());
        assert!(executor.closed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_memcached_managed_executor_close_removes_router_once() {
        let executor = ManagedExecutor {
            router: RwLock::new(Some(Arc::new(Router {
                servers: Vec::new(),
                slots: Vec::new(),
            }))),
        };
        assert!(executor.snapshot().await.is_ok());
        executor.close().await;
        executor.close().await;
        assert!(executor.snapshot().await.is_err());
    }

    #[tokio::test]
    async fn test_memcached_client_connect_rejects_invalid_config_before_io() {
        assert!(
            MemcachedClient::connect(MemcachedConfig::default())
                .await
                .is_err()
        );
        let zero_dial = MemcachedConfig {
            servers: vec!["127.0.0.1:11211".to_owned()],
            dial_timeout: DialTimeout::After(Duration::ZERO),
            ..MemcachedConfig::default()
        };
        assert!(MemcachedClient::connect(zero_dial).await.is_err());
        let zero_operation = MemcachedConfig {
            servers: vec!["127.0.0.1:11211".to_owned()],
            operation_timeout: OperationTimeout::After(Duration::ZERO),
            ..MemcachedConfig::default()
        };
        assert!(MemcachedClient::connect(zero_operation).await.is_err());
    }

    #[tokio::test]
    async fn test_memcached_bulk_empty_input_performs_no_executor_call() {
        let executor = Arc::new(ScriptedExecutor::with_batches(Vec::new()));
        let driver = primitives(executor.clone());
        assert!(driver.get_many(&[]).await.unwrap().is_empty());
        assert!(driver.exists_many(&[]).await.unwrap().is_empty());
        assert!(executor.operations.lock().await.is_empty());
    }

    #[tokio::test]
    async fn test_memcached_bulk_rejects_wrong_reply_count() {
        let executor = Arc::new(ScriptedExecutor::with_batches(vec![Ok(vec![
            WireReply::Miss,
        ])]));
        let driver = primitives(executor);
        assert!(
            driver
                .get_many(&["a".to_owned(), "b".to_owned()])
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_memcached_provider_backend_and_construction_perform_no_io() {
        let executor = Arc::new(ScriptedExecutor::new(Vec::new()));
        let client = Arc::new(MemcachedClient::with_executor(executor.clone()));
        let provider = MemcachedProvider::new(client, Config::default());

        assert_eq!(provider.backend(), "memcached");
        assert!(executor.operations.lock().await.is_empty());
    }

    #[tokio::test]
    async fn test_memcached_provider_named_database_validates_and_qualifies_keys() {
        let executor = Arc::new(ScriptedExecutor::new(vec![Ok(WireReply::Stored)]));
        let client = Arc::new(MemcachedClient::with_executor(executor.clone()));
        let provider = MemcachedProvider::new(client, Config {
            prefix: "app".to_owned(),
            databases: vec!["orders".to_owned()],
            ..Config::default()
        });

        assert!(provider.set_database("").await.is_err());
        assert!(provider.set_database("unknown").await.is_err());
        let db = provider.set_database("orders").await.unwrap();
        assert_eq!(db.name, "orders");
        assert_eq!(db.index, 0);
        db.volatile
            .set("session", b"value", &crate::core::Options::default())
            .await
            .unwrap();

        assert_eq!(executor.operations.lock().await.as_slice(), &[
            WireOperation::Store {
                key: "app:orders:cache:vol:session".to_owned(),
                value: b"value".to_vec(),
                expiry: 0,
                condition: StoreCondition::Set,
            }
        ]);
    }

    #[tokio::test]
    async fn test_memcached_provider_numeric_database_embeds_index() {
        let executor = Arc::new(ScriptedExecutor::new(vec![Ok(WireReply::Stored)]));
        let client = Arc::new(MemcachedClient::with_executor(executor.clone()));
        let provider = MemcachedProvider::new(client, Config {
            prefix: "app".to_owned(),
            ..Config::default()
        });

        let db = provider.select_index(3).await.unwrap();
        assert_eq!(db.name, "");
        assert_eq!(db.index, 3);
        db.volatile
            .set("session", b"value", &crate::core::Options::default())
            .await
            .unwrap();

        assert_eq!(
            executor.operations.lock().await[0].key(),
            "app:db3:cache:vol:session"
        );
    }

    #[tokio::test]
    async fn test_memcached_provider_exposes_only_bulk_optional_capability() {
        let executor = Arc::new(ScriptedExecutor::new(Vec::new()));
        let client = Arc::new(MemcachedClient::with_executor(executor));
        let provider = MemcachedProvider::new(client, Config::default());
        let db = provider.select_index(0).await.unwrap();

        assert!(matches!(
            db.document.keys().await,
            Err(CacheError::Unsupported)
        ));
        assert!(matches!(
            db.indexed.ids_by_index("tenant", "acme").await,
            Err(CacheError::Unsupported)
        ));
        assert!(matches!(
            db.document.ttl("document").await,
            Err(CacheError::Unsupported)
        ));
        assert!(matches!(
            db.volatile.scan("*").await,
            Err(CacheError::Unsupported)
        ));
    }

    #[tokio::test]
    async fn test_memcached_provider_drop_database_validates_then_reports_unsupported() {
        let executor = Arc::new(ScriptedExecutor::new(Vec::new()));
        let client = Arc::new(MemcachedClient::with_executor(executor));
        let provider = MemcachedProvider::new(client, Config {
            databases: vec!["known".to_owned()],
            ..Config::default()
        });

        assert!(matches!(
            provider.drop_database("").await,
            Err(CacheError::Internal(_))
        ));
        assert!(matches!(
            provider.drop_database("stale-name").await,
            Err(CacheError::Unsupported)
        ));
    }

    #[tokio::test]
    async fn test_memcached_provider_database_close_keeps_root_client_open() {
        let executor = Arc::new(ScriptedExecutor::new(vec![Ok(WireReply::Stored)]));
        let client = Arc::new(MemcachedClient::with_executor(executor));
        let provider = MemcachedProvider::new(client.clone(), Config::default());
        let db = provider.select_index(0).await.unwrap();

        db.close().await.unwrap();
        db.volatile
            .set("after-db-close", b"value", &crate::core::Options::default())
            .await
            .unwrap();
        client.close().await;
        assert!(matches!(
            db.volatile
                .get("after-client-close", &mut Vec::new())
                .await,
            Err(CacheError::Internal(message)) if message.contains("closed")
        ));
    }
}
