//! Standalone Redis client, Provider, and low-level cache primitives.
//!
//! The module owns transport concerns only: connection setup, reconnecting
//! command execution, native Redis database selection, and direct command
//! mappings. Core strategies continue to own key layout, TTL policy, value
//! framing, indexing decisions, and load coordination.
//!
//! One [`RedisClient`](crate::drivers::redis::RedisClient) uses a multiplexed
//! connection manager. Named databases share that manager and separate keys with
//! [`Keyspace`](crate::core::Keyspace); selecting a different native Redis
//! index derives another client whose lifetime belongs to the returned
//! [`DB`](crate::core::DB).

use std::{collections::HashSet, fmt::Display, sync::Arc, time::Duration};

use async_trait::async_trait;
use redis::{
    Cmd, ConnectionInfo, ErrorKind, FromRedisValue, IntoConnectionInfo, Pipeline,
    RedisConnectionInfo, RedisError, RedisResult, Value,
    aio::{ConnectionManager, ConnectionManagerConfig},
};
use tokio::sync::RwLock;

use crate::{
    CacheError, Config, DialTimeout, Result,
    core::{
        Bulk, Capabilities, DB, DatabaseSpec, Driver, Fenced, Keyspace, Leases, Provider, Release,
        Scanner, SetScanVisitor, SetScanner, Sets, build_database, check_known, check_namespace,
        drop_database,
    },
};

/// Address used when [`RedisConfig::address`] is empty.
pub const DEFAULT_REDIS_ADDRESS: &str = "localhost:6379";

/// Cursor count hint shared by Redis keyspace scans.
const SCAN_BATCH: usize = 256;
/// Stable Redis identity and preset defaults supplied to the shared RESP path.
const REDIS_PROFILE: RespProfile = RespProfile::new("redis", DEFAULT_REDIS_ADDRESS);

/// Immutable identity and defaults for one Redis-compatible server preset.
///
/// The profile is crate-private so future built-in presets can reuse the RESP
/// implementation without letting callers relabel an arbitrary Redis client.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RespProfile {
    /// Stable backend name used by DB metadata and diagnostic errors.
    backend: &'static str,
    /// Address selected when the preset-specific public config leaves it empty.
    default_address: &'static str,
}

impl RespProfile {
    /// Creates one built-in RESP preset without performing backend I/O.
    pub(crate) const fn new(backend: &'static str, default_address: &'static str) -> Self {
        Self {
            backend,
            default_address,
        }
    }
}

/// Connection settings for one standalone Redis client.
///
/// The client uses one multiplexed, reconnecting connection manager rather
/// than a separate pool. Cloned command handles can run concurrently without a
/// global operation lock. Cluster/ring adoption is intentionally outside this
/// first backend boundary.
///
/// **Use case**: Configure an application-owned connection to one standalone
/// Redis server. Explicit addresses are recommended in deployed environments;
/// an empty address selects [`DEFAULT_REDIS_ADDRESS`] for local convenience.
///
/// **Scalability**: Commands multiplex over one reconnecting connection. There
/// is no configurable connection pool in this initial implementation.
///
/// **Security**: The type deliberately does not implement `Debug`, preventing
/// accidental password disclosure through routine configuration logging.
pub struct RedisConfig {
    /// Redis address in `host:port` or `[ipv6]:port` form.
    pub address: String,
    /// Optional Redis ACL username; empty omits it.
    pub username: String,
    /// Optional Redis password; empty omits it.
    pub password: String,
    /// Numeric Redis database selected when each connection is established.
    pub database: usize,
    /// Policy for bounding initial and reconnect connection attempts.
    pub dial_timeout: DialTimeout,
}

impl Clone for RedisConfig {
    fn clone(&self) -> Self {
        Self {
            address: self.address.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            database: self.database,
            dial_timeout: self.dial_timeout,
        }
    }
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            address: String::new(),
            username: String::new(),
            password: String::new(),
            database: 0,
            dial_timeout: DialTimeout::Default,
        }
    }
}

/// Internal seam for executing already-constructed Redis commands and pipelines.
///
/// Production uses [`ManagedExecutor`]. Tests substitute a scripted executor
/// so exact RESP commands and failures can be checked without a Redis service.
/// The seam contains no cache semantics and must not manufacture misses.
#[async_trait]
pub(crate) trait CommandExecutor: Send + Sync {
    /// Executes one command and returns its untyped Redis protocol value.
    ///
    /// **Cost**: Exactly one Redis round trip.
    async fn execute(&self, command: Cmd) -> RedisResult<Value>;

    /// Executes one non-transactional command pipeline in one round trip.
    ///
    /// **Side effects**: Runs every queued Redis command in order without
    /// transaction isolation.
    async fn execute_pipeline(&self, pipeline: Pipeline) -> RedisResult<Vec<Value>>;

    /// Stops future command admission and releases the owned transport handle.
    ///
    /// In-flight command snapshots may still complete. Closing is local and
    /// idempotent; it does not send a Redis command.
    async fn close(&self);
}

/// Lifecycle wrapper around redis-rs's reconnecting connection manager.
///
/// The optional manager distinguishes an open client from a closed one. The
/// asynchronous lock protects only that state transition; it must never remain
/// held during network I/O or all supposedly multiplexed commands would become
/// serialized.
struct ManagedExecutor {
    /// Present while the client admits commands; removed exactly once on close.
    manager: RwLock<Option<ConnectionManager>>,
}

#[async_trait]
impl CommandExecutor for ManagedExecutor {
    /// Executes one command through a short-lived manager snapshot.
    ///
    /// The lifecycle read lock is released before network I/O so multiplexed
    /// commands remain concurrent. A closed executor rejects new admission;
    /// an already-cloned manager may finish while close proceeds.
    async fn execute(&self, command: Cmd) -> RedisResult<Value> {
        // Clone while holding the read lock, then release the guard before I/O.
        // ConnectionManager clones address the same multiplexed connection, so
        // an in-flight snapshot remains valid if close races after this point.
        let mut manager = match self.manager.read().await.as_ref() {
            Some(manager) => manager.clone(),
            None => {
                return Err(RedisError::from((
                    ErrorKind::Client,
                    "RESP client is closed",
                )));
            }
        };
        command.query_async(&mut manager).await
    }

    /// Executes one non-transactional pipeline through a manager snapshot.
    ///
    /// Locking and close-race behavior match [`CommandExecutor::execute`]; Redis
    /// runs the queued commands in order without transaction isolation.
    async fn execute_pipeline(&self, pipeline: Pipeline) -> RedisResult<Vec<Value>> {
        let mut manager = match self.manager.read().await.as_ref() {
            Some(manager) => manager.clone(),
            None => {
                return Err(RedisError::from((
                    ErrorKind::Client,
                    "RESP client is closed",
                )));
            }
        };
        pipeline.query_async(&mut manager).await
    }

    /// Stops future admission by dropping the owned manager exactly once.
    ///
    /// This performs no Redis command and does not cancel operations that
    /// already cloned the manager before the state transition.
    async fn close(&self) {
        // `take` makes repeated close calls harmless and rejects later execute
        // calls without affecting snapshots already running.
        self.manager.write().await.take();
    }
}

/// Caller-owned connection to one standalone Redis database.
///
/// Construction connects and verifies Redis with PING, so authentication and
/// reachability failures surface during startup rather than the first cache
/// request. Cache DBs built from this client share its reconnecting manager but
/// do not close it; the caller remains responsible for [`RedisClient::close`].
///
/// **Trade-offs**: Multiplexing is lightweight for ordinary cache commands but
/// is not intended for blocking Redis operations. Selecting another native
/// index requires another client because Redis binds an index per connection.
///
/// **Scalability**: Clones of its internal executor can issue concurrent
/// commands without a global I/O lock. Reconnection is handled by redis-rs.
///
/// **Best for**: A long-lived application client shared by one or more cache
/// Providers and DBs on a standalone Redis server.
pub struct RedisClient {
    /// Normalized settings retained so another native index can be derived.
    config: RedisConfig,
    /// Backend identity/defaults retained across derived database clients.
    profile: RespProfile,
    /// Shared transport and close state used by every primitive adapter.
    executor: Arc<dyn CommandExecutor>,
}

impl RedisClient {
    /// Connects to Redis and verifies the selected database with PING.
    ///
    /// **Cost**: Connection/authentication plus one PING round trip.
    /// **Concurrency**: The returned handle supports concurrent commands.
    /// **Side effects**: Opens a reconnecting network connection.
    /// **When to use**: Once during application startup, before constructing a
    /// [`RedisProvider`]. Connection, authentication, PING, and invalid address
    /// failures are returned without exposing credentials.
    pub async fn connect(config: RedisConfig) -> Result<Self> {
        Self::connect_with_profile(config, REDIS_PROFILE).await
    }

    /// Connects one built-in Redis-compatible preset through the shared RESP
    /// path while retaining its identity for errors and derived clients.
    pub(crate) async fn connect_with_profile(
        mut config: RedisConfig,
        profile: RespProfile,
    ) -> Result<Self> {
        let address = if config.address.is_empty() {
            profile.default_address.to_owned()
        } else {
            config.address.clone()
        };
        let (host, port) = parse_address(profile.backend, &address)?;
        let database = i64::try_from(config.database).map_err(|_| {
            CacheError::Internal(format!(
                "{}: database index {} exceeds RESP range",
                profile.backend, config.database
            ))
        })?;
        let mut redis_settings = RedisConnectionInfo::default().set_db(database);
        if !config.username.is_empty() {
            redis_settings = redis_settings.set_username(&config.username);
        }
        if !config.password.is_empty() {
            redis_settings = redis_settings.set_password(&config.password);
        }
        let connection_info: ConnectionInfo = (host, port)
            .into_connection_info()
            .map_err(|error| internal(profile.backend, "configure connection", error))?
            .set_redis_settings(redis_settings);
        let redis_client = redis::Client::open(connection_info)
            .map_err(|error| internal(profile.backend, "configure connection", error))?;
        let manager_config = connection_manager_config(profile.backend, config.dial_timeout)?;
        let manager = ConnectionManager::new_with_config(redis_client, manager_config)
            .await
            .map_err(|error| internal(profile.backend, "connect", error))?;
        config.address = address;
        let client = Self {
            config,
            profile,
            executor: Arc::new(ManagedExecutor {
                manager: RwLock::new(Some(manager)),
            }),
        };
        client.ping().await?;
        Ok(client)
    }

    /// Closes this caller-owned connection handle.
    ///
    /// **Cost**: Local handle release; no Redis round trip.
    /// **Concurrency**: In-flight snapshots may finish; later commands fail.
    /// **Side effects**: Drops the transport after its last snapshot is gone.
    /// **When to use**: During application shutdown, after every DB borrowing
    /// the root client has been closed or stopped.
    pub async fn close(&self) {
        self.executor.close().await;
    }

    /// Verifies that the selected Redis database is currently reachable.
    ///
    /// **Cost**: One PING round trip. **Side effects**: None on stored data.
    /// Provider selection uses this so failures surface before returning a DB.
    async fn ping(&self) -> Result<()> {
        let value = self
            .executor
            .execute(redis::cmd("PING"))
            .await
            .map_err(|error| internal(self.profile.backend, "ping", error))?;
        let _: String = decode(self.profile.backend, "ping reply", value)?;
        Ok(())
    }

    /// Creates a capability adapter sharing this client's transport.
    ///
    /// The adapter does not own or close the client. Building it is local and
    /// only increments the executor's reference count.
    pub(crate) fn primitives(&self) -> Arc<RespPrimitives> {
        Arc::new(RespPrimitives {
            profile: self.profile,
            executor: self.executor.clone(),
        })
    }

    /// Copies normalized settings for metadata or derived-index selection.
    ///
    /// The copy may contain credentials and therefore remains crate-private.
    /// It performs no I/O and does not expose the settings through `Debug`.
    pub(crate) fn config(&self) -> RedisConfig {
        self.config.clone()
    }

    /// Returns the immutable preset identity retained by this client.
    pub(crate) fn profile(&self) -> RespProfile {
        self.profile
    }

    #[cfg(test)]
    /// Constructs a client around an in-process executor for unit tests.
    fn with_executor(config: RedisConfig, executor: Arc<dyn CommandExecutor>) -> Self {
        Self::with_profile_executor(config, REDIS_PROFILE, executor)
    }

    #[cfg(test)]
    /// Constructs a profiled client around an in-process test executor.
    pub(crate) fn with_profile_executor(
        config: RedisConfig,
        profile: RespProfile,
        executor: Arc<dyn CommandExecutor>,
    ) -> Self {
        Self {
            config,
            profile,
            executor,
        }
    }
}

/// Internal factory for a verified client on a derived RESP database index.
///
/// Provider tests replace this boundary to observe copied configuration and
/// lifecycle behavior without opening network connections.
#[async_trait]
trait RespConnector: Send + Sync {
    /// Connects and PING-verifies a client for the supplied configuration.
    ///
    /// **Cost**: Connection setup plus the PING performed by
    /// [`RedisClient::connect`]. A returned client is ready for DB construction.
    async fn connect(&self, config: RedisConfig, profile: RespProfile) -> Result<Arc<RedisClient>>;
}

/// Production connector delegating derived selection to [`RedisClient`].
struct DefaultRespConnector;

#[async_trait]
impl RespConnector for DefaultRespConnector {
    async fn connect(&self, config: RedisConfig, profile: RespProfile) -> Result<Arc<RedisClient>> {
        Ok(Arc::new(
            RedisClient::connect_with_profile(config, profile).await?,
        ))
    }
}

/// Redis implementation of the cache [`Provider`] boundary.
///
/// Named databases reuse the caller-owned root client and separate keys by
/// namespace. Selecting another native Redis index derives a client owned by
/// the returned [`DB`], whose explicit close releases only that derived handle.
/// All databases receive Redis Driver, Sets, and Scanner capabilities.
///
/// **Trade-offs**: Native indexes avoid repeating the index in every key, but
/// selecting a different index establishes another connection. Named databases
/// share one Redis index and require a cursor scan for administrative deletion.
///
/// **Scalability**: All DBs borrowing the root client share its multiplexed
/// connection. Each simultaneously used non-root native index adds one derived
/// connection manager until its DB is explicitly closed.
///
/// **Best for**: Constructing the four cache strategies after the caller has
/// explicitly selected either a key-namespaced or native Redis database.
pub struct RedisProvider {
    /// Caller-owned root client; the Provider and borrowed DBs never close it.
    client: Arc<RedisClient>,
    /// Strategy defaults and optional named-database allowlist.
    config: Config,
    /// Factory used only when selecting an index different from the root.
    connector: Arc<dyn RespConnector>,
}

impl RedisProvider {
    /// Binds cache policy to a caller-owned Redis client.
    ///
    /// **Cost**: Local allocation only; selection performs reachability checks.
    /// **Concurrency**: Returned providers and DB strategies share the client's
    /// multiplexed manager safely.
    /// **Side effects**: Does not connect, ping, or take ownership of the client.
    /// **When to use**: After constructing the caller-owned root client and
    /// before selecting a named or numeric cache database.
    pub fn new(client: Arc<RedisClient>, config: Config) -> Self {
        Self {
            client,
            config,
            connector: Arc::new(DefaultRespConnector),
        }
    }

    /// Wires one selected client into every core strategy and capability.
    ///
    /// Driver, Sets, Leases, Scanner, SetScanner, Bulk, and Fenced are
    /// trait-object views of one primitive, keeping all commands on the selected
    /// client. `release` is present only when this DB owns a derived client.
    /// Construction is local and performs no Redis round trip.
    fn database(
        &self,
        client: Arc<RedisClient>,
        namespace: String,
        database: usize,
        release: Option<Release>,
    ) -> DB {
        let primitives = client.primitives();
        let driver: Arc<dyn Driver> = primitives.clone();
        let sets: Arc<dyn Sets> = primitives.clone();
        let leases: Arc<dyn Leases> = primitives.clone();
        let bulk: Arc<dyn Bulk> = primitives.clone();
        let set_scanner: Arc<dyn SetScanner> = primitives.clone();
        let fenced: Arc<dyn Fenced> = primitives.clone();
        let scanner: Arc<dyn Scanner> = primitives;
        let capabilities = Capabilities::new()
            .with_sets(sets)
            .with_leases(leases)
            .with_bulk(bulk)
            .with_set_scanner(set_scanner)
            .with_fenced(fenced)
            .with_scanner(scanner);
        build_database(driver, capabilities, DatabaseSpec {
            prefix: self.config.prefix.clone(),
            namespace,
            database,
            embed_db: false,
            default_ttl: self.config.default_ttl,
            default_stale: self.config.default_stale,
            concurrency: self.config.concurrency,
            require_ttl: self.config.require_ttl,
            release,
            ..DatabaseSpec::default()
        })
    }

    #[cfg(test)]
    /// Installs a deterministic derived-client factory for Provider tests.
    fn with_connector(
        client: Arc<RedisClient>,
        config: Config,
        connector: Arc<dyn RespConnector>,
    ) -> Self {
        Self {
            client,
            config,
            connector,
        }
    }
}

#[async_trait]
impl Provider for RedisProvider {
    /// Selects a named key namespace on the root client's native Redis index.
    ///
    /// Validation and allowlist checks happen before I/O. A successful call
    /// performs one PING, then constructs strategies locally. The returned DB
    /// borrows the root transport, so DB close drains background work but does
    /// not close the caller-owned client.
    async fn set_database(&self, name: &str) -> Result<DB> {
        check_namespace(name)?;
        check_known(name, &self.config.databases)?;
        self.client.ping().await?;
        let database = self.client.config().database;
        Ok(self.database(self.client.clone(), name.to_owned(), database, None))
    }

    /// Selects a native Redis database index.
    ///
    /// The current index costs one PING and reuses the root client. Another
    /// index copies the normalized connection settings, connects and PINGs a
    /// derived client, and installs an async release owned by the returned DB.
    /// A failed derivation returns no DB and leaves the root client untouched.
    async fn select_index(&self, index: usize) -> Result<DB> {
        let current = self.client.config();
        if current.database == index {
            self.client.ping().await?;
            return Ok(self.database(self.client.clone(), String::new(), index, None));
        }

        let mut derived_config = current;
        derived_config.database = index;
        let derived = self
            .connector
            .connect(derived_config, self.client.profile())
            .await?;
        let released = derived.clone();
        // Strategies keep their executor alive, so DB close must explicitly
        // close the derived handle after core drains background refresh work.
        let release: Release = Box::new(move || {
            Box::pin(async move {
                released.close().await;
                Ok(())
            })
        });
        Ok(self.database(derived, String::new(), index, Some(release)))
    }

    /// Deletes keys under one named namespace on the root client's index.
    ///
    /// **Cost**: One complete cursor walk plus bounded DEL batches.
    /// **Side effects**: Permanently deletes matching cache keys; the operation
    /// is non-atomic and can report a partial count on failure.
    /// **Safety**: Core escapes the literal head and validates every scanned
    /// key. Like Go, administrative deletion does not enforce the allowlist.
    async fn drop_database(&self, name: &str) -> Result<usize> {
        check_namespace(name)?;
        let current = self.client.config();
        let keyspace = Keyspace::new(&self.config.prefix, name, current.database, false);
        let primitives = self.client.primitives();
        drop_database(primitives.as_ref(), primitives.as_ref(), keyspace.head()).await
    }

    /// Returns the stable Redis identity without backend I/O.
    fn backend(&self) -> &str {
        self.client.profile.backend
    }
}

/// Backend-aware implementation of Redis-compatible RESP cache primitives.
///
/// This adapter deliberately contains no Keyspace or strategy policy. Ordinary
/// non-empty operations map to one RESP command; Bulk maps an ordered command
/// pipeline to one round trip. The retained profile supplies metadata and error
/// identity without changing command semantics.
/// One instance can be viewed as Driver, Sets, Leases, Scanner, SetScanner,
/// Bulk, and Fenced while sharing the same executor.
///
/// **Trade-offs**: Direct commands preserve atomic Redis primitives, but the
/// Scanner must collect a full matching key list to satisfy the current Go-
/// aligned contract. Higher-level batching and validation remain in core.
///
/// **Scalability**: Ordinary commands multiplex concurrently. SCAN uses a 256
/// count hint and may require many small round trips instead of blocking Redis
/// with `KEYS`.
///
/// **Use case**: Backend adapter supplied by built-in RESP Providers; callers
/// use the strategy traits exposed by [`DB`] rather than constructing it
/// directly.
pub(crate) struct RespPrimitives {
    /// Backend identity used by metadata and contextual errors.
    profile: RespProfile,
    /// Shared command transport; closing remains the client's responsibility.
    executor: Arc<dyn CommandExecutor>,
}

impl RespPrimitives {
    /// Executes one RESP command and adds safe backend/operation context.
    ///
    /// Concrete Redis errors cannot fit the closed cache error enum, so they
    /// become `Internal`; command values remain untyped until the caller checks
    /// the reply shape. Credentials are never included in this context.
    async fn command(&self, operation: &str, command: Cmd) -> Result<Value> {
        self.executor
            .execute(command)
            .await
            .map_err(|error| internal(self.profile.backend, operation, error))
    }

    /// Executes one non-transactional RESP pipeline with operation context.
    async fn pipeline(&self, operation: &str, pipeline: Pipeline) -> Result<Vec<Value>> {
        self.executor
            .execute_pipeline(pipeline)
            .await
            .map_err(|error| internal(self.profile.backend, operation, error))
    }

    /// Implements unconditional, NX, and XX writes with one atomic SET.
    ///
    /// A zero TTL omits PX and means permanent storage. A positive TTL is
    /// converted with checked millisecond ceiling. Redis nil is the normal
    /// false result for an unmet NX/XX condition; other reply shapes are
    /// validated instead of being treated as success silently.
    async fn write(
        &self,
        key: &str,
        value: &[u8],
        ttl: Duration,
        condition: Option<&str>,
    ) -> Result<bool> {
        let mut command = redis::cmd("SET");
        command.arg(key).arg(value);
        if !ttl.is_zero() {
            command
                .arg("PX")
                .arg(ttl_millis(self.profile.backend, ttl)?);
        }
        if let Some(condition) = condition {
            command.arg(condition);
        }
        let response = self.command("set", command).await?;
        match response {
            Value::Nil if condition.is_some() => Ok(false),
            Value::Nil => Err(internal(
                self.profile.backend,
                "set reply",
                "unexpected nil response from unconditional SET",
            )),
            value => {
                let _: String = decode(self.profile.backend, "set reply", value)?;
                Ok(true)
            }
        }
    }
}

#[async_trait]
impl Driver for RespPrimitives {
    /// Identifies this adapter locally; no Redis round trip or side effect.
    fn name(&self) -> &str {
        self.profile.backend
    }

    /// Reads bytes with GET in one round trip.
    ///
    /// Redis nil is the cache miss sentinel. Transport and protocol failures
    /// remain errors so an outage cannot masquerade as absent application data.
    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        let mut command = redis::cmd("GET");
        command.arg(key);
        match self.command("get", command).await? {
            Value::Nil => Err(CacheError::NotFound),
            value => decode(self.profile.backend, "get reply", value),
        }
    }

    /// Writes bytes unconditionally with one SET round trip.
    ///
    /// Zero TTL stores permanently; positive TTL emits PX milliseconds. This
    /// changes the value and replaces any previous lease for the key.
    async fn set(&self, key: &str, value: &[u8], ttl: Duration) -> Result<()> {
        self.write(key, value, ttl, None).await?;
        Ok(())
    }

    /// Creates an absent key atomically with SET NX in one round trip.
    ///
    /// Returns false without modifying Redis when the key already exists.
    async fn add(&self, key: &str, value: &[u8], ttl: Duration) -> Result<bool> {
        self.write(key, value, ttl, Some("NX")).await
    }

    /// Replaces an existing key atomically with SET XX in one round trip.
    ///
    /// Returns false without modifying Redis when the key is absent.
    async fn replace(&self, key: &str, value: &[u8], ttl: Duration) -> Result<bool> {
        self.write(key, value, ttl, Some("XX")).await
    }

    /// Removes all supplied keys with one DEL round trip.
    ///
    /// Missing keys are harmless. Empty input is a local no-op, avoiding an
    /// invalid Redis command and preserving the Driver contract.
    async fn delete(&self, keys: &[&str]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let mut command = redis::cmd("DEL");
        command.arg(keys);
        let _: i64 = decode(
            self.profile.backend,
            "delete reply",
            self.command("delete", command).await?,
        )?;
        Ok(())
    }

    /// Tests key liveness with EXISTS in one round trip and no mutation.
    async fn exists(&self, key: &str) -> Result<bool> {
        let mut command = redis::cmd("EXISTS");
        command.arg(key);
        let count: i64 = decode(
            self.profile.backend,
            "exists reply",
            self.command("exists", command).await?,
        )?;
        Ok(count > 0)
    }

    /// Changes an existing key's lease without rewriting its value.
    ///
    /// Positive TTL uses one PEXPIRE command. Permanent Touch uses one Lua
    /// round trip because PERSIST alone returns the same zero for a missing key
    /// and an already-permanent key. The script returns `-1` only for absence,
    /// preserving the Driver's required `NotFound` distinction and correcting
    /// GO-004 without adding a check-then-mutate race.
    async fn touch(&self, key: &str, ttl: Duration) -> Result<()> {
        let result = if ttl.is_zero() {
            let mut command = redis::cmd("EVAL");
            command
                .arg("if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end return redis.call('PERSIST', KEYS[1])")
                .arg(1)
                .arg(key);
            let value = self.command("touch", command).await?;
            decode::<i64>(self.profile.backend, "touch reply", value)? >= 0
        } else {
            let mut command = redis::cmd("PEXPIRE");
            command.arg(key).arg(ttl_millis(self.profile.backend, ttl)?);
            let value = self.command("touch", command).await?;
            decode::<i64>(self.profile.backend, "touch reply", value)? > 0
        };
        if result {
            Ok(())
        } else {
            Err(CacheError::NotFound)
        }
    }
}

#[async_trait]
impl Leases for RespPrimitives {
    /// Reports remaining expiry with one PTTL round trip.
    ///
    /// Redis `-2` is missing and `-1` is permanent. A live sub-millisecond
    /// result can be zero, so it is rounded up to one millisecond to preserve
    /// the cache contract's exclusive zero/permanent sentinel.
    async fn ttl(&self, key: &str) -> Result<Duration> {
        let mut command = redis::cmd("PTTL");
        command.arg(key);
        let ttl: i64 = decode(
            self.profile.backend,
            "ttl reply",
            self.command("ttl", command).await?,
        )?;
        match ttl {
            -2 => Err(CacheError::NotFound),
            -1 => Ok(Duration::ZERO),
            0 => Ok(Duration::from_millis(1)),
            milliseconds if milliseconds > 0 => {
                let milliseconds = u64::try_from(milliseconds).map_err(|error| {
                    internal(
                        self.profile.backend,
                        "ttl reply",
                        format!("positive PTTL does not fit u64: {error}"),
                    )
                })?;
                Ok(Duration::from_millis(milliseconds))
            }
            sentinel => Err(internal(
                self.profile.backend,
                "ttl reply",
                format!("unexpected Redis PTTL sentinel {sentinel}"),
            )),
        }
    }
}

#[async_trait]
impl Bulk for RespPrimitives {
    /// Fetches ordered values with one pipeline of single-key GET commands.
    ///
    /// Individual GETs preserve future Redis Cluster compatibility; a nil reply
    /// is an ordinary miss and keeps its position as `None`.
    async fn get_many(&self, keys: &[String]) -> Result<Vec<Option<Vec<u8>>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipeline = redis::pipe();
        for key in keys {
            pipeline.cmd("GET").arg(key);
        }
        let replies = self.pipeline("get many", pipeline).await?;
        validate_bulk_replies(self.profile.backend, "get many", keys.len(), replies.len())?;
        replies
            .into_iter()
            .map(|reply| match reply {
                Value::Nil => Ok(None),
                value => decode(self.profile.backend, "get-many reply", value).map(Some),
            })
            .collect()
    }

    /// Checks ordered liveness with one pipeline of single-key EXISTS commands.
    async fn exists_many(&self, keys: &[String]) -> Result<Vec<bool>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipeline = redis::pipe();
        for key in keys {
            pipeline.cmd("EXISTS").arg(key);
        }
        let replies = self.pipeline("exists many", pipeline).await?;
        validate_bulk_replies(
            self.profile.backend,
            "exists many",
            keys.len(),
            replies.len(),
        )?;
        replies
            .into_iter()
            .map(|reply| {
                decode::<i64>(self.profile.backend, "exists-many reply", reply)
                    .map(|count| count > 0)
            })
            .collect()
    }
}

#[async_trait]
impl Fenced for RespPrimitives {
    /// Atomically compares and deletes one key with a Lua script.
    ///
    /// **Cost**: One EVAL round trip. Redis executes comparison and deletion as
    /// one indivisible server-side operation, so lease turnover cannot create a
    /// GET/DEL race. Missing and mismatched values return false.
    async fn delete_if(&self, key: &str, expected: &[u8]) -> Result<bool> {
        let mut command = redis::cmd("EVAL");
        command
            .arg("if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0")
            .arg(1)
            .arg(key)
            .arg(expected);
        let deleted: i64 = decode(
            self.profile.backend,
            "delete-if reply",
            self.command("delete if", command).await?,
        )?;
        Ok(deleted > 0)
    }
}

#[async_trait]
impl Sets for RespPrimitives {
    /// Adds members with one SADD round trip.
    ///
    /// Redis sets deduplicate members. Empty input is a local no-op because
    /// Redis requires at least one member argument.
    async fn set_add(&self, key: &str, members: &[&str]) -> Result<()> {
        if members.is_empty() {
            return Ok(());
        }
        let mut command = redis::cmd("SADD");
        command.arg(key).arg(members);
        let _: i64 = decode(
            self.profile.backend,
            "set-add reply",
            self.command("set add", command).await?,
        )?;
        Ok(())
    }

    /// Removes members with one SREM round trip.
    ///
    /// Missing members and sets are harmless; empty input performs no I/O.
    async fn set_remove(&self, key: &str, members: &[&str]) -> Result<()> {
        if members.is_empty() {
            return Ok(());
        }
        let mut command = redis::cmd("SREM");
        command.arg(key).arg(members);
        let _: i64 = decode(
            self.profile.backend,
            "set-remove reply",
            self.command("set remove", command).await?,
        )?;
        Ok(())
    }

    /// Returns all members with one SMEMBERS round trip and no mutation.
    ///
    /// A missing Redis set decodes to an empty vector, matching the capability
    /// contract. Large sets motivate a future SetScanner capability.
    async fn set_members(&self, key: &str) -> Result<Vec<String>> {
        let mut command = redis::cmd("SMEMBERS");
        command.arg(key);
        decode(
            self.profile.backend,
            "set-members reply",
            self.command("set members", command).await?,
        )
    }
}

#[async_trait]
impl SetScanner for RespPrimitives {
    /// Walks one Redis set with SSCAN and processes each page immediately.
    ///
    /// **Cost**: One SSCAN round trip per cursor page with a count hint of 256.
    /// **Side effects**: Driver-side none; the visitor may perform cleanup.
    async fn set_scan(&self, key: &str, visitor: &mut dyn SetScanVisitor) -> Result<()> {
        let mut cursor = 0_u64;
        loop {
            let mut command = redis::cmd("SSCAN");
            command.arg(key).arg(cursor).arg("COUNT").arg(SCAN_BATCH);
            let response = self.command("set scan", command).await?;
            let (next, members): (u64, Vec<String>) =
                decode(self.profile.backend, "set-scan reply", response)?;
            if !members.is_empty() {
                visitor.visit(members).await?;
            }
            if next == 0 {
                return Ok(());
            }
            cursor = next;
        }
    }
}

#[async_trait]
impl Scanner for RespPrimitives {
    /// Walks keys matching Redis glob syntax using cursor-based SCAN.
    ///
    /// **Cost**: One or more SCAN round trips with a count hint of 256.
    /// **Side effects**: None. Results are accumulated because the current
    /// Scanner contract returns one vector.
    /// **Concurrency**: Redis may repeat keys while the keyspace changes, so
    /// this adapter deduplicates results while preserving first-seen order.
    async fn scan(&self, pattern: &str) -> Result<Vec<String>> {
        let mut cursor = 0_u64;
        let mut unique = HashSet::new();
        let mut keys = Vec::new();
        loop {
            let mut command = redis::cmd("SCAN");
            command
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(SCAN_BATCH);
            let response = self.command("scan", command).await?;
            let (next, batch): (u64, Vec<String>) =
                decode(self.profile.backend, "scan reply", response)?;
            for key in batch {
                if unique.insert(key.clone()) {
                    keys.push(key);
                }
            }
            if next == 0 {
                return Ok(keys);
            }
            cursor = next;
        }
    }
}

/// Parses the deliberately narrow standalone address forms accepted publicly.
///
/// Hostnames/IPv4 use `host:port`; IPv6 must use `[address]:port` so the final
/// colon is unambiguous. Empty hosts, missing ports, zero, and out-of-range
/// ports fail locally before credentials or network state are touched.
fn parse_address(backend: &str, address: &str) -> Result<(String, u16)> {
    let (host, port) = if let Some(rest) = address.strip_prefix('[') {
        let (host, port) = rest.split_once("]:").ok_or_else(|| {
            CacheError::Internal(format!(
                "{backend}: address must be host:port or [ipv6]:port"
            ))
        })?;
        (host, port)
    } else {
        address.rsplit_once(':').ok_or_else(|| {
            CacheError::Internal(format!(
                "{backend}: address must be host:port or [ipv6]:port"
            ))
        })?
    };
    if host.is_empty() {
        return Err(CacheError::Internal(format!(
            "{backend}: address host cannot be empty"
        )));
    }
    let port = port.parse::<u16>().map_err(|_| {
        CacheError::Internal(format!(
            "{backend}: address port must be between 1 and 65535"
        ))
    })?;
    if port == 0 {
        return Err(CacheError::Internal(format!(
            "{backend}: address port must be between 1 and 65535"
        )));
    }
    Ok((host.to_owned(), port))
}

/// Resolves the public timeout policy into redis-rs manager configuration.
///
/// `Default` deliberately avoids calling the setter, preserving future redis-rs
/// default changes. `Disabled` is the only branch that installs `None`.
fn connection_manager_config(
    backend: &str,
    timeout: DialTimeout,
) -> Result<ConnectionManagerConfig> {
    match timeout {
        DialTimeout::Default => Ok(ConnectionManagerConfig::new()),
        DialTimeout::Disabled => Ok(ConnectionManagerConfig::new().set_connection_timeout(None)),
        DialTimeout::After(duration) if duration.is_zero() => Err(CacheError::Internal(format!(
            "{backend}: dial timeout must be positive; use Default or Disabled"
        ))),
        DialTimeout::After(duration) => {
            Ok(ConnectionManagerConfig::new().set_connection_timeout(Some(duration)))
        }
    }
}

/// Converts a Rust duration to Redis milliseconds without shortening a lease.
///
/// Any positive fractional millisecond rounds up. Rounding down could turn a
/// positive TTL into zero, whose meaning differs dangerously between SET and
/// PEXPIRE. Values outside Redis's integer range return an error.
fn ttl_millis(backend: &str, ttl: Duration) -> Result<u64> {
    let whole = ttl.as_millis();
    let rounded = if ttl.subsec_nanos() % 1_000_000 == 0 {
        whole
    } else {
        whole.checked_add(1).ok_or_else(|| {
            CacheError::Internal(format!("{backend}: TTL exceeds millisecond range"))
        })?
    };
    u64::try_from(rounded)
        .map_err(|_| CacheError::Internal(format!("{backend}: TTL exceeds millisecond range")))
}

/// Decodes one raw protocol value and attaches operation context on mismatch.
fn decode<T: FromRedisValue>(backend: &str, operation: &str, value: Value) -> Result<T> {
    redis::from_redis_value(value).map_err(|error| internal(backend, operation, error))
}

/// Converts a backend failure without exposing connection configuration.
///
/// The closed cache error enum cannot retain redis-rs's concrete error type, so
/// the message records the backend and safe operation name for diagnostics.
fn internal(backend: &str, operation: &str, error: impl Display) -> CacheError {
    CacheError::Internal(format!("{backend}: {operation}: {error}"))
}

/// Rejects malformed pipeline result counts before ordered replies are exposed.
fn validate_bulk_replies(
    backend: &str,
    operation: &str,
    expected: usize,
    actual: usize,
) -> Result<()> {
    if expected == actual {
        return Ok(());
    }
    Err(internal(
        backend,
        operation,
        format!("received {actual} replies for {expected} commands"),
    ))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::atomic::{AtomicBool, Ordering},
    };

    use tokio::sync::Mutex;

    use super::*;

    /// Records packed commands and returns queued protocol replies.
    ///
    /// This verifies the private primitive layer without weakening its
    /// visibility or requiring a network service in unit tests.
    struct ScriptedExecutor {
        responses: Mutex<VecDeque<RedisResult<Value>>>,
        commands: Mutex<Vec<Vec<u8>>>,
        closed: AtomicBool,
    }
    impl ScriptedExecutor {
        fn new(responses: impl IntoIterator<Item = RedisResult<Value>>) -> Self {
            Self {
                responses: Mutex::new(responses.into_iter().collect()),
                commands: Mutex::new(Vec::new()),
                closed: AtomicBool::new(false),
            }
        }

        async fn commands(&self) -> Vec<Vec<u8>> {
            self.commands.lock().await.clone()
        }
    }

    #[async_trait]
    impl CommandExecutor for ScriptedExecutor {
        async fn execute(&self, command: Cmd) -> RedisResult<Value> {
            self.commands
                .lock()
                .await
                .push(command.get_packed_command());
            match self.responses.lock().await.pop_front() {
                Some(response) => response,
                None => Err(RedisError::from((
                    ErrorKind::Client,
                    "missing scripted reply",
                ))),
            }
        }

        async fn execute_pipeline(&self, pipeline: Pipeline) -> RedisResult<Vec<Value>> {
            self.commands
                .lock()
                .await
                .push(pipeline.get_packed_pipeline());
            match self.responses.lock().await.pop_front() {
                Some(Ok(Value::Array(replies))) => Ok(replies),
                Some(Ok(_)) => Err(RedisError::from((
                    ErrorKind::Client,
                    "scripted pipeline reply must be an array",
                ))),
                Some(Err(error)) => Err(error),
                None => Err(RedisError::from((
                    ErrorKind::Client,
                    "missing scripted pipeline reply",
                ))),
            }
        }

        async fn close(&self) {
            self.closed.store(true, Ordering::SeqCst);
        }
    }

    struct PageVisitor {
        pages: Vec<Vec<String>>,
        fail: bool,
    }

    #[async_trait]
    impl SetScanVisitor for PageVisitor {
        async fn visit(&mut self, members: Vec<String>) -> Result<()> {
            if self.fail {
                return Err(CacheError::Internal("visitor failed".to_owned()));
            }
            self.pages.push(members);
            Ok(())
        }
    }

    /// Records derived-index configuration and returns queued fake clients.
    ///
    /// Provider lifecycle tests use it to distinguish the borrowed root from a
    /// DB-owned derived client deterministically.
    struct ScriptedConnector {
        clients: Mutex<VecDeque<Result<Arc<RedisClient>>>>,
        configs: Mutex<Vec<RedisConfig>>,
        profiles: Mutex<Vec<RespProfile>>,
    }

    impl ScriptedConnector {
        fn new(clients: impl IntoIterator<Item = Result<Arc<RedisClient>>>) -> Self {
            Self {
                clients: Mutex::new(clients.into_iter().collect()),
                configs: Mutex::new(Vec::new()),
                profiles: Mutex::new(Vec::new()),
            }
        }

        async fn configs(&self) -> Vec<RedisConfig> {
            self.configs.lock().await.clone()
        }

        async fn profiles(&self) -> Vec<RespProfile> {
            self.profiles.lock().await.clone()
        }
    }

    #[async_trait]
    impl RespConnector for ScriptedConnector {
        async fn connect(
            &self,
            config: RedisConfig,
            profile: RespProfile,
        ) -> Result<Arc<RedisClient>> {
            self.configs.lock().await.push(config);
            self.profiles.lock().await.push(profile);
            match self.clients.lock().await.pop_front() {
                Some(result) => result,
                None => Err(CacheError::Internal(format!(
                    "{}: missing scripted connection",
                    profile.backend
                ))),
            }
        }
    }

    /// Builds one primitive adapter and retains its observable test executor.
    fn primitives(
        responses: impl IntoIterator<Item = RedisResult<Value>>,
    ) -> (RespPrimitives, Arc<ScriptedExecutor>) {
        let executor = Arc::new(ScriptedExecutor::new(responses));
        (
            RespPrimitives {
                profile: REDIS_PROFILE,
                executor: executor.clone(),
            },
            executor,
        )
    }

    /// Produces the exact RESP bytes used for command assertions.
    fn packed(command: Cmd) -> Vec<u8> {
        command.get_packed_command()
    }

    /// Builds a fake client bound to one native index without network I/O.
    fn client(
        database: usize,
        responses: impl IntoIterator<Item = RedisResult<Value>>,
    ) -> (Arc<RedisClient>, Arc<ScriptedExecutor>) {
        profiled_client(database, REDIS_PROFILE, responses)
    }

    /// Builds a fake client with a selected internal RESP profile.
    fn profiled_client(
        database: usize,
        profile: RespProfile,
        responses: impl IntoIterator<Item = RedisResult<Value>>,
    ) -> (Arc<RedisClient>, Arc<ScriptedExecutor>) {
        let executor = Arc::new(ScriptedExecutor::new(responses));
        let client = Arc::new(RedisClient::with_profile_executor(
            RedisConfig {
                address: DEFAULT_REDIS_ADDRESS.to_owned(),
                database,
                ..RedisConfig::default()
            },
            profile,
            executor.clone(),
        ));
        (client, executor)
    }

    #[test]
    fn test_redis_config_default_uses_deferred_default_address() {
        let config = RedisConfig::default();

        assert!(config.address.is_empty());
        assert_eq!(config.database, 0);
        assert_eq!(config.dial_timeout, DialTimeout::Default);
    }

    #[test]
    fn test_redis_dial_timeout_resolves_all_explicit_policies() {
        let default = connection_manager_config("redis", DialTimeout::Default).unwrap();
        let disabled = connection_manager_config("redis", DialTimeout::Disabled).unwrap();
        let custom =
            connection_manager_config("redis", DialTimeout::After(Duration::from_secs(3))).unwrap();

        assert_eq!(
            default.connection_timeout(),
            ConnectionManagerConfig::new().connection_timeout()
        );
        assert_eq!(disabled.connection_timeout(), None);
        assert_eq!(custom.connection_timeout(), Some(Duration::from_secs(3)));
    }

    #[test]
    fn test_redis_dial_timeout_rejects_zero_after_duration() {
        let result = connection_manager_config("redis", DialTimeout::After(Duration::ZERO));

        assert!(
            matches!(result, Err(CacheError::Internal(message)) if message.contains("Default or Disabled"))
        );
    }

    #[tokio::test]
    async fn test_redis_client_connect_invalid_address_fails_before_network() {
        let result = RedisClient::connect(RedisConfig {
            address: "missing-port".to_owned(),
            ..RedisConfig::default()
        })
        .await;

        assert!(
            matches!(result, Err(CacheError::Internal(message)) if message.contains("host:port"))
        );
    }

    #[tokio::test]
    async fn test_redis_client_close_releases_executor() {
        let executor = Arc::new(ScriptedExecutor::new([]));
        let client = RedisClient::with_executor(RedisConfig::default(), executor.clone());

        client.close().await;

        assert!(executor.closed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_redis_driver_get_hit_and_miss() {
        let (driver, _) = primitives([Ok(Value::BulkString(b"value".to_vec())), Ok(Value::Nil)]);

        assert_eq!(driver.get("key").await.unwrap(), b"value");
        assert!(matches!(
            driver.get("missing").await,
            Err(CacheError::NotFound)
        ));
    }

    #[tokio::test]
    async fn test_redis_driver_set_add_replace_encode_atomic_conditions() {
        let (driver, executor) = primitives([Ok(Value::Okay), Ok(Value::Okay), Ok(Value::Nil)]);

        driver.set("plain", b"one", Duration::ZERO).await.unwrap();
        assert!(
            driver
                .add("leased", b"two", Duration::from_micros(1))
                .await
                .unwrap()
        );
        assert!(
            !driver
                .replace("missing", b"three", Duration::from_secs(2))
                .await
                .unwrap()
        );

        let mut set = redis::cmd("SET");
        set.arg("plain").arg(b"one".as_slice());
        let mut add = redis::cmd("SET");
        add.arg("leased")
            .arg(b"two".as_slice())
            .arg("PX")
            .arg(1_u64)
            .arg("NX");
        let mut replace = redis::cmd("SET");
        replace
            .arg("missing")
            .arg(b"three".as_slice())
            .arg("PX")
            .arg(2_000_u64)
            .arg("XX");
        assert_eq!(executor.commands().await, vec![
            packed(set),
            packed(add),
            packed(replace)
        ]);
    }

    #[tokio::test]
    async fn test_redis_driver_unconditional_set_rejects_nil_reply() {
        let (driver, _) = primitives([Ok(Value::Nil)]);

        let result = driver.set("key", b"value", Duration::ZERO).await;

        assert!(
            matches!(result, Err(CacheError::Internal(message)) if message.contains("unconditional SET"))
        );
    }

    #[tokio::test]
    async fn test_redis_driver_delete_and_exists_map_integer_replies() {
        let (driver, executor) = primitives([Ok(Value::Int(2)), Ok(Value::Int(1))]);

        driver.delete(&["one", "two"]).await.unwrap();
        assert!(driver.exists("one").await.unwrap());

        let mut delete = redis::cmd("DEL");
        delete.arg(&["one", "two"]);
        let mut exists = redis::cmd("EXISTS");
        exists.arg("one");
        assert_eq!(executor.commands().await, vec![
            packed(delete),
            packed(exists)
        ]);
    }

    #[tokio::test]
    async fn test_redis_driver_empty_delete_is_local_noop() {
        let (driver, executor) = primitives([]);

        driver.delete(&[]).await.unwrap();

        assert!(executor.commands().await.is_empty());
    }

    #[tokio::test]
    async fn test_redis_driver_command_error_preserves_operation_context() {
        let (driver, _) = primitives([Err(RedisError::from((
            ErrorKind::Client,
            "scripted failure",
        )))]);

        let result = driver.get("key").await;

        assert!(
            matches!(result, Err(CacheError::Internal(message)) if message.contains("redis: get"))
        );
    }

    #[tokio::test]
    async fn test_redis_driver_touch_handles_lease_permanence_and_miss() {
        let (driver, executor) =
            primitives([Ok(Value::Int(1)), Ok(Value::Int(0)), Ok(Value::Int(-1))]);

        driver
            .touch("permanent", Duration::from_millis(5))
            .await
            .unwrap();
        driver
            .touch("already-permanent", Duration::ZERO)
            .await
            .unwrap();
        assert!(matches!(
            driver.touch("missing", Duration::ZERO).await,
            Err(CacheError::NotFound)
        ));

        let mut expire = redis::cmd("PEXPIRE");
        expire.arg("permanent").arg(5_u64);
        let script = "if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end return redis.call('PERSIST', KEYS[1])";
        let mut persist = redis::cmd("EVAL");
        persist.arg(script).arg(1).arg("already-permanent");
        let mut missing = redis::cmd("EVAL");
        missing.arg(script).arg(1).arg("missing");
        assert_eq!(executor.commands().await, vec![
            packed(expire),
            packed(persist),
            packed(missing)
        ]);
    }

    #[tokio::test]
    async fn test_redis_leases_pttl_maps_duration_permanent_and_missing() {
        let (driver, executor) = primitives([
            Ok(Value::Int(275)),
            Ok(Value::Int(0)),
            Ok(Value::Int(-1)),
            Ok(Value::Int(-2)),
        ]);

        assert_eq!(
            Leases::ttl(&driver, "leased").await.unwrap(),
            Duration::from_millis(275)
        );
        assert_eq!(
            Leases::ttl(&driver, "sub-millisecond").await.unwrap(),
            Duration::from_millis(1)
        );
        assert_eq!(
            Leases::ttl(&driver, "permanent").await.unwrap(),
            Duration::ZERO
        );
        assert!(matches!(
            Leases::ttl(&driver, "missing").await,
            Err(CacheError::NotFound)
        ));

        let mut leased = redis::cmd("PTTL");
        leased.arg("leased");
        let mut sub_millisecond = redis::cmd("PTTL");
        sub_millisecond.arg("sub-millisecond");
        let mut permanent = redis::cmd("PTTL");
        permanent.arg("permanent");
        let mut missing = redis::cmd("PTTL");
        missing.arg("missing");
        assert_eq!(executor.commands().await, vec![
            packed(leased),
            packed(sub_millisecond),
            packed(permanent),
            packed(missing)
        ]);
    }

    #[tokio::test]
    async fn test_redis_leases_pttl_rejects_unknown_negative_sentinel() {
        let (driver, _) = primitives([Ok(Value::Int(-3))]);

        let result = Leases::ttl(&driver, "key").await;

        assert!(
            matches!(result, Err(CacheError::Internal(message)) if message.contains("sentinel -3"))
        );
    }

    #[tokio::test]
    async fn test_redis_bulk_pipelines_ordered_gets_and_exists() {
        let (driver, executor) = primitives([
            Ok(Value::Array(vec![
                Value::BulkString(b"first".to_vec()),
                Value::Nil,
            ])),
            Ok(Value::Array(vec![Value::Int(1), Value::Int(0)])),
        ]);
        let keys = vec!["one".to_owned(), "missing".to_owned()];

        assert_eq!(Bulk::get_many(&driver, &keys).await.unwrap(), vec![
            Some(b"first".to_vec()),
            None
        ]);
        assert_eq!(Bulk::exists_many(&driver, &keys).await.unwrap(), vec![
            true, false
        ]);
        assert!(Bulk::get_many(&driver, &[]).await.unwrap().is_empty());
        assert!(Bulk::exists_many(&driver, &[]).await.unwrap().is_empty());

        let mut gets = redis::pipe();
        gets.cmd("GET").arg("one");
        gets.cmd("GET").arg("missing");
        let mut exists = redis::pipe();
        exists.cmd("EXISTS").arg("one");
        exists.cmd("EXISTS").arg("missing");
        assert_eq!(executor.commands().await, vec![
            gets.get_packed_pipeline(),
            exists.get_packed_pipeline()
        ]);
    }

    #[tokio::test]
    async fn test_redis_bulk_rejects_wrong_pipeline_reply_count() {
        let (driver, _) = primitives([Ok(Value::Array(vec![Value::Nil]))]);
        let keys = vec!["one".to_owned(), "two".to_owned()];

        let result = Bulk::get_many(&driver, &keys).await;

        assert!(
            matches!(result, Err(CacheError::Internal(message)) if message.contains("1 replies for 2 commands"))
        );
    }

    #[tokio::test]
    async fn test_redis_fenced_delete_if_maps_mismatch_and_owner_delete() {
        let (driver, executor) = primitives([Ok(Value::Int(0)), Ok(Value::Int(1))]);

        assert!(!Fenced::delete_if(&driver, "lock", b"other").await.unwrap());
        assert!(Fenced::delete_if(&driver, "lock", b"owner").await.unwrap());

        let script = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0";
        let mut mismatch = redis::cmd("EVAL");
        mismatch.arg(script).arg(1).arg("lock").arg(b"other");
        let mut owner = redis::cmd("EVAL");
        owner.arg(script).arg(1).arg("lock").arg(b"owner");
        assert_eq!(executor.commands().await, vec![
            packed(mismatch),
            packed(owner)
        ]);
    }

    #[tokio::test]
    async fn test_redis_fenced_delete_if_invalid_reply_propagates_error() {
        let (driver, _) = primitives([Ok(Value::SimpleString("wrong".to_owned()))]);

        let result = Fenced::delete_if(&driver, "lock", b"owner").await;

        assert!(
            matches!(result, Err(CacheError::Internal(message)) if message.contains("delete-if reply"))
        );
    }

    #[tokio::test]
    async fn test_redis_sets_commands_and_empty_noops() {
        let (driver, executor) = primitives([
            Ok(Value::Int(2)),
            Ok(Value::Int(1)),
            Ok(Value::Array(vec![
                Value::BulkString(b"one".to_vec()),
                Value::BulkString(b"two".to_vec()),
            ])),
        ]);

        driver.set_add("set", &["one", "two"]).await.unwrap();
        driver.set_remove("set", &["two"]).await.unwrap();
        assert_eq!(driver.set_members("set").await.unwrap(), ["one", "two"]);
        driver.set_add("set", &[]).await.unwrap();
        driver.set_remove("set", &[]).await.unwrap();

        assert_eq!(executor.commands().await.len(), 3);
    }

    #[tokio::test]
    async fn test_redis_set_scanner_walks_pages_without_accumulating() {
        let (driver, executor) = primitives([
            Ok(Value::Array(vec![
                Value::BulkString(b"7".to_vec()),
                Value::Array(vec![Value::BulkString(b"one".to_vec())]),
            ])),
            Ok(Value::Array(vec![
                Value::BulkString(b"0".to_vec()),
                Value::Array(vec![
                    Value::BulkString(b"one".to_vec()),
                    Value::BulkString(b"two".to_vec()),
                ]),
            ])),
        ]);
        let mut visitor = PageVisitor {
            pages: Vec::new(),
            fail: false,
        };

        driver.set_scan("members", &mut visitor).await.unwrap();

        assert_eq!(visitor.pages, vec![vec!["one".to_owned()], vec![
            "one".to_owned(),
            "two".to_owned()
        ]]);
        let mut first = redis::cmd("SSCAN");
        first.arg("members").arg(0_u64).arg("COUNT").arg(256_usize);
        let mut second = redis::cmd("SSCAN");
        second.arg("members").arg(7_u64).arg("COUNT").arg(256_usize);
        assert_eq!(executor.commands().await, vec![
            packed(first),
            packed(second)
        ]);
    }

    #[tokio::test]
    async fn test_redis_set_scanner_stops_after_visitor_failure() {
        let (driver, executor) = primitives([
            Ok(Value::Array(vec![
                Value::BulkString(b"7".to_vec()),
                Value::Array(vec![Value::BulkString(b"one".to_vec())]),
            ])),
            Ok(Value::Array(vec![
                Value::BulkString(b"0".to_vec()),
                Value::Array(Vec::new()),
            ])),
        ]);
        let mut visitor = PageVisitor {
            pages: Vec::new(),
            fail: true,
        };

        let result = driver.set_scan("members", &mut visitor).await;

        assert!(matches!(
            result,
            Err(CacheError::Internal(message)) if message == "visitor failed"
        ));
        assert_eq!(executor.commands().await.len(), 1);
    }

    #[tokio::test]
    async fn test_redis_scanner_walks_cursor_and_deduplicates_keys() {
        let (driver, executor) = primitives([
            Ok(Value::Array(vec![
                Value::BulkString(b"7".to_vec()),
                Value::Array(vec![Value::BulkString(b"one".to_vec())]),
            ])),
            Ok(Value::Array(vec![
                Value::BulkString(b"0".to_vec()),
                Value::Array(vec![
                    Value::BulkString(b"one".to_vec()),
                    Value::BulkString(b"two".to_vec()),
                ]),
            ])),
        ]);

        let keys = driver.scan("app:orders:cache:*").await.unwrap();

        assert_eq!(keys, ["one", "two"]);
        let commands = executor.commands().await;
        assert_eq!(commands.len(), 2);
        let mut first = redis::cmd("SCAN");
        first
            .arg(0_u64)
            .arg("MATCH")
            .arg("app:orders:cache:*")
            .arg("COUNT")
            .arg(256_usize);
        let mut second = redis::cmd("SCAN");
        second
            .arg(7_u64)
            .arg("MATCH")
            .arg("app:orders:cache:*")
            .arg("COUNT")
            .arg(256_usize);
        assert_eq!(commands, vec![packed(first), packed(second)]);
    }

    #[test]
    fn test_redis_ttl_millis_rounds_nonzero_duration_up() {
        assert_eq!(ttl_millis("redis", Duration::from_nanos(1)).unwrap(), 1);
        assert_eq!(ttl_millis("redis", Duration::from_millis(2)).unwrap(), 2);
    }

    #[test]
    fn test_redis_parse_address_supports_host_and_ipv6() {
        assert_eq!(
            parse_address("redis", "localhost:6379").unwrap(),
            ("localhost".to_owned(), 6379)
        );
        assert_eq!(
            parse_address("redis", "[::1]:6380").unwrap(),
            ("::1".to_owned(), 6380)
        );
    }

    #[test]
    fn test_redis_provider_new_reports_backend_without_io() {
        let (client, _) = client(0, []);
        let provider = RedisProvider::new(client, Config::default());

        assert_eq!(provider.backend(), "redis");
    }

    #[tokio::test]
    async fn test_resp_profile_propagates_to_driver_provider_and_database_identity() {
        let profile = RespProfile::new("compatible", "compatible.local:6380");
        let (client, _) = profiled_client(2, profile, [Ok(Value::SimpleString("PONG".to_owned()))]);
        assert_eq!(client.primitives().name(), "compatible");

        let provider = RedisProvider::new(client, Config::default());
        assert_eq!(provider.backend(), "compatible");
        let database = provider.set_database("orders").await.unwrap();
        assert_eq!(database.backend, "compatible");
    }

    #[tokio::test]
    async fn test_resp_profile_prefixes_configuration_and_command_errors() {
        let profile = RespProfile::new("compatible", "compatible.local:6380");
        let config = RedisConfig {
            address: "invalid-address".to_owned(),
            password: "credential-must-not-appear".to_owned(),
            ..RedisConfig::default()
        };
        let result = RedisClient::connect_with_profile(config, profile).await;
        assert!(matches!(result, Err(CacheError::Internal(message))
                if message.starts_with("compatible:")
                    && !message.contains("credential-must-not-appear")));

        let error = RedisError::from((ErrorKind::Client, "scripted failure"));
        let (client, _) = profiled_client(0, profile, [Err(error)]);
        assert!(matches!(client.primitives().get("key").await,
                Err(CacheError::Internal(message)) if message.contains("compatible: get")));
    }

    #[tokio::test]
    async fn test_redis_provider_set_database_validates_pings_and_wires_capabilities() {
        let (client, executor) = client(4, [
            Ok(Value::SimpleString("PONG".to_owned())),
            Ok(Value::Array(vec![
                Value::BulkString(b"0".to_vec()),
                Value::Array(vec![Value::BulkString(b"entry".to_vec())]),
            ])),
            Ok(Value::Array(vec![Value::Int(1)])),
            Ok(Value::Int(125)),
            Ok(Value::Array(vec![
                Value::BulkString(b"0".to_vec()),
                Value::Array(vec![Value::BulkString(
                    b"app:orders:cache:vol:session:one".to_vec(),
                )]),
            ])),
        ]);
        let provider = RedisProvider::new(client, Config {
            prefix: "app".to_owned(),
            databases: vec!["orders".to_owned()],
            ..Config::default()
        });

        let database = provider.set_database("orders").await.unwrap();
        let keys = database.document.keys().await.unwrap();
        let ttl = database.document.ttl("entry").await.unwrap();
        let volatile_keys = database.volatile.scan("session:*").await.unwrap();

        assert_eq!(database.name, "orders");
        assert_eq!(database.index, 4);
        assert_eq!(database.backend, "redis");
        assert_eq!(keys, ["entry".to_owned()]);
        assert_eq!(ttl, Duration::from_millis(125));
        assert_eq!(volatile_keys, [
            "app:orders:cache:vol:session:one".to_owned()
        ]);
        let mut members = redis::cmd("SSCAN");
        members
            .arg("app:orders:cache:doc:index")
            .arg(0_u64)
            .arg("COUNT")
            .arg(256_usize);
        let mut exists = redis::pipe();
        exists.cmd("EXISTS").arg("app:orders:cache:doc:entry:entry");
        let mut ttl = redis::cmd("PTTL");
        ttl.arg("app:orders:cache:doc:entry:entry");
        let mut scan = redis::cmd("SCAN");
        scan.arg(0_u64)
            .arg("MATCH")
            .arg("app:orders:cache:vol:session:*")
            .arg("COUNT")
            .arg(256_usize);
        assert_eq!(executor.commands().await, vec![
            packed(redis::cmd("PING")),
            packed(members),
            exists.get_packed_pipeline(),
            packed(ttl),
            packed(scan)
        ]);
    }

    #[tokio::test]
    async fn test_redis_provider_set_database_rejects_invalid_or_unknown_name_before_io() {
        let (client, executor) = client(0, []);
        let provider = RedisProvider::new(client, Config {
            databases: vec!["orders".to_owned()],
            ..Config::default()
        });

        assert!(provider.set_database("bad:name").await.is_err());
        assert!(provider.set_database("users").await.is_err());
        assert!(executor.commands().await.is_empty());
    }

    #[tokio::test]
    async fn test_redis_provider_select_current_index_reuses_root_without_releasing_it() {
        let (client, executor) = client(3, [Ok(Value::SimpleString("PONG".to_owned()))]);
        let provider = RedisProvider::new(client, Config::default());

        let database = provider.select_index(3).await.unwrap();
        database.close().await.unwrap();

        assert_eq!(database.index, 3);
        assert!(database.name.is_empty());
        assert!(!executor.closed.load(Ordering::SeqCst));
        assert_eq!(executor.commands().await, vec![packed(redis::cmd("PING"))]);
    }

    #[tokio::test]
    async fn test_redis_provider_select_other_index_derives_and_releases_client() {
        let (root, root_executor) = client(2, []);
        let (derived, derived_executor) = client(7, []);
        let connector = Arc::new(ScriptedConnector::new([Ok(derived)]));
        let provider = RedisProvider::with_connector(root, Config::default(), connector.clone());

        let database = provider.select_index(7).await.unwrap();
        database.close().await.unwrap();

        assert_eq!(database.index, 7);
        assert!(database.name.is_empty());
        assert!(derived_executor.closed.load(Ordering::SeqCst));
        assert!(!root_executor.closed.load(Ordering::SeqCst));
        let configs = connector.configs().await;
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].database, 7);
        assert_eq!(connector.profiles().await, vec![REDIS_PROFILE]);
    }

    #[tokio::test]
    async fn test_resp_profile_propagates_to_derived_index_connection() {
        let profile = RespProfile::new("compatible", "compatible.local:6380");
        let (root, _) = profiled_client(2, profile, []);
        let (derived, _) = profiled_client(7, profile, []);
        let connector = Arc::new(ScriptedConnector::new([Ok(derived)]));
        let provider = RedisProvider::with_connector(root, Config::default(), connector.clone());

        let database = provider.select_index(7).await.unwrap();

        assert_eq!(database.backend, "compatible");
        assert_eq!(connector.profiles().await, vec![profile]);
    }

    #[tokio::test]
    async fn test_redis_provider_select_other_index_propagates_connection_failure() {
        let (root, _) = client(0, []);
        let connector = Arc::new(ScriptedConnector::new([Err(CacheError::Internal(
            "redis: derived connection failed".to_owned(),
        ))]));
        let provider = RedisProvider::with_connector(root, Config::default(), connector);

        let result = provider.select_index(9).await;

        assert!(
            matches!(result, Err(CacheError::Internal(message)) if message.contains("derived connection failed"))
        );
    }

    #[tokio::test]
    async fn test_redis_provider_drop_database_scans_literal_head_and_deletes_matches() {
        let key = "app:orders:cache:vol:session";
        let (client, executor) = client(0, [
            Ok(Value::Array(vec![
                Value::BulkString(b"0".to_vec()),
                Value::Array(vec![Value::BulkString(key.as_bytes().to_vec())]),
            ])),
            Ok(Value::Int(1)),
        ]);
        let provider = RedisProvider::new(client, Config {
            prefix: "app".to_owned(),
            ..Config::default()
        });

        let deleted = provider.drop_database("orders").await.unwrap();

        assert_eq!(deleted, 1);
        let mut scan = redis::cmd("SCAN");
        scan.arg(0_u64)
            .arg("MATCH")
            .arg("app:orders:cache:*")
            .arg("COUNT")
            .arg(256_usize);
        let mut delete = redis::cmd("DEL");
        delete.arg(&[key]);
        assert_eq!(executor.commands().await, vec![
            packed(scan),
            packed(delete)
        ]);
    }
}
