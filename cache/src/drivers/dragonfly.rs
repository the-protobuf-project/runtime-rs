//! Dragonfly preset over the shared Redis-compatible RESP implementation.
//!
//! Dragonfly speaks the Redis protocol, so this module contributes identity,
//! defaults, and type-safe public wrappers rather than another command driver.
//! Transport, capabilities, database selection, error mapping, and lifecycle
//! remain in the profile-aware RESP path hosted by [`crate::drivers::redis`].
//!
//! The current transport supports standalone Dragonfly. Cluster/ring adoption
//! and caller-selected backend relabeling remain outside this public boundary.

use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    Config, DialTimeout, Result,
    core::{DB, Provider},
};

use super::redis::{RedisClient, RedisConfig, RedisProvider, RespProfile};

/// Stable backend identity reported by Dragonfly Providers and databases.
pub const BACKEND: &str = "dragonfly";

/// Address used when [`DragonflyConfig::address`] is empty.
pub const DEFAULT_DRAGONFLY_ADDRESS: &str = "localhost:6379";

/// Dragonfly's default database count when `--dbnum` is not configured.
///
/// This is informational: a server may choose another count, so selection is
/// verified by the server instead of rejected locally at this boundary.
pub const DEFAULT_DRAGONFLY_DATABASES: usize = 16;

/// Identity and defaults supplied to the shared RESP implementation.
const DRAGONFLY_PROFILE: RespProfile = RespProfile::new(BACKEND, DEFAULT_DRAGONFLY_ADDRESS);

/// Connection settings for one standalone Dragonfly client.
///
/// Dragonfly uses the same authenticated Redis-compatible connection model as
/// the shared RESP transport. An empty address selects
/// [`DEFAULT_DRAGONFLY_ADDRESS`]. The database count is server-configurable, so
/// `database` is not bounded by [`DEFAULT_DRAGONFLY_DATABASES`] locally.
///
/// **Trade-offs**: The reconnecting multiplexed manager has no pool-size knob
/// and this preset does not adopt cluster/ring clients. Avoiding `Debug` keeps
/// the password out of routine configuration logs.
///
/// **Use when**: Establishing one application-owned Dragonfly connection before
/// constructing a [`DragonflyProvider`].
pub struct DragonflyConfig {
    /// Dragonfly address in `host:port` or `[ipv6]:port` form.
    pub address: String,
    /// Optional ACL username; empty omits it.
    pub username: String,
    /// Optional password; empty omits it.
    pub password: String,
    /// Native Dragonfly database selected for every root connection.
    pub database: usize,
    /// Policy bounding initial and reconnect connection attempts.
    pub dial_timeout: DialTimeout,
}

impl Clone for DragonflyConfig {
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

impl Default for DragonflyConfig {
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

/// Caller-owned connection to one standalone Dragonfly database.
///
/// This type-safe wrapper selects the immutable Dragonfly RESP profile and
/// hides the shared transport implementation. Providers and selected databases
/// borrow the same inner connection; only the caller closes it.
///
/// **Trade-offs**: The wrapper prevents accidental backend relabeling and keeps
/// redis-rs private, but does not expose Go's generic RESP Wrap/Unwrap escape
/// hatch or cluster/ring adoption.
///
/// **Scalability**: Concurrent commands share one reconnecting multiplexed
/// manager. Selecting another native index derives another manager owned by
/// that returned DB.
///
/// **Best for**: A long-lived Dragonfly connection shared by one or more cache
/// Providers and selected databases.
pub struct DragonflyClient {
    /// Shared profiled RESP client; its close remains caller-controlled.
    inner: Arc<RedisClient>,
}

impl DragonflyClient {
    /// Connects to Dragonfly and verifies the configured database with PING.
    ///
    /// **Cost**: Connection/authentication plus one PING round trip.
    /// **Concurrency**: The returned handle supports concurrent commands.
    /// **Side effects**: Opens a reconnecting network connection.
    /// **When to use**: Once during startup before constructing a
    /// [`DragonflyProvider`]. Invalid configuration and reachability failures
    /// use Dragonfly context without exposing credentials.
    pub async fn connect(config: DragonflyConfig) -> Result<Self> {
        let inner =
            RedisClient::connect_with_profile(redis_config(config), DRAGONFLY_PROFILE).await?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Closes this caller-owned Dragonfly connection handle.
    ///
    /// **Cost**: Local handle release; no Dragonfly command.
    /// **Concurrency**: In-flight snapshots may finish; later commands fail.
    /// **Side effects**: Drops the transport after its last snapshot is gone.
    /// **When to use**: During shutdown after borrowed DBs have stopped.
    pub async fn close(&self) {
        self.inner.close().await;
    }

    /// Clones the shared profiled client for Provider construction.
    fn inner(&self) -> Arc<RedisClient> {
        self.inner.clone()
    }

    #[cfg(test)]
    /// Builds a service-free profiled client around a scripted RESP executor.
    fn with_executor(
        config: DragonflyConfig,
        executor: Arc<dyn super::redis::CommandExecutor>,
    ) -> Self {
        Self {
            inner: Arc::new(RedisClient::with_profile_executor(
                redis_config(config),
                DRAGONFLY_PROFILE,
                executor,
            )),
        }
    }
}

/// Dragonfly implementation of the cache [`Provider`] boundary.
///
/// This wrapper delegates every selection and capability decision to the shared
/// RESP Provider retained by its Dragonfly-profiled client. Named databases use
/// key namespaces; numeric databases use Dragonfly's native connection index.
///
/// **Trade-offs**: Named database deletion is a safe non-atomic SCAN/DEL walk,
/// not FLUSHDB. Numeric selection may create another connection manager.
///
/// **Scalability**: Root selections share one multiplexed manager. Every active
/// non-root numeric database owns one derived manager until DB close.
///
/// **Best for**: Exposing all four cache strategies and the full RESP capability
/// set over a caller-owned [`DragonflyClient`].
pub struct DragonflyProvider {
    /// Shared Provider logic retaining the client's Dragonfly profile.
    inner: RedisProvider,
}

impl DragonflyProvider {
    /// Binds cache policy to a caller-owned Dragonfly client.
    ///
    /// **Cost**: Local allocation only; selection performs reachability checks.
    /// **Concurrency**: Returned DBs safely share the profiled RESP transport.
    /// **Side effects**: Does not connect, ping, or take ownership of shutdown.
    /// **When to use**: After constructing the root client and before selecting
    /// a named or numeric cache database.
    pub fn new(client: Arc<DragonflyClient>, config: Config) -> Self {
        Self {
            inner: RedisProvider::new(client.inner(), config),
        }
    }
}

#[async_trait]
impl Provider for DragonflyProvider {
    /// Selects a named namespace and PING-verifies the root Dragonfly database.
    async fn set_database(&self, name: &str) -> Result<DB> {
        self.inner.set_database(name).await
    }

    /// Selects and verifies one native Dragonfly database index.
    ///
    /// The current index reuses the root connection; another index derives a
    /// profiled connection whose release belongs to the returned DB.
    async fn select_index(&self, index: usize) -> Result<DB> {
        self.inner.select_index(index).await
    }

    /// Safely cursor-walks and deletes one named Dragonfly keyspace.
    ///
    /// **Cost**: One complete SCAN plus bounded DEL batches. **Side effects**:
    /// Permanently deletes matching keys and may report partial deletion.
    async fn drop_database(&self, name: &str) -> Result<usize> {
        self.inner.drop_database(name).await
    }

    /// Returns `dragonfly` locally without backend I/O.
    fn backend(&self) -> &str {
        self.inner.backend()
    }
}

/// Converts the preset configuration without exposing a generic RESP config.
fn redis_config(config: DragonflyConfig) -> RedisConfig {
    RedisConfig {
        address: config.address,
        username: config.username,
        password: config.password,
        database: config.database,
        dial_timeout: config.dial_timeout,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::atomic::{AtomicBool, Ordering},
    };

    use redis::{Cmd, ErrorKind, Pipeline, RedisError, RedisResult, Value};
    use tokio::sync::Mutex;

    use super::super::redis::CommandExecutor;
    use super::*;
    use crate::{CacheError, core::Options};

    /// Minimal scripted RESP transport for preset delegation tests.
    struct ScriptedExecutor {
        commands: Mutex<Vec<Vec<u8>>>,
        responses: Mutex<VecDeque<RedisResult<Value>>>,
        closed: AtomicBool,
    }

    impl ScriptedExecutor {
        /// Queues protocol replies in exact command order.
        fn new(responses: impl IntoIterator<Item = RedisResult<Value>>) -> Self {
            Self {
                commands: Mutex::new(Vec::new()),
                responses: Mutex::new(responses.into_iter().collect()),
                closed: AtomicBool::new(false),
            }
        }

        /// Returns packed commands recorded by this executor.
        async fn commands(&self) -> Vec<Vec<u8>> {
            self.commands.lock().await.clone()
        }

        /// Returns the next reply or a deterministic missing-script failure.
        async fn response(&self) -> RedisResult<Value> {
            self.responses.lock().await.pop_front().unwrap_or_else(|| {
                Err(RedisError::from((
                    ErrorKind::Client,
                    "missing scripted Dragonfly reply",
                )))
            })
        }
    }

    #[async_trait]
    impl CommandExecutor for ScriptedExecutor {
        async fn execute(&self, command: Cmd) -> RedisResult<Value> {
            if self.closed.load(Ordering::SeqCst) {
                return Err(RedisError::from((
                    ErrorKind::Client,
                    "scripted Dragonfly client is closed",
                )));
            }
            self.commands
                .lock()
                .await
                .push(command.get_packed_command());
            self.response().await
        }

        async fn execute_pipeline(&self, pipeline: Pipeline) -> RedisResult<Vec<Value>> {
            if self.closed.load(Ordering::SeqCst) {
                return Err(RedisError::from((
                    ErrorKind::Client,
                    "scripted Dragonfly client is closed",
                )));
            }
            self.commands
                .lock()
                .await
                .push(pipeline.get_packed_pipeline());
            match self.response().await? {
                Value::Array(replies) => Ok(replies),
                _ => Err(RedisError::from((
                    ErrorKind::Client,
                    "scripted Dragonfly pipeline reply must be an array",
                ))),
            }
        }

        async fn close(&self) {
            self.closed.store(true, Ordering::SeqCst);
        }
    }

    /// Builds a caller-owned fake Dragonfly client and retains its executor.
    fn client(
        database: usize,
        responses: impl IntoIterator<Item = RedisResult<Value>>,
    ) -> (Arc<DragonflyClient>, Arc<ScriptedExecutor>) {
        let executor = Arc::new(ScriptedExecutor::new(responses));
        let client = Arc::new(DragonflyClient::with_executor(
            DragonflyConfig {
                database,
                ..DragonflyConfig::default()
            },
            executor.clone(),
        ));
        (client, executor)
    }

    #[test]
    fn test_dragonfly_config_defaults_match_go_preset() {
        let config = DragonflyConfig::default();

        assert_eq!(BACKEND, "dragonfly");
        assert_eq!(DEFAULT_DRAGONFLY_ADDRESS, "localhost:6379");
        assert_eq!(DEFAULT_DRAGONFLY_DATABASES, 16);
        assert!(config.address.is_empty());
        assert_eq!(config.database, 0);
        assert_eq!(config.dial_timeout, DialTimeout::Default);
    }

    #[tokio::test]
    async fn test_dragonfly_client_invalid_config_uses_safe_backend_context() {
        let result = DragonflyClient::connect(DragonflyConfig {
            address: "invalid-address".to_owned(),
            password: "credential-must-not-appear".to_owned(),
            ..DragonflyConfig::default()
        })
        .await;

        assert!(matches!(result, Err(CacheError::Internal(message))
                if message.starts_with("dragonfly:")
                    && !message.contains("credential-must-not-appear")));
    }

    #[tokio::test]
    async fn test_dragonfly_provider_named_database_reports_identity_and_capabilities() {
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
        let provider = DragonflyProvider::new(client, Config {
            prefix: "app".to_owned(),
            databases: vec!["orders".to_owned()],
            ..Config::default()
        });

        assert_eq!(provider.backend(), "dragonfly");
        let database = provider.set_database("orders").await.unwrap();
        assert_eq!(database.backend, "dragonfly");
        assert_eq!(database.name, "orders");
        assert_eq!(database.index, 4);
        assert_eq!(database.document.keys().await.unwrap(), vec!["entry"]);
        assert_eq!(
            database.document.ttl("entry").await.unwrap(),
            std::time::Duration::from_millis(125)
        );
        assert_eq!(database.volatile.scan("session:*").await.unwrap(), vec![
            "app:orders:cache:vol:session:one"
        ]);
        assert_eq!(
            executor.commands().await.first(),
            Some(&redis::cmd("PING").get_packed_command())
        );
    }

    #[tokio::test]
    async fn test_dragonfly_provider_current_index_and_close_preserve_root_ownership() {
        let (client, executor) = client(3, [
            Ok(Value::SimpleString("PONG".to_owned())),
            Ok(Value::SimpleString("OK".to_owned())),
        ]);
        let provider = DragonflyProvider::new(client.clone(), Config::default());

        let database = provider.select_index(3).await.unwrap();
        database.close().await.unwrap();
        database
            .volatile
            .set("after-db-close", b"value", &Options::default())
            .await
            .unwrap();
        assert!(!executor.closed.load(Ordering::SeqCst));

        client.close().await;
        assert!(executor.closed.load(Ordering::SeqCst));
        assert!(
            database
                .volatile
                .get("closed", &mut Vec::new())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_dragonfly_provider_drop_uses_scan_without_flush() {
        let (client, executor) = client(0, [Ok(Value::Array(vec![
            Value::BulkString(b"0".to_vec()),
            Value::Array(Vec::new()),
        ]))]);
        let provider = DragonflyProvider::new(client, Config {
            prefix: "app".to_owned(),
            ..Config::default()
        });

        assert_eq!(provider.drop_database("orders").await.unwrap(), 0);
        let commands = executor.commands().await;
        let mut scan = redis::cmd("SCAN");
        scan.arg(0_u64)
            .arg("MATCH")
            .arg("app:orders:cache:*")
            .arg("COUNT")
            .arg(256_usize);
        assert_eq!(commands, vec![scan.get_packed_command()]);
    }
}
