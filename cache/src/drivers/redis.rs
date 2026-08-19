//! Standalone Redis client and cache primitives.

use std::{collections::HashSet, fmt::Display, sync::Arc, time::Duration};

use async_trait::async_trait;
use redis::{
    Cmd, ConnectionInfo, ErrorKind, FromRedisValue, IntoConnectionInfo, RedisConnectionInfo,
    RedisError, RedisResult, Value,
    aio::{ConnectionManager, ConnectionManagerConfig},
};
use tokio::sync::RwLock;

use crate::{
    CacheError, Result,
    core::{Driver, Scanner, Sets},
};

/// Address used when [`RedisConfig::address`] is empty.
pub const DEFAULT_REDIS_ADDRESS: &str = "localhost:6379";

#[allow(dead_code, reason = "consumed by the Redis provider milestone")]
const SCAN_BATCH: usize = 256;
#[allow(dead_code, reason = "consumed by the Redis provider milestone")]
const BACKEND: &str = "redis";

/// Connection settings for one standalone Redis client.
///
/// The client uses one multiplexed, reconnecting connection manager rather
/// than a separate pool. Cloned command handles can run concurrently without a
/// global operation lock. Cluster/ring adoption is intentionally outside this
/// first backend boundary.
pub struct RedisConfig {
    /// Redis address in `host:port` or `[ipv6]:port` form.
    pub address: String,
    /// Optional Redis ACL username; empty omits it.
    pub username: String,
    /// Optional Redis password; empty omits it.
    pub password: String,
    /// Numeric Redis database selected when each connection is established.
    pub database: usize,
    /// Bound for initial and reconnect attempts; zero uses redis-rs defaults.
    pub connection_timeout: Duration,
}

impl Clone for RedisConfig {
    fn clone(&self) -> Self {
        Self {
            address: self.address.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            database: self.database,
            connection_timeout: self.connection_timeout,
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
            connection_timeout: Duration::ZERO,
        }
    }
}

#[async_trait]
trait CommandExecutor: Send + Sync {
    async fn execute(&self, command: Cmd) -> RedisResult<Value>;
    async fn close(&self);
}

struct ManagedExecutor {
    manager: RwLock<Option<ConnectionManager>>,
}

#[async_trait]
impl CommandExecutor for ManagedExecutor {
    async fn execute(&self, command: Cmd) -> RedisResult<Value> {
        let mut manager = match self.manager.read().await.as_ref() {
            Some(manager) => manager.clone(),
            None => {
                return Err(RedisError::from((
                    ErrorKind::Client,
                    "Redis client is closed",
                )));
            }
        };
        command.query_async(&mut manager).await
    }

    async fn close(&self) {
        self.manager.write().await.take();
    }
}

/// Caller-owned connection to one standalone Redis database.
///
/// Construction connects and verifies Redis with PING, so authentication and
/// reachability failures surface during startup rather than the first cache
/// request. Cache DBs built from this client share its reconnecting manager but
/// do not close it; the caller remains responsible for [`RedisClient::close`].
pub struct RedisClient {
    #[allow(dead_code, reason = "consumed by the Redis provider milestone")]
    config: RedisConfig,
    executor: Arc<dyn CommandExecutor>,
}

impl RedisClient {
    /// Connects to Redis and verifies the selected database with PING.
    ///
    /// **Cost**: Connection/authentication plus one PING round trip.
    /// **Concurrency**: The returned handle supports concurrent commands.
    /// **Side effects**: Opens a reconnecting network connection.
    pub async fn connect(mut config: RedisConfig) -> Result<Self> {
        let address = if config.address.is_empty() {
            DEFAULT_REDIS_ADDRESS.to_owned()
        } else {
            config.address.clone()
        };
        let (host, port) = parse_address(&address)?;
        let database = i64::try_from(config.database).map_err(|_| {
            CacheError::Internal(format!(
                "redis: database index {} exceeds Redis range",
                config.database
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
            .map_err(|error| internal("configure connection", error))?
            .set_redis_settings(redis_settings);
        let redis_client = redis::Client::open(connection_info)
            .map_err(|error| internal("configure connection", error))?;
        let timeout = if config.connection_timeout.is_zero() {
            None
        } else {
            Some(config.connection_timeout)
        };
        let manager_config = ConnectionManagerConfig::new().set_connection_timeout(timeout);
        let manager = ConnectionManager::new_with_config(redis_client, manager_config)
            .await
            .map_err(|error| internal("connect", error))?;
        config.address = address;
        let client = Self {
            config,
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
    pub async fn close(&self) {
        self.executor.close().await;
    }

    async fn ping(&self) -> Result<()> {
        let value = self
            .executor
            .execute(redis::cmd("PING"))
            .await
            .map_err(|error| internal("ping", error))?;
        let _: String = decode("ping reply", value)?;
        Ok(())
    }

    #[allow(dead_code, reason = "consumed by the Redis provider milestone")]
    pub(crate) fn primitives(&self) -> Arc<RedisPrimitives> {
        Arc::new(RedisPrimitives {
            executor: self.executor.clone(),
        })
    }

    #[allow(dead_code, reason = "consumed by the Redis provider milestone")]
    pub(crate) fn config(&self) -> RedisConfig {
        self.config.clone()
    }

    #[cfg(test)]
    fn with_executor(config: RedisConfig, executor: Arc<dyn CommandExecutor>) -> Self {
        Self { config, executor }
    }
}

/// Redis implementation of the low-level cache primitives.
#[allow(dead_code, reason = "consumed by the Redis provider milestone")]
pub(crate) struct RedisPrimitives {
    executor: Arc<dyn CommandExecutor>,
}

impl RedisPrimitives {
    #[allow(dead_code, reason = "consumed by the Redis provider milestone")]
    async fn command(&self, operation: &str, command: Cmd) -> Result<Value> {
        self.executor
            .execute(command)
            .await
            .map_err(|error| internal(operation, error))
    }

    #[allow(dead_code, reason = "consumed by the Redis provider milestone")]
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
            command.arg("PX").arg(ttl_millis(ttl)?);
        }
        if let Some(condition) = condition {
            command.arg(condition);
        }
        let response = self.command("set", command).await?;
        match response {
            Value::Nil => Ok(false),
            value => {
                let _: String = decode("set reply", value)?;
                Ok(true)
            }
        }
    }
}

#[async_trait]
impl Driver for RedisPrimitives {
    fn name(&self) -> &str {
        BACKEND
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        let mut command = redis::cmd("GET");
        command.arg(key);
        match self.command("get", command).await? {
            Value::Nil => Err(CacheError::NotFound),
            value => decode("get reply", value),
        }
    }

    async fn set(&self, key: &str, value: &[u8], ttl: Duration) -> Result<()> {
        self.write(key, value, ttl, None).await?;
        Ok(())
    }

    async fn add(&self, key: &str, value: &[u8], ttl: Duration) -> Result<bool> {
        self.write(key, value, ttl, Some("NX")).await
    }

    async fn replace(&self, key: &str, value: &[u8], ttl: Duration) -> Result<bool> {
        self.write(key, value, ttl, Some("XX")).await
    }

    async fn delete(&self, keys: &[&str]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let mut command = redis::cmd("DEL");
        command.arg(keys);
        let _: i64 = decode("delete reply", self.command("delete", command).await?)?;
        Ok(())
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let mut command = redis::cmd("EXISTS");
        command.arg(key);
        let count: i64 = decode("exists reply", self.command("exists", command).await?)?;
        Ok(count > 0)
    }

    async fn touch(&self, key: &str, ttl: Duration) -> Result<()> {
        let result = if ttl.is_zero() {
            let mut command = redis::cmd("EVAL");
            command
                .arg("if redis.call('EXISTS', KEYS[1]) == 0 then return -1 end return redis.call('PERSIST', KEYS[1])")
                .arg(1)
                .arg(key);
            let value = self.command("touch", command).await?;
            decode::<i64>("touch reply", value)? >= 0
        } else {
            let mut command = redis::cmd("PEXPIRE");
            command.arg(key).arg(ttl_millis(ttl)?);
            let value = self.command("touch", command).await?;
            decode::<i64>("touch reply", value)? > 0
        };
        if result {
            Ok(())
        } else {
            Err(CacheError::NotFound)
        }
    }
}

#[async_trait]
impl Sets for RedisPrimitives {
    async fn set_add(&self, key: &str, members: &[&str]) -> Result<()> {
        if members.is_empty() {
            return Ok(());
        }
        let mut command = redis::cmd("SADD");
        command.arg(key).arg(members);
        let _: i64 = decode("set-add reply", self.command("set add", command).await?)?;
        Ok(())
    }

    async fn set_remove(&self, key: &str, members: &[&str]) -> Result<()> {
        if members.is_empty() {
            return Ok(());
        }
        let mut command = redis::cmd("SREM");
        command.arg(key).arg(members);
        let _: i64 = decode(
            "set-remove reply",
            self.command("set remove", command).await?,
        )?;
        Ok(())
    }

    async fn set_members(&self, key: &str) -> Result<Vec<String>> {
        let mut command = redis::cmd("SMEMBERS");
        command.arg(key);
        decode(
            "set-members reply",
            self.command("set members", command).await?,
        )
    }
}

#[async_trait]
impl Scanner for RedisPrimitives {
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
            let (next, batch): (u64, Vec<String>) = decode("scan reply", response)?;
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

fn parse_address(address: &str) -> Result<(String, u16)> {
    let (host, port) = if let Some(rest) = address.strip_prefix('[') {
        let (host, port) = rest.split_once("]:").ok_or_else(|| {
            CacheError::Internal("redis: address must be host:port or [ipv6]:port".to_owned())
        })?;
        (host, port)
    } else {
        address.rsplit_once(':').ok_or_else(|| {
            CacheError::Internal("redis: address must be host:port or [ipv6]:port".to_owned())
        })?
    };
    if host.is_empty() {
        return Err(CacheError::Internal(
            "redis: address host cannot be empty".to_owned(),
        ));
    }
    let port = port.parse::<u16>().map_err(|_| {
        CacheError::Internal("redis: address port must be between 1 and 65535".to_owned())
    })?;
    if port == 0 {
        return Err(CacheError::Internal(
            "redis: address port must be between 1 and 65535".to_owned(),
        ));
    }
    Ok((host.to_owned(), port))
}

#[allow(dead_code, reason = "consumed by the Redis provider milestone")]
fn ttl_millis(ttl: Duration) -> Result<u64> {
    let whole = ttl.as_millis();
    let rounded = if ttl.subsec_nanos() % 1_000_000 == 0 {
        whole
    } else {
        whole.checked_add(1).ok_or_else(|| {
            CacheError::Internal("redis: TTL exceeds millisecond range".to_owned())
        })?
    };
    u64::try_from(rounded)
        .map_err(|_| CacheError::Internal("redis: TTL exceeds millisecond range".to_owned()))
}

fn decode<T: FromRedisValue>(operation: &str, value: Value) -> Result<T> {
    redis::from_redis_value(value).map_err(|error| internal(operation, error))
}

fn internal(operation: &str, error: impl Display) -> CacheError {
    CacheError::Internal(format!("redis: {operation}: {error}"))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::atomic::{AtomicBool, Ordering},
    };

    use tokio::sync::Mutex;

    use super::*;

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

        async fn close(&self) {
            self.closed.store(true, Ordering::SeqCst);
        }
    }

    fn primitives(
        responses: impl IntoIterator<Item = RedisResult<Value>>,
    ) -> (RedisPrimitives, Arc<ScriptedExecutor>) {
        let executor = Arc::new(ScriptedExecutor::new(responses));
        (
            RedisPrimitives {
                executor: executor.clone(),
            },
            executor,
        )
    }

    fn packed(command: Cmd) -> Vec<u8> {
        command.get_packed_command()
    }

    #[test]
    fn test_redis_config_default_uses_deferred_default_address() {
        let config = RedisConfig::default();

        assert!(config.address.is_empty());
        assert_eq!(config.database, 0);
        assert!(config.connection_timeout.is_zero());
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
        assert_eq!(ttl_millis(Duration::from_nanos(1)).unwrap(), 1);
        assert_eq!(ttl_millis(Duration::from_millis(2)).unwrap(), 2);
    }

    #[test]
    fn test_redis_parse_address_supports_host_and_ipv6() {
        assert_eq!(
            parse_address("localhost:6379").unwrap(),
            ("localhost".to_owned(), 6379)
        );
        assert_eq!(
            parse_address("[::1]:6380").unwrap(),
            ("::1".to_owned(), 6380)
        );
    }
}
