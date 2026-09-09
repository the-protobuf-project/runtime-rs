//! Backend-independent cache strategies over Redis, Dragonfly, and Memcached.
//!
//! The crate separates cache policy from storage protocols. Applications first
//! connect a caller-owned backend client, bind cache-wide [`Config`] policy to
//! its Provider, and then explicitly select a database. The resulting [`DB`]
//! exposes four strategies wired over the selected backend.
//!
//! # Quick start
//!
//! This example is compiled but not run because connecting performs real Redis
//! I/O and verifies the server with PING.
//!
//! ```no_run
//! use std::{sync::Arc, time::Duration};
//!
//! use runtime_cache::{
//!     Config, Provider, Result, chain,
//!     core::Options,
//!     drivers::redis::{RedisClient, RedisConfig, RedisProvider},
//!     typed, with_logging_middleware, with_retry_middleware,
//! };
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Deserialize, PartialEq, Serialize)]
//! struct User {
//!     name: String,
//! }
//!
//! # async fn example() -> Result<()> {
//! let client = Arc::new(RedisClient::connect(RedisConfig {
//!     address: "localhost:6379".to_owned(),
//!     ..RedisConfig::default()
//! }).await?);
//! let provider = RedisProvider::new(client.clone(), Config {
//!     prefix: "example".to_owned(),
//!     default_ttl: Duration::from_secs(60),
//!     ..Config::default()
//! });
//! let db = provider.set_database("orders").await?;
//!
//! // Chain applies wrappers in order, so logging is outermost and observes
//! // the complete retry sequence. Typed JSON remains outside raw middleware.
//! let users = typed::<User>(chain(db.document.clone(), [
//!     with_retry_middleware(3, Duration::from_millis(25)),
//!     with_logging_middleware(),
//! ]));
//! let options = Options::default().with_ttl(Duration::from_secs(30));
//! let id = users.create(&User { name: "Ada".to_owned() }, &options).await?;
//! assert_eq!(users.get(&id).await?, User { name: "Ada".to_owned() });
//!
//! // A DB closes only resources derived while selecting it. The root client
//! // remains caller-owned and is closed separately.
//! db.close().await?;
//! client.close().await;
//! # Ok(())
//! # }
//! ```
//!
//! # Choosing a strategy
//!
//! | Strategy | Addressing | Extra state | Choose it when |
//! | --- | --- | --- | --- |
//! | [`core::Document`] | Generated or explicit ID | Enumeration set | Values must be listed or discovered by ID. |
//! | [`core::Volatile`] | Caller-known key | None | Independent TTL-oriented values need no enumeration. |
//! | [`core::Indexed`] | ID plus secondary fields | Enumeration and secondary sets | Values must be found by attributes such as tenant or e-mail. |
//! | [`core::Aside`] | Loader-specific ID | Versioned value/absence frame | Reads should load through a slower authoritative source and collapse concurrent misses. |
//!
//! Document's direct create/get/update/delete operations work without an
//! enumeration capability, but [`core::Document::keys`] and
//! [`core::Document::list`] return [`CacheError::Unsupported`] when the backend
//! has no server-side Sets. Indexed adds write and cleanup work for every
//! secondary membership. Aside can serve stale values, negatively cache loader
//! misses, and coordinate loads in-process; Redis-compatible backends also use
//! a fenced claim to reduce duplicate work across processes.
//!
//! # Selecting a database
//!
//! [`Provider::set_database`] creates a portable key namespace on the root
//! client's current database. The backend does not enforce this boundary, so
//! every participant must use the same keyspace convention.
//!
//! [`Provider::select_index`] selects a native Redis/Dragonfly database when
//! available. Selecting a different RESP index derives a connection owned by
//! the returned DB. Memcached has no native databases, so a numeric index is
//! encoded into its keys instead. Prefer a named database unless server-level
//! index isolation is specifically required.
//!
//! [`Provider::drop_database`] walks and deletes a named namespace. It is
//! non-atomic and proportional to the backend keyspace; unsupported scanning
//! returns [`CacheError::Unsupported`], and a later delete failure reports
//! earlier progress through [`CacheError::PartialDelete`].
//!
//! # Expiry policy
//!
//! Each write resolves its lease in this order:
//!
//! 1. an explicit [`Options::ttl`](core::Options::ttl);
//! 2. explicit [`Options::permanent`](core::Options::permanent);
//! 3. [`Config::default_ttl`];
//! 4. [`CacheError::NoTTL`] when [`Config::require_ttl`] is enabled;
//! 5. permanent storage.
//!
//! An explicit TTL wins over explicit permanence. This makes accidental
//! permanent entries rejectable while [`core::Options::permanent`] still
//! expresses deliberate permanence. Aside's stale window extends backend
//! retention beyond the fresh deadline; it never silently enables itself.
//!
//! # Backend capabilities
//!
//! | Capability | Redis | Dragonfly | Memcached |
//! | --- | :---: | :---: | :---: |
//! | Direct Driver operations | Yes | Yes | Yes |
//! | Bulk reads | Yes | Yes | Yes |
//! | Document enumeration / secondary Sets | Yes | Yes | No |
//! | Remaining-TTL reporting | Yes | Yes | No |
//! | Keyspace scan / named-database drop | Yes | Yes | No |
//! | Fenced cross-process Aside claim | Yes | Yes | No |
//!
//! Missing optional capabilities remain visible through the same public
//! strategy contracts and fail with [`CacheError::Unsupported`]; strategies do
//! not emulate backend state in process memory. See [`core::Capabilities`] for
//! the internal construction boundary.
//!
//! # Typed views and middleware
//!
//! Raw strategies store encoded bytes. [`Typed`] adds JSON serialization at
//! the application boundary without changing storage behavior. Document
//! [`Middleware`] is likewise provider-independent:
//!
//! - [`with_retry`] repeats safe reads and deletion, but not Create, Update, or
//!   a settled NotFound result;
//! - [`with_logging`] emits structured tracing events through the active
//!   subscriber without installing one;
//! - [`with_telemetry`] records bounded OpenTelemetry operation, duration, and
//!   hit/miss measurements through an explicitly supplied Meter.
//!
//! [`chain`] makes the last supplied middleware outermost. Put logging or
//! telemetry after retry to observe one complete logical operation; put it
//! before retry to observe each attempt.
//!
//! # Ownership, shutdown, and errors
//!
//! Backend clients are caller-owned. [`DB::close`] first drains admitted Aside
//! refreshes and then releases only resources derived for that DB; it never
//! closes the root client. Close selected databases before closing that client.
//!
//! Match [`CacheError`] variants for control flow. Formatted Internal errors
//! carry safe backend and operation context for diagnostics but are not a
//! stable classification API. Dropping an ordinary operation Future cancels
//! it; shared Aside loads and database cleanup follow their documented
//! cancellation-safe ownership rules.

#![warn(missing_docs)]

pub mod core;
mod dial_timeout;
pub mod drivers;
pub mod error;
pub mod logging;
pub mod middleware;
mod operation_timeout;
pub mod retry;
pub mod strategies;
pub mod telemetry;
pub mod typed;

pub use core::{DB, DatabaseSpec, NewId, Provider, Release, build_database};
pub use dial_timeout::DialTimeout;
pub use error::{CacheError, Result};
pub use logging::{with_logging, with_logging_middleware};
pub use middleware::{Middleware, chain};
pub use operation_timeout::OperationTimeout;
pub use retry::{with_retry, with_retry_middleware};
pub use telemetry::{with_telemetry, with_telemetry_middleware};
pub use typed::{Typed, typed};

/// Backend-neutral defaults and database-selection policy for a cache Provider.
///
/// Providers translate this shared configuration into backend connections and
/// [`DatabaseSpec`] values. Operation-specific [`core::Options`] still take
/// precedence over these defaults, so callers can select a lease or deliberate
/// permanence per write.
///
/// **Trade-offs**: Keeping common policy here makes drivers consistent, while
/// backend-specific transport settings remain in their own client configs.
/// **Scalability**: `concurrency` bounds fallback fan-out; backend-native Bulk
/// capabilities may reduce round trips further.
/// **Use when**: Constructing a Provider shared by one or more selected cache
/// databases.
#[derive(Debug)]
pub struct Config {
    /// Namespaces every key so multiple caches can share one database
    pub prefix: String,

    /// Default time-to-live for cache entries
    pub default_ttl: std::time::Duration,

    /// Require TTL on all writes, no permanent entries
    pub require_ttl: bool,

    /// Default staleness window for reads
    pub default_stale: std::time::Duration,

    /// Known database names (empty = any name allowed)
    pub databases: Vec<String>,

    /// Concurrency limit for fanout operations
    pub concurrency: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            prefix: String::new(),
            default_ttl: std::time::Duration::ZERO,
            require_ttl: false,
            default_stale: std::time::Duration::ZERO,
            databases: Vec::new(),
            concurrency: 0,
        }
    }
}
