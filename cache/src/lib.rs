//! Cache module providing abstraction over cache backends (Redis, Memcached, etc.)

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
