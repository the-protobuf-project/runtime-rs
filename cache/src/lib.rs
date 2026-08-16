//! Cache module providing abstraction over cache backends (Redis, Memcached, etc.)

pub mod core;
pub mod strategies;
pub mod error;

pub use error::{CacheError, Result};
pub use core::{DB, Provider};

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
