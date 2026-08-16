//! Core cache abstractions and interfaces

pub mod aside;
pub mod document;
pub mod driver;
pub mod keyspace;
pub mod memory_driver;
pub mod options;
pub mod options_builder;
pub mod sets;
pub mod volatile;

pub use aside::{Aside, Loader};
pub use document::Document;
pub use driver::{Driver, ErrMiss};
pub use keyspace::{IDGenerator, Keyspace, check_namespace};
pub use memory_driver::MemoryDriver;
pub use options::Options;
pub use sets::{MemorySets, Sets};
pub use volatile::Volatile;

use crate::Result;
use std::sync::Arc;

pub mod types;
pub use types::*;

/// Provider is a cache backend bound to a client
#[async_trait::async_trait]
pub trait Provider: Send + Sync {
    /// Select a named database and return strategies over it
    async fn set_database(&self, ctx: &mut tokio::task::JoinSet<()>, name: &str) -> Result<DB>;

    /// Select a database by index
    async fn select_index(&self, ctx: &mut tokio::task::JoinSet<()>, index: usize) -> Result<DB>;

    /// Drop a named database and return the count of deleted keys
    async fn drop_database(&self, name: &str) -> Result<usize>;

    /// Backend name for logging (e.g., "redis", "memcache")
    fn backend(&self) -> &str;
}

/// DB is one database, exposing a strategy per field
pub struct DB {
    pub document: Arc<dyn Document>,
    pub volatile: Arc<dyn Volatile>,
    pub backend: String,
    pub name: String,
    pub index: usize,
}
