//! Core cache abstractions and interfaces

pub mod aside;
pub mod capabilities;
pub mod database;
pub mod document;
pub mod driver;
pub mod indexed;
pub mod keyspace;
pub mod memory_driver;
pub mod options;
pub mod options_builder;
pub mod sets;
pub mod volatile;

pub use aside::{Aside, Loader};
pub use capabilities::Capabilities;
pub use database::{DB, DatabaseSpec, Release, build_database};
pub use document::Document;
pub use driver::{Driver, ErrMiss};
pub use indexed::Indexed;
pub use keyspace::{IDGenerator, Keyspace, check_namespace};
pub use memory_driver::MemoryDriver;
pub use options::Options;
pub use sets::{MemorySets, Sets};
pub use volatile::Volatile;

use crate::Result;

pub mod types;
pub use types::*;

/// A cache backend bound to a caller-owned client.
///
/// A Provider selects a database but exposes no cache operations itself, which
/// prevents accidental use of an implicit default database. Selecting may
/// derive resources for the returned [`DB`]; those resources belong to
/// [`DB::close`], while the root client remains the caller's responsibility.
#[async_trait::async_trait]
pub trait Provider: Send + Sync {
    /// Selects a named, key-namespaced database.
    ///
    /// **Cost**: Backend-specific connection validation plus local strategy
    /// construction. **Side effects**: May derive resources owned by the DB.
    async fn set_database(&self, name: &str) -> Result<DB>;

    /// Selects a native or backend-emulated numeric database.
    ///
    /// **Cost**: Backend-specific selection plus local strategy construction.
    /// **Side effects**: May create a derived client released by DB close.
    async fn select_index(&self, index: usize) -> Result<DB>;

    /// Deletes keys belonging to a named database and returns the count.
    ///
    /// **Cost**: Normally a non-atomic keyspace walk. Backends without a cursor
    /// return `Unsupported`. **Side effects**: Deletes matching cache keys.
    async fn drop_database(&self, name: &str) -> Result<usize>;

    /// Returns the stable backend name used in diagnostics.
    ///
    /// **Cost**: O(1), with no backend round trip or side effects.
    fn backend(&self) -> &str;
}
