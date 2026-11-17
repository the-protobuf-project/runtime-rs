//! Caches for the `cached` tests: the runtime cache crate's own Aside over its in-memory
//! driver, so the adapter is under test too, and one that refuses every load.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use cache::core::{Capabilities, MemoryDriver, Options};
use cache::{DatabaseSpec, build_database};
use database::Result;
use database::cached::{Aside, Cache, Load, Overloaded, from_aside};

/// A read-through cache over an in-process cache database, with a minute's TTL.
pub fn memory_cache() -> Arc<dyn Cache> {
    let db = build_database(
        Arc::new(MemoryDriver::new()),
        Capabilities::new(),
        DatabaseSpec {
            prefix: "test".into(),
            namespace: "records".into(),
            default_ttl: Duration::from_secs(60),
            ..DatabaseSpec::default()
        },
    );
    from_aside(
        Arc::new(db),
        Options::default().with_ttl(Duration::from_secs(60)),
    )
}

/// A cache that will not start any load, as one past its budget: Go's fake with `overload`.
pub struct Refusing;

impl Cache for Refusing {
    fn aside(&self, _: Load) -> Arc<dyn Aside> {
        Arc::new(Refusing)
    }
}

#[async_trait::async_trait]
impl Aside for Refusing {
    async fn get_or_load(&self, key: &str) -> Result<Vec<u8>> {
        Err(Overloaded { key: key.into() }.into())
    }

    async fn invalidate(&self, _: &[String]) -> Result<()> {
        Ok(())
    }
}
