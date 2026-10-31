# Cache contracts, capabilities, and DB construction

[Cache guide](../../README.md) · [Strategies](../strategies/README.md)

Core is the boundary between byte-oriented backend commands and cache policy.
A provider selects a backend and passes its `Driver`, optional `Capabilities`,
and `DatabaseSpec` into `build_database`, which creates all strategy views.

```rust
use std::{sync::Arc, time::Duration};
use cache::{build_database, core::{Capabilities, DatabaseSpec, MemoryDriver, MemorySets, Options}};

#[tokio::main]
async fn main() -> cache::Result<()> {
    let driver = Arc::new(MemoryDriver::new());
    let capabilities = Capabilities::new()
        .with_sets(Arc::new(MemorySets::new()))
        .with_leases(driver.clone())
        .with_bulk(driver.clone())
        .with_fenced(driver.clone());
    let db = build_database(driver, capabilities, DatabaseSpec {
        prefix: "app".into(), namespace: "orders".into(),
        default_ttl: Duration::from_secs(60), ..Default::default()
    });
    let id = db.document.create(b"order", &Options::default().with_id("order-1")).await?;
    let mut value = Vec::new();
    db.document.get(&id, &mut value).await?;
    assert_eq!(value, b"order");
    assert_eq!(db.document.keys().await?, vec!["order-1".to_owned()]);
    db.close().await
}
```

This in-memory wiring demonstrates backend construction; production callers
normally obtain the DB from a Redis/Dragonfly/Memcached provider instead.

`driver.rs` defines get/set/add/replace/delete/exists/touch primitives.
`sets.rs`, `bulk.rs`, `leases.rs`, `scanner.rs`, and `fenced.rs` define optional
backend capabilities; `capabilities.rs` assembles them explicitly. A missing
capability remains visible as `CacheError::Unsupported`, rather than being
emulated with process-local application data.

`document.rs`, `volatile.rs`, `indexed.rs`, and `aside.rs` define the public
strategy traits and Loader. `options.rs` / `options_builder.rs` carry per-call
policy; `types.rs` supplies common wrappers. `keyspace.rs` alone constructs
qualified keys, separating prefix, database namespace, strategy, and segment.

`database.rs` owns the strategy bundle, one single-flight coordinator and
bounded refresher shared by Aside views, and cancellation-safe close state.
`drop.rs` performs bounded non-atomic namespace deletion, preserving partial
progress on error. `memory_driver.rs` and `MemorySets` support local tests;
they are neither persistent nor coordinated across processes.

TTL precedence is explicit TTL, explicit permanence, default TTL, required-TTL
error, then permanent storage. Stale retention is a separate opt-in extension
to fresh TTL. A DB closes its derived resources and refresh work, not the
caller-owned root client.

```bash
cargo test -p tpp-cache --lib core
```
