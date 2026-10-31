# Cache strategy execution

[Cache guide](../../README.md) · [Contracts](../core/README.md)

Strategies combine a Driver with optional server capabilities. They never own
a second in-memory copy of cached application data. Choose Document for
ID-addressed enumerable values, Volatile for known keys, Indexed for secondary
lookups, or Aside for loading through an authoritative source.

```rust
use std::{sync::Arc, time::Duration};
use cache::{build_database, core::{Capabilities, DatabaseSpec, Loader, MemoryDriver, Options}};

#[tokio::main]
async fn main() -> cache::Result<()> {
    let driver = Arc::new(MemoryDriver::new());
    let db = build_database(driver.clone(), Capabilities::new().with_fenced(driver),
        DatabaseSpec { default_ttl: Duration::from_secs(60), ..Default::default() });
    let loader: Loader = Arc::new(|id| Box::pin(async move {
        Ok(format!("from-source:{id}").into_bytes())
    }));
    let aside = db.aside(loader);
    let mut value = Vec::new();
    aside.get_or_load("item-1", &mut value, &Options::default()).await?;
    assert_eq!(value, b"from-source:item-1");
    aside.invalidate("item-1").await?;
    db.close().await
}
```

## Operation flow

`volatile.rs` qualifies a caller key and resolves TTL before issuing direct
commands. `document.rs` stores bytes under an ID and updates an enumeration
set when Sets exists. Direct document access still works without enumeration.
`indexed.rs` adds field/value sets and inverse per-ID memberships so cleanup
does not scan every index. Multi-key updates are ordered but not transactional;
partial backend failures remain possible.

`aside.rs` reads a versioned envelope distinguishing a value, remembered
absence, and its freshness deadline. Fresh hits return immediately. Cold
misses join one in-process load (`flight.rs`); optional fenced claims reduce
duplicate work across processes. Loader `NotFound` can be remembered. Stale
values may return while `refresher.rs` admits one bounded background refresh.
When capacity is full, refresh is declined rather than queued; later reads can
retry. Close stops admission and drains accepted refresh work.

`batch.rs` preserves order and bounds fan-out, preferring native Bulk and
paged set scans when provided. `mod.rs` exposes the implementation types.
Keep key layout in `Keyspace` and backend protocol details in drivers.

```bash
cargo test -p tpp-cache --lib strategies
```
