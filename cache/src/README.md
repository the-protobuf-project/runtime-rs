# Cache assembly, typed views, and middleware

[Crate guide](../README.md) · [Core](core/README.md) ·
[Drivers](drivers/README.md) · [Strategies](strategies/README.md)

`lib.rs` is the public facade and defines shared `Config`. An application
connects a backend client, constructs its provider, selects a `DB`, and wraps
a strategy at its public contract boundary. Serialization and cross-cutting
policy remain outside backend command implementations.

```rust
use std::{sync::Arc, time::Duration};
use cache::{build_database, chain, typed, with_retry_middleware,
    core::{Capabilities, DatabaseSpec, MemoryDriver, Options}};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct User { name: String }

#[tokio::main]
async fn main() -> cache::Result<()> {
    let db = build_database(Arc::new(MemoryDriver::new()), Capabilities::new(),
        DatabaseSpec { namespace: "users".into(), ..Default::default() });
    let users = typed::<User>(chain(db.document.clone(), [
        with_retry_middleware(3, Duration::from_millis(25)),
    ]));
    let id = users.create(&User { name: "Ada".into() }, &Options::default()).await?;
    println!("{:?}", users.get(&id).await?);
    db.close().await
}
```

This process-local example needs no server; the minimal capability bundle
supports direct documents but not enumeration. Use a real provider for shared
storage. `typed.rs` encodes/decodes JSON while retaining the raw strategy's
IDs, keyspace, TTL, and errors.

`middleware.rs` defines `Middleware` and `chain`: the last supplied wrapper
is outermost. `retry.rs` repeats safe reads/deletion, not create/update or
settled NotFound. `logging.rs` uses the caller's tracing subscriber;
`telemetry.rs` accepts a caller-owned meter; `observed.rs` provides the shared
three-signal telemetry wrapper. Place observation outside retry to measure a
logical operation, or inside it to measure individual attempts.

`dial_timeout.rs` and `operation_timeout.rs` distinguish default, disabled,
and explicit transport deadlines; drivers translate that policy to their
client APIs. `error.rs` is the stable control-flow vocabulary. Close selected
DBs before root clients so admitted Aside refreshes drain before connections
are released.

```bash
cargo test -p tpp-cache --lib
cargo test -p tpp-cache --test observability
```
