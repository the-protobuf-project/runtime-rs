# Redis, Dragonfly, and Memcached adapters

[Cache guide](../../README.md) · [Local services](../../docker/README.md)

A driver translates storage primitives and capabilities to backend commands.
Key qualification, freshness envelopes, indexing, and loaders live in core and
strategies. The provider binds a connected client to shared cache policy.

```rust
use std::{sync::Arc, time::Duration};
use cache::{Config, Provider, core::Options,
    drivers::redis::{RedisClient, RedisConfig, RedisProvider}};

#[tokio::main]
async fn main() -> cache::Result<()> {
    let client = Arc::new(RedisClient::connect(RedisConfig {
        address: "127.0.0.1:16379".into(), ..Default::default()
    }).await?);
    let provider = RedisProvider::new(client.clone(), Config {
        prefix: "example".into(), default_ttl: Duration::from_secs(60), ..Default::default()
    });
    let db = provider.set_database("sessions").await?;
    db.volatile.set("alice", b"online", &Options::default()).await?;
    let mut value = Vec::new();
    db.volatile.get("alice", &mut value).await?;
    assert_eq!(value, b"online");
    db.close().await?;
    client.close().await;
    Ok(())
}
```

Start Redis first using the commands in the Docker guide. Connecting verifies
the server; constructing a provider alone does not imply an active selected DB.

`redis.rs` implements standalone RESP transport, connection manager, native
index selection, and command capability mappings. Named databases share the
root manager and isolate keys; selecting a different native index derives a
connection owned by that DB. `dragonfly.rs` supplies typed presets over this
shared implementation, rather than duplicating Redis command logic.

`memcached.rs` uses the meta text protocol (Memcached 1.6+), one pooled client
per distinct server, and Go-compatible CRC32/modulo placement including repeated
server weights. Numeric databases are key namespaces. It supports direct
operations and bulk reads; enumeration/index sets, scans, leases, and fencing
are unsupported. `memcached_compatibility.rs` guards dependency API assumptions.

All clients remain caller-owned. Close each selected DB, then close the root
client. Timeout intent and error translation belong here; application retry
policy belongs outside the driver. See the crate guide's capability comparison.

```bash
cargo test -p tpp-cache --lib drivers
cargo test -p tpp-cache --features live-tests --test redis_live
```

The live target requires the selected server and fails when it is unavailable.
