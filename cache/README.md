# tpp-cache

Backend-independent cache strategies over Redis, Dragonfly, and Memcached. A
Rust port of [`runtime-go/cache`](https://github.com/the-protobuf-project/runtime-go/tree/main/cache),
built around async traits, explicit capabilities, and caller-owned clients.

## Install

```bash
cargo add tpp-cache
```

The library is imported as `cache`.

## Copyable Redis quick start

Start the Redis service shown below first. Add `tpp-cache`, `serde` with
`derive`, and `tokio` with `macros`/`rt-multi-thread` to your application.

```rust
use std::{sync::Arc, time::Duration};
use cache::{Config, Provider, core::Options, typed,
    drivers::redis::{RedisClient, RedisConfig, RedisProvider}};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct User { name: String }

#[tokio::main]
async fn main() -> cache::Result<()> {
    let client = Arc::new(RedisClient::connect(RedisConfig {
        address: "127.0.0.1:16379".into(), ..Default::default()
    }).await?);
    let provider = RedisProvider::new(client.clone(), Config {
        prefix: "demo".into(), default_ttl: Duration::from_secs(60), ..Default::default()
    });
    let db = provider.set_database("users").await?;
    let users = typed::<User>(db.document.clone());
    let id = users.create(&User { name: "Ada".into() }, &Options::default()).await?;
    println!("{:?}", users.get(&id).await?);
    users.delete(&id).await?;
    db.close().await?;
    client.close().await;
    Ok(())
}
```

The provider selects a namespace; typed storage serializes JSON around the raw
Document strategy. The selected DB drains its refreshes before the root client
closes. Handle `CacheError::NotFound` for ordinary misses and `Unsupported` when
an optional backend capability is required.

See [source and middleware](src/README.md), [core](src/core/README.md),
[drivers](src/drivers/README.md), [strategies](src/strategies/README.md),
[examples](examples/README.md), [Docker](docker/README.md), and [tests](tests/README.md).

## How it fits together

Applications follow three explicit steps:

1. connect a Redis, Dragonfly, or Memcached client;
2. bind shared cache policy to its Provider;
3. select a named or numeric database and use one of its strategies.

The four strategies have deliberately different costs:

- **Document** stores ID-addressed values and supports enumeration when the
  backend has server-side sets.
- **Volatile** stores caller-keyed values without maintaining an index.
- **Indexed** adds secondary field/value memberships to Document.
- **Aside** loads through an authoritative source, collapses concurrent misses,
  remembers absence, and can serve stale values while refreshing.

Typed JSON views and provider-independent retry, tracing, and OpenTelemetry
middleware wrap Document without changing backend strategy code. The complete,
compile-checked quick start and policy guide live in the crate documentation:

```bash
cargo doc --open -p tpp-cache
```

## Backend capabilities

Redis and Dragonfly provide direct operations, sets, lease reporting, scans,
bulk reads, and fenced Aside claims. Memcached provides direct operations and
bulk reads; enumeration, secondary lookup, remaining-TTL reporting, scans, and
cross-process fencing return `CacheError::Unsupported` where required.

## Runnable Redis example

Start the repository's isolated Redis service, run the public-API walkthrough,
then remove only that Compose project's resources:

```bash
docker compose -p runtime-cache-live -f cache/docker/compose.live.yaml up -d --wait redis
cargo run -p tpp-cache --example redis
docker compose -p runtime-cache-live -f cache/docker/compose.live.yaml down
```

The example defaults to `127.0.0.1:16379`. Set
`RUNTIME_CACHE_REDIS_ADDRESS` to use another Redis endpoint.

## Runnable Dragonfly example

The shorter Dragonfly walkthrough demonstrates that Indexed and Aside use the
same RESP capability implementation under a type-safe Dragonfly preset:

```bash
docker compose -p runtime-cache-dragonfly-live -f cache/docker/compose.dragonfly.live.yaml up -d --wait dragonfly
cargo run -p tpp-cache --example dragonfly
docker compose -p runtime-cache-dragonfly-live -f cache/docker/compose.dragonfly.live.yaml down
```

It defaults to `127.0.0.1:16380`; override this with
`RUNTIME_CACHE_DRAGONFLY_ADDRESS`.

## Live contract tests

The repository includes separate Compose services and feature-gated public API
tests for each backend:

```bash
docker compose -p runtime-cache-live -f cache/docker/compose.live.yaml up -d --wait redis
cargo test -p tpp-cache --features live-tests --test redis_live

docker compose -p runtime-cache-dragonfly-live -f cache/docker/compose.dragonfly.live.yaml up -d --wait dragonfly
cargo test -p tpp-cache --features live-tests --test dragonfly_live

docker compose -p runtime-cache-memcached-live -f cache/docker/compose.memcached.live.yaml up -d --wait memcached
cargo test -p tpp-cache --features live-tests --test memcached_live
```

## License

Apache-2.0

## Shared runtime observability

`cache::with_observability(document, telemetry::Telemetry)` and its middleware
form add the shared runtime metrics, correlated logs and cancellation-safe
traces. Supply the same handles used by database operations. Existing logging
and metrics APIs remain compatible. See [runtime telemetry](../telemetry/README.md)
for provider ownership and the bounded-label policy. Keys and values are never
exported by the shared wrapper; no-op bindings return the original document.
