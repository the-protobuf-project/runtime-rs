# tpp-cache

One cache contract over many providers. A program picks a provider, selects a
database from it, and caches through one `DB`, whichever provider is behind it.
This is the Rust port of
[runtime-go/cache](https://github.com/the-protobuf-project/runtime-go/tree/main/cache),
and its key layout and value encoding are Go's, so a Go and a Rust service share
cache entries.

The library is imported as `cache`.

## Contents

- [Choosing a provider](#choosing-a-provider)
- [Installing](#installing)
- [The shape every provider shares](#the-shape-every-provider-shares)
- [Getting a DB from each provider](#getting-a-db-from-each-provider)
- [The four strategies](#the-four-strategies)
- [Sequencing: what happens in what order](#sequencing-what-happens-in-what-order)
- [Validation and policy](#validation-and-policy)
- [Outcomes and errors](#outcomes-and-errors)
- [Edge cases, provider by provider](#edge-cases-provider-by-provider)
- [Middleware and telemetry](#middleware-and-telemetry)
- [Testing](#testing)

## Choosing a provider

| Provider | Module | Pick it when |
| --- | --- | --- |
| Redis | `drivers::redis` | The general choice: every strategy and capability |
| Dragonfly | `drivers::dragonfly` | You run Dragonfly: the same RESP implementation as Redis, as a typed preset |
| memcached | `drivers::memcached` | You already run memcached and only need direct values and bulk reads |
| Workers KV | `drivers::workerskv` | You serve from Cloudflare and want a value read everywhere and written rarely |

Every strategy is on every `DB`. What a provider cannot back returns
`CacheError::Unsupported` when you call it:

| | Redis | Dragonfly | memcached | Workers KV |
| --- | --- | --- | --- | --- |
| Direct values (`volatile` get, set, delete) | yes | yes | yes | yes |
| Lease extension (`volatile.touch`) | yes | yes | yes | Unsupported |
| Conditional writes (`document.update`) | yes | yes | yes | Unsupported |
| Bulk reads | yes | yes | yes | no |
| Remaining TTL (`ttl`) | yes | yes | Unsupported | yes |
| Enumeration (`document.keys`, `document.list`) | yes | yes | Unsupported | Unsupported |
| Secondary indexes (`indexed`) | yes | yes | Unsupported | Unsupported |
| Scans (`volatile.scan`), `drop_database` | yes | yes | Unsupported | yes, by prefix |
| Read-through (`aside`), fenced across processes | yes | yes | in-process only | in-process only |
| Numeric databases (`select_index`) | native | native | a key namespace | a key namespace |

Workers KV is eventually consistent: a write can take up to a minute to be seen at
other locations, a key takes at most one write a second, and a lease under sixty
seconds is lengthened to sixty, the shortest KV keeps anything. It is reached through
Cloudflare's official `cloudflare` crate.

For what Cloudflare's CDN has cached, rather than what this crate stores,
`cdn::Purger` purges a zone by URL, cache tag, host or prefix.

## Installing

```bash
cargo add tpp-cache
```

All four providers are always built; they share one `redis` 1.x crate (Redis and
Dragonfly), and add a memcached client and Cloudflare's `cloudflare` SDK (Workers KV).

## The shape every provider shares

Every provider works in the same five steps:

1. **Own a client.** `connect` verifies the server before returning. The client is
   yours: the provider borrows it, and one client can also serve the database's
   Redis records through `connection_manager()`.
2. **Make a provider** from the client and a `Config`, the policy every selected
   database shares: key prefix, default TTL, whether a TTL is required, default
   staleness, an optional allowlist of database names, and the fan-out limit.
3. **Select a database:** `set_database("name")` for a key namespace, or
   `select_index(n)` for a numbered one.
4. **Use the `DB`.** Its strategies are fields: `db.document`, `db.volatile`,
   `db.indexed`, plus `db.aside(loader)` for read-through.
5. **Close the `DB`, then the client.** Closing the `DB` drains its background
   refreshes first, so a refresh never runs on a closed client.

Everything after selection is the same code for every provider:

```rust
use std::time::Duration;
use cache::{CacheError, DB, core::Options};

async fn use_any_provider(db: &DB) -> cache::Result<()> {
    let ttl = Options { ttl: Some(Duration::from_secs(60)), ..Options::default() };

    // Volatile: your key, no index.
    db.volatile.set("session:alice", b"online", &ttl).await?;
    let mut out = Vec::new();
    db.volatile.get("session:alice", &mut out).await?;

    // Document: the cache picks the ID (or Options::id does).
    let id = db.document.create(b"{\"name\":\"Ada\"}", &ttl).await?;
    db.document.get(&id, &mut out).await?;
    db.document.update(&id, b"{\"name\":\"Ada L.\"}", &ttl).await?;

    // A miss is NotFound, an ordinary outcome rather than a failure.
    match db.document.get("missing", &mut out).await {
        Err(CacheError::NotFound) => {}
        other => other?,
    }
    db.document.delete(&id).await
}
```

A `Typed` view stores serde types as JSON around the same document strategy:

```rust,ignore
let users = cache::typed::<User>(db.document.clone());
let id = users.create(&User { name: "Ada".into() }, &Options::default()).await?;
let ada: User = users.get(&id).await?;
```

## Getting a DB from each provider

Each block ends with a `DB`; from there, the code above applies.

### Redis

```rust,no_run
use std::{sync::Arc, time::Duration};
use cache::{Config, Provider, drivers::redis::{RedisClient, RedisConfig, RedisProvider}};

#[tokio::main]
async fn main() -> cache::Result<()> {
    let client = Arc::new(RedisClient::connect(RedisConfig {
        address: "127.0.0.1:6379".into(), ..Default::default()
    }).await?);
    let provider = RedisProvider::new(client.clone(), Config {
        prefix: "app".into(), default_ttl: Duration::from_secs(60), ..Default::default()
    });
    let db = provider.set_database("users").await?;
    // ...
    db.close().await?;
    client.close().await;
    Ok(())
}
```

- **Named databases** share the client's connection and are isolated by key
  prefix.
- **`select_index(n)`** selects a native Redis database. It derives a connection
  that the `DB` owns and closes.
- **`connection_manager()`** lends the multiplexed connection to another layer,
  such as the database crate's Redis records, so both share one connection. The
  lent handle outlives `close`, which only stops the client admitting new commands.

### Dragonfly

```rust,ignore
use cache::drivers::dragonfly::{DragonflyClient, DragonflyConfig, DragonflyProvider};

let client = Arc::new(DragonflyClient::connect(DragonflyConfig {
    address: "127.0.0.1:6380".into(), ..Default::default()
}).await?);
let db = DragonflyProvider::new(client.clone(), config).set_database("users").await?;
```

Dragonfly speaks RESP, so it runs the same implementation as Redis behind its own
types. Everything in the Redis section applies, `connection_manager()` included.

### memcached

```rust,ignore
use cache::drivers::memcached::{MemcachedClient, MemcachedConfig, MemcachedProvider};

let client = Arc::new(MemcachedClient::connect(MemcachedConfig {
    servers: vec!["127.0.0.1:11211".into()], ..Default::default()
}).await?);
let db = MemcachedProvider::new(client.clone(), config).set_database("users").await?;
```

- **Protocol:** the meta text protocol, which needs memcached 1.6 or later.
- **Servers:** one pooled client per distinct server. Keys are placed with Go's
  CRC32-modulo scheme, so a Go and a Rust service pick the same server for a key.
  Listing a server more than once weights it.
- **Numeric databases** are key namespaces; memcached has none of its own.
- **`drop_database`** is refused rather than falling back to `flush_all`, which
  would erase every other application's keys.

## The four strategies

| Strategy | Keys | Use it for | Cost |
| --- | --- | --- | --- |
| `volatile` | Yours | Sessions, rate counters, flags: values you address by a key you already have | One command per call; nothing to keep in sync |
| `document` | The cache's, or `Options::id` | Values you create, then fetch by ID and enumerate | One extra set membership per write, for enumeration |
| `indexed` | As `document` | Documents you also look up by a field value (`by_index("email", ..)`) | One membership per index per write |
| `aside` | Yours | Read-through in front of a slower source: databases, APIs | Collapses concurrent misses into one load; remembers absence |

**Read-through with `aside`.** You hand it a loader for the authoritative source,
and it caches what the loader returns:

```rust,ignore
let users = db.aside(Arc::new(|id: String| Box::pin(async move { load_user_bytes(&id).await })));
let mut out = Vec::new();
users.get_or_load("users/ada", &mut out, &Options::default()).await?;
users.invalidate("users/ada").await?;   // after a write to the source
```

- **Concurrent misses** for one key make one load. The others wait for it, fenced
  across processes on Redis and Dragonfly.
- **Absence is remembered:** a loader `NotFound` is kept for 30 seconds, so a
  missing key does not hammer the source.
- **Stale reads:** with a staleness window (`Options::stale` or
  `Config::default_stale`), an expired value is served while one background
  refresh reloads it.
- **Loader errors** come back unchanged, as `CacheError::Loader`, so the caller can
  match its own error type.
- **The fan-out limit:** loads beyond `Config::concurrency` are refused with
  `Overloaded` rather than queued.

## Sequencing: what happens in what order

**Starting up**
1. `connect` dials and verifies the server. A wrong address or bad credentials
   fail here.
2. Building the provider does no I/O.
3. `set_database` validates the name ([Validation](#validation-and-policy)) before
   it touches the server, so a refused name changes nothing.

**Each write.** The value goes in a freshness envelope, Go's encoding, that carries
its staleness deadline. Its TTL is, in order of precedence:
1. `Options::ttl`;
2. `Config::default_ttl`;
3. none, unless `Config::require_ttl` is set, in which case the write is refused
   with `NoTTL`.

`Options::permanent` opts out of expiry explicitly.

**Document and indexed writes** update the value and its memberships. A reader
can briefly see a membership before its value, or the reverse, so a listed ID can
return `NotFound` if it expired or was deleted in between; treat that as absent.

**Shutting down.** Close each `DB`, which drains refreshes and releases any
connection it derived, then close the client. Closing the client first stops
in-flight refreshes mid-command.

## Validation and policy

| What | Rule |
| --- | --- |
| Database name | Not empty, and no `:` (colons separate the prefix, database and key). With `Config::databases` set, only a name on that list is selected |
| TTL | `Config::require_ttl` refuses writes with neither a TTL nor `permanent`, so nothing lingers forever by accident |
| IDs | `Options::id` names a document; otherwise the provider's ID generator does |
| Fan-out | `Config::concurrency` bounds concurrent loads and fallback fan-out; 0 means the default |

## Outcomes and errors

Callers match on `CacheError`, never on a provider's messages:

| Error | When |
| --- | --- |
| `NotFound` | A miss: no value, or it expired. An ordinary outcome |
| `Unsupported` | The provider cannot back the call ([the capability table](#choosing-a-provider)) |
| `NoTTL` | A write with no TTL while `require_ttl` is set |
| `Overloaded` | More concurrent loads than `concurrency` allows; read through to the source, or retry |
| `Loader(error)` | An `aside` loader failed; its own error, unchanged |
| `PartialDelete { deleted, source }` | `drop_database` failed partway, after deleting `deleted` keys |
| `Internal(reason)` | A refused name, a connection or protocol failure |

## Edge cases, provider by provider

**Redis and Dragonfly**
- A named database is a key prefix, so `drop_database` walks the keyspace with
  `SCAN` and deletes what it finds. This is not atomic: keys written during the
  walk may survive.
- On a Redis Cluster, multi-key commands need one hash slot. This crate targets
  standalone servers and a connection manager, never a cluster.
- Dragonfly sizes its threads to the machine. On a small machine, cap
  `--proactor_threads` and `--maxmemory`, as the repo's Compose file does.

**memcached**
- There is no enumeration, no indexes, no TTL reporting and no scan: the calls
  that need them return `Unsupported`.
- `aside` collapses concurrent misses within the process only, since memcached
  has nothing to fence across processes with.
- Values are capped by the server's item size, 1 MiB by default.

## Middleware and telemetry

Retries, logging, metrics and tracing wrap `document` without changing any provider:

- `with_retry`, `with_logging` and `with_telemetry`, or their `*_middleware`
  forms composed with `chain`.
- `cache::with_observability(document, telemetry)` adds the shared runtime
  metrics, correlated logs and cancellation-safe traces. It never exports keys or
  values, and with no-op handles it returns the document unchanged. See
  [runtime telemetry](../telemetry/README.md).

## Testing

```bash
cargo test -p tpp-cache                     # every strategy over the in-memory driver
```

Each provider has a live suite against its own Compose service on a high port, so
it can run beside a cache you already have:

| Provider | Start | Run |
| --- | --- | --- |
| Redis | `docker compose -p runtime-cache-live -f cache/docker/compose.live.yaml up -d --wait` (16379) | `cargo test -p tpp-cache --features live-tests --test redis_live` |
| Dragonfly | `docker compose -p runtime-cache-dragonfly-live -f cache/docker/compose.dragonfly.live.yaml up -d --wait` (16380) | `cargo test -p tpp-cache --features live-tests --test dragonfly_live` |
| memcached | `docker compose -p runtime-cache-memcached-live -f cache/docker/compose.memcached.live.yaml up -d --wait` (11212) | `cargo test -p tpp-cache --features live-tests --test memcached_live` |

With `live-tests` on, an unreachable server fails the suite rather than skipping
it. CI runs all three. The [examples](examples/README.md) walk through Redis and
Dragonfly end to end (`cargo run -p tpp-cache --example redis`), and
`cargo bench -p tpp-cache --features live-tests --bench redis_paths` measures the
hot Redis paths.

More detail: [source and middleware](src/README.md), [core](src/core/README.md),
[drivers](src/drivers/README.md), [strategies](src/strategies/README.md),
[Docker](docker/README.md) and [tests](tests/README.md).

## License

Apache-2.0
