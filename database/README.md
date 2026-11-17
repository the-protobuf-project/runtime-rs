# Database

Descriptor-driven storage shaped after
[runtime-go/database](https://github.com/the-protobuf-project/runtime-go/tree/main/database),
so one store generator emits Go and Rust clients from the same template.
Backends: InfluxDB 3 (`influxdb3-client` 0.3.0, live-tested against Core
3.12.2) and Arrow IPC captures for time series, and PostgreSQL, MySQL and SQLite
for proto records. Every Rust file stays under 200 lines; a test enforces it.
3.12.2) and Arrow IPC captures for time series, and MongoDB for proto records
(`database::records`). Every Rust file stays under 200 lines; a test enforces it.
3.12.2) and Arrow IPC captures for time series; Redis for proto records. Every Rust file stays under 200 lines; a test
enforces it.

The `influxdb` feature, on by default, is the InfluxDB 3 backend. `influxdb3-client`
0.3 requires Arrow 59 while the capture backend uses 60, so a capture-only build
sets `default-features = false` to link one Arrow major and no HTTP client.
The SQL backend is off by default: `orm-postgres`, `orm-mysql` and `orm-sqlite`
each link one sqlx driver, and `orm` turns on all three.

## Runnable create/read/close example

This complete local example uses Arrow files and needs no database server.
Add `tpp-database`, `serde` with `derive`, and `tokio` with
`macros`/`rt-multi-thread` to your application.

```rust
use database::{Provider, arrow::ArrowProvider, collection,
    store::{Column, Kind, ListOptions, Resource, Role}};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct Reading { device: String, value: f64, time: i64 }

#[tokio::main]
async fn main() -> database::Result<()> {
    let resource = Resource::new("readings", vec![
        Column::new("device", Kind::String, Role::Tag),
        Column::new("value", Kind::Float, Role::Field),
        Column::new("time", Kind::Timestamp, Role::Timestamp),
    ])?;
    let provider = ArrowProvider::new("captures");
    provider.create_database("demo").await?;
    let db = provider.set_database("demo").await?;
    let readings = collection::<Reading>(&db, resource);
    readings.create(&Reading { device: "ahu1".into(), value: 21.5, time: 1_000_000_000 }).await?;
    let (page, next, total) = readings.list(&ListOptions::default().page(10)).await?;
    println!("{page:?}; next={next}; total={total}");
    db.close().await
}
```

Records persist under `captures/demo/readings/`. Re-running appends another
capture; it does not reset previous rows. The InfluxDB provider can replace
Arrow selection while keeping the descriptor/collection calls the same;
see its runnable example for credentials and provisioning.

## The contract (`database::store`)

| runtime-go | Rust |
| --- | --- |
| `Provider.SetDatabase(ctx, name)`, `Backend()` | `Provider::set_database(name)`, `backend()` |
| `DB{Driver, Tx, Schema, Series, Backend, Name}`, `Close()` | `DB { driver, tx, schema, batch, series, streams, backend, name }`, `close()` |
| `store.Build(d, backend, name, release)` | `DB::build(backend, name, Capabilities { .. })` |
| `Driver`: Create/Get/Update/Delete/List/Count/Exists | `Driver`; `DB` derefs to it, so `db.create(..)` |
| `Batcher.CreateMany/GetMany` | `db.batch.create_many/get_many` |
| `Migrator.EnsureSchema/DropSchema/HasSchema` | `db.schema.ensure_schema/drop_schema/has_schema` |
| `TimeSeries.EnsureHypertable/Range/Aggregate` | `db.series.ensure_hypertable/range/aggregate`, same option and `Bucket` shapes |
| `ListOptions/ListResult/WriteResult{Pending}` | same, `WriteResult { record, pending, metadata }` |
| `For[T](d, res)` | `For::<T>::new(&db)` for a generated `Model` |
| `database.Collection[T]` | `database::collection::<T>(&db, resource)` for serde structs |
| `store.Instrument`, `NewRegistry` | `store::instrument`, `Registry::new` |
| `ErrNotFound/ErrAlreadyExists/ErrUnimplemented` | `Error::NotFound/AlreadyExists/Unimplemented { backend, capability }` |

Every capability is always present; one a backend lacks refuses by name.
Filters are the AIP-160 subset runtime-go accepts — conjunctions of
`column op value` with `= != > >= < <=`, OR refused — parsed once for every
backend. Times are epoch nanoseconds (RFC 3339 also accepted in filters).
Streams (`db.streams`) are a Rust extension for windows too large for pages.

## Generated models

The generator emits a static descriptor and conversions per message; see
[examples/influxdb/model.rs](examples/influxdb/model.rs):

```rust
use database::store::{DB, For, Model};

async fn save_generated<T: Model>(db: &DB, value: &T) -> database::Result<()> {
    let rows = For::<T>::new(db);
    rows.create(value).await?;
    Ok(())
}
```

## Sustained writes and recordings

Generated code calls `create` once per point. `database::buffered(db, config)`
queues each create into a bounded background batch writer and returns
`pending: true`, runtime-go's signal for an accepted but not yet durable write.
`database::mirror(primary, copy)` writes to both stores concurrently and fails a
write if either side fails; reads use the primary. `db.close()` drains every
queue and closes both stores.

```rust
use database::{DB, IngestConfig, buffered, mirror};

fn capture_writes(influx: DB, arrow: DB) -> database::Result<DB> {
    buffered(mirror(influx, arrow), IngestConfig { max_inflight: 8, ..Default::default() })
}
```

Each durable InfluxDB write waits for the server's WAL flush (~1 s), so
throughput scales with batches in flight. Measured against Core 3.12.2 with
5,000-point batches: 1 in flight ~7k/s, 4 ~20k/s, 8 ~40k/s. A 60 s run of
the capture example at 22,000 points/s stored all 1,320,220 points in both
InfluxDB and Arrow. Queued points are in memory: call `close` before exit.

## InfluxDB (`database::influxdb`)

A point is a record without a key: table + complete tag set + timestamp.
Rewriting an identity merges fields. `create`/`create_many` write points,
`list`/`count`/`range`/`aggregate` read them, and get/update/delete/exists and
transactions are refused. Writes pin nanosecond precision, synchronized WAL,
no partial-line acceptance, and at most 10,000 points per request; requests
are not atomic together. `range` pages use keyset tokens bound to the host,
database, descriptor and query, with `total: -1` (a count is a second scan).

`InfluxAdmin` creates databases with `Retention` (fixed at creation on Core)
and drops them. `schema.drop_schema` removes a table and all its points. Core
has no selective row deletion; this crate never sends Enterprise's
`data_only` flag, which Core ignores and drops the table.

## Arrow captures (`database::arrow`)

Append-only IPC stream segments under `<root>/<database>/<table>/`, rotated by
size or age (default 1 GiB / 15 min), each published only once its header is
written and given a `.bounds` sidecar on close so range reads skip it. A crash
loses at most the batch being written; `load` returns the rest. Pages use
offset tokens with totals. Open the files with pyarrow, polars or DuckDB.

## Records over SQL (`database::records`, `database::orm`)

`records` is runtime-go's `database/store` for proto messages: a `Resource`
descriptor (columns, SQL types, key, unique, managed key and audit times), a
`Driver` over `DynamicMessage`, and `Transactional`/`Migrator`/`Batcher`/`Watcher`
capabilities that refuse by name where a backend lacks them. `orm` is its SQL
backend, the port of runtime-go's `database/orm`: one dynamic driver for every
resource, built on sqlx pools and sea-query statements rather than an ORM.

```rust
use database::orm::{OrmProvider, sqlx::PgPool};
use database::records::{DB, Provider, Resource, TxWork};
use futures::{FutureExt, future::BoxFuture};
use prost_reflect::DynamicMessage;

// Names the closure's lifetime as `TxWork` wants it.
fn work<F>(f: F) -> TxWork
where F: for<'a> FnOnce(&'a DB) -> BoxFuture<'a, database::Result<()>> + Send + 'static {
    Box::new(f)
}

async fn store(pool: PgPool, books: Resource, book: DynamicMessage) -> database::Result<()> {
    let tenant = OrmProvider::new(pool).set_database("tenant_a").await?;
    tenant.schema.ensure_schema(&books).await?; // CREATE TABLE IF NOT EXISTS
    tenant.tx.run(work(move |tx| async move {
        tx.create(&books, &book).await?;
        Ok(())
    }.boxed())).await
}
```

| runtime-go `orm` | Rust `orm` |
| --- | --- |
| `NewProvider(gormDB)`, `SetDatabase`, `Backend()` = `gorm` | `OrmProvider::new(pool)`, `set_database`, `backend()` = `orm` |
| `New(gormDB)` | `orm::driver(pool)`: no selection, no ping |
| `Driver.Run` (GORM savepoints when nested) | `db.tx.run`: savepoints when nested |
| `EnsureSchema/DropSchema/HasSchema` | `db.schema.*`, same DDL |

- The pool is yours (`database::orm::sqlx` is the version to open it with);
  `close` is a no-op. The selected name is checked by `check_database_name`,
  pinged, and overrides `Resource::schema`: a Postgres schema, a MySQL database,
  an attached SQLite database. It must exist; `ensure_schema` creates tables only.
- Identifiers are quoted per dialect (backticks on MySQL). Columns without an
  `sql_type` get the dialect's type for their kind (e.g. `BYTEA`/`LONGBLOB`/`BLOB`).
- Unique and primary-key violations are `Error::AlreadyExists`, on create and
  on update; a missing row on get, update or delete is `Error::NotFound`.
- Filters are `records::core::parse_filter`'s subset, each column checked
  against the descriptor and its value typed by kind. `order_by` takes AIP-132
  `field [desc], …`; an unknown field orders by the key, as Go does, and the
  key always ends the order so offset pages are stable. `omit_total` skips the
  count and reads one row past the page.
- Typed parameters (sqlx's, unlike Go's) mean a record key is typed by the key
  column's kind, and an unsigned value past 2^63 is refused outside MySQL.
- On SQLite an in-memory database is per connection: use a one-connection pool.
- SQLite is bundled at 3.53 (`libsqlite3-sys` 0.37, the newest sqlx 0.9 allows), which
  carries CVE-2026-119375 (RUSTSEC-2026-0459): opening a database file with a crafted
  `-wal` file can return stale process memory. Open only database files the service
  itself wrote. The advisory is accepted in `.cargo/audit.toml` until sqlx allows
  `libsqlite3-sys` 0.39.

`docker/compose.orm.live.yaml` runs Postgres (`127.0.0.1:15432`) and MySQL
(`127.0.0.1:13306`) for the `orm_live` test; SQLite tests run in-process.
## A read-through cache (`database::cached`, feature `cached`)

The port of runtime-go's `database/cached`: a decorator, not a backend.
`cached::wrap(&db, cache)` returns a records `DB` whose `get` is served
cache-aside through [tpp-cache](../cache/README.md)'s Aside strategy, and
whose writes go to the store first and invalidate after.

```rust
use std::{sync::Arc, time::Duration};
use cache::{Config, Provider, core::Options, drivers::redis::RedisProvider};
use database::{cached, records::DB};

async fn cache_reads(db: &DB, redis: RedisProvider) -> database::Result<DB> {
    let cdb = redis.set_database("records").await.expect("cache database");
    let ttl = Options::default().with_ttl(Duration::from_secs(60));
    Ok(cached::wrap(db, cached::from_aside(Arc::new(cdb), ttl)))
}
```

| Operation | Through the cache |
| --- | --- |
| `get` | cached per record, keyed by its primary key; concurrent misses collapse into one load; `NotFound` is remembered (the cache's 30 s negative lease) |
| `create`, `update`, `delete` | written through, then the key is invalidated, which also clears a remembered absence; an invalidation failure fails nothing |
| `list`, `count`, `exists` | passed through, never cached: they depend on the whole set, or are cheaper than a cached record |
| `tx.run` | reads inside go to the store; written keys are invalidated after a commit, left alone on a rollback |
| `schema`, `watch` | carried across unchanged |
| `batch` | kept where the backend has it: `create_many` invalidates like `create`, `get_many` passes through. Go's `Wrap` hides `Batcher`/`Watcher` instead |

Records travel as proto wire bytes in a base64 JSON string, Go's encoding of a
`[]byte`, decoded against the resource's descriptor. Keys are the record key
alone, as in Go, so resources sharing one cache database must not share keys
(AIP names never do). A cache refusing new loads (`cached::Overloaded`) is
read through to the store rather than failed; any other store error comes back
as itself. Always set a TTL: it bounds what invalidation misses, such as a
failed invalidation or a writer that bypasses the decorator.

`cached::Cache` is the narrow contract; `from_aside` adapts tpp-cache to it, and
any cache that collapses loads and remembers absences can stand in. The live
suite needs Redis; see [tests](tests/README.md).
## MongoDB (`database::mongodb`)

The port of runtime-go's `database/mongodb`, behind the `mongodb` feature (the
official `mongodb` 3.x driver, bson 3, rustls over ring). It implements the
`database::records` contract: a resource is a collection, a record a document,
and each column a field, so the data reads naturally in any MongoDB tool.

```rust
use database::mongodb::{MongoClient, MongoConfig, MongoProvider};
use database::records::{Provider, Resource};
use prost_reflect::DynamicMessage;

async fn save(books: &Resource, book: &DynamicMessage) -> database::Result<()> {
    let client = MongoClient::connect(MongoConfig::address("localhost:27017")).await?;
    let db = MongoProvider::new(client.clone()).set_database("tenant_a").await?;
    db.schema.ensure_schema(books).await?; // the collection and its unique indexes
    db.create(books, book).await?;
    client.close().await; // the client is yours; nothing here closes it
    Ok(())
}
```

| runtime-go | Rust |
| --- | --- |
| `Config{URI, Address, Username, Password, AuthSource, ConnectTimeout, MaxPoolSize, MinPoolSize, MaxConnIdleTime}` | `MongoConfig { uri, address, username, password, auth_source, connect_timeout, max_pool_size, min_pool_size, max_conn_idle_time }` |
| `NewClient` (pings), `Wrap`, `Unwrap`, `Close` | `MongoClient::connect`, `wrap`, `unwrap`, `close` |
| `NewProvider`, `SetDatabase`, `DropDatabase` | `MongoProvider::new`, `set_database`, `drop_database` |
| `New(client)` | `MongoDriver::new(&client)`: each resource in its `schema` database |
| `Run` with a session-bound `*store.DB` | `db.tx.run(work)`: CRUD and bulk calls in `work` join the session |
| `EnsureSchema` / `DropSchema` / `HasSchema` | `db.schema.*`: the collection plus a `uniq_<column>` index per unique column |
| `CreateMany` / `GetMany` | `db.batch.*`: one ordered insert; one `$in` per 512 keys, `None` for missing |
| `Watch` | `db.watch.watch(res, opts)`: a change stream, Extended JSON resume tokens |

The selected name is a real MongoDB database, so tenants are isolated by the
server; it is checked with `check_database_name` and the server pinged before a
`DB` is returned. The encoding is Go's: the key is `_id`, integers int64, enums
int32, times BSON dates (milliseconds, UTC), bytes generic binary (empty as
null), an unset optional field null, and a `uint64` past `i64::MAX` a
Decimal128 so it reads back exactly. Code 11000 is `AlreadyExists`; a missing
document `NotFound`. Filters are the shared AIP-160 subset with values typed by
column kind; `order_by` sorts by one column (the key as `_id`, ties by `_id`);
pages use the shared offset tokens, and `omit_total` reads one past the page.

Where it differs from Go: a column named twice in a filter keeps both bounds
(Go's map kept the last); a transaction's work is `FnOnce`, so a commit with an
unknown outcome is retried but the work is never rerun, and nesting is refused;
bulk calls inside a transaction join it. Transactions and change streams need a
replica set; [the live compose file](docker/compose.mongodb.live.yaml) runs a
single-node one on `127.0.0.1:27117`.
## Redis records (`database::redis`, feature `redis`)

The [`records`](src/records/mod.rs) contract over Redis, ported from
runtime-go's `database/redis`. Each record is its proto wire bytes, so bytes
and `uint64` past 2^53 survive and a Go program reads what Rust wrote. The key
layout is Go's, byte for byte, under an optional prefix and the selected
database (a key segment, not a numbered Redis database):

```text
{prefix}:{database}:{table}:rec:{key}          one record
{prefix}:{database}:{table}:ids.z              sorted set of keys: list, count
{prefix}:{database}:{table}:uniq:{col}:{value} a unique value's reservation
```

```rust,ignore
let client = Arc::new(cache::drivers::redis::RedisClient::connect(config).await?);
let records = database::redis::RedisProvider::new(client.connection_manager().await?)
    .with_prefix("app");
let db = records.set_database("tenant_a").await?;
db.create(&books, &book).await?;
```

The provider takes a `redis::aio::ConnectionManager` it never closes; the
cache's `RedisClient::connection_manager` (or Dragonfly's) lends its own, so
cache and records share one multiplexed connection, as Go's layers share one
client. Create, update and delete are each one Lua script over the record, its
index entry and its unique reservations, so `AlreadyExists` is decided by the
server and a refused or interrupted write leaves nothing behind (Go claims keys
one `SET NX` at a time and rolls back by hand). `list` pages by key in the
server, either direction, total and page in one pipeline, or one key past the
page with `omit_total`; a filter is refused by name, as Go refuses it.
`get_many` is one round trip of `MGET`s; `create_many` stops at the first
failure. Schema always exists, `drop_schema` walks the resource's keys, and
transactions are refused. Live tests: [docker](docker/README.md).

## Examples and telemetry

- [examples/influxdb](examples/influxdb/README.md): the full InfluxDB workflow
  as generated client code, in a disposable database.
- [examples/capture](examples/capture/README.md): 22k/s for 15 minutes into
  InfluxDB with every point recorded to Arrow, verified by counting both.

`with_telemetry(signals)` on providers and `store::instrument(db, signals)`
report every capability without names, keys, filters or raw errors; no-op by
default. See [runtime telemetry](../telemetry/README.md).

- https://github.com/InfluxCommunity/influxdb3-rust
- https://docs.influxdata.com/influxdb3/core/reference/internals/data-retention/

## Implementation and verification

See [source composition](src/README.md), [store contracts](src/store/README.md),
[InfluxDB](src/influxdb/README.md), [Arrow](src/arrow/README.md),
[MongoDB](src/mongodb/mod.rs),
[ingestion](src/ingest/README.md), [observations](src/observe/README.md),
and [tests](tests/README.md).

```bash
cargo test -p tpp-database
cargo test -p tpp-database --features orm   # SQLite in-process
cargo doc -p tpp-database --no-deps --all-features
cargo doc -p tpp-database --no-deps
# MongoDB, against the live replica set (see tests/README.md):
cargo test -p tpp-database --all-features
```
