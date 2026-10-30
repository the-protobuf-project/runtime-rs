# Database

Descriptor-driven storage with the InfluxDB 3 Rust SDK (`influxdb3-client` 0.3.0).
Live-tested against InfluxDB 3 Core 3.12.2. Every Rust source file, example and
test stays under 200 lines including documentation; a test enforces the rule.

## Create a database from Rust

An administrative token is required. No database-creation CLI is needed:

```rust
use std::time::Duration;
use database::{Provider, Retention};
use database::drivers::influxdb::{ClientConfig, InfluxAdmin, InfluxProvider};

async fn example() -> Result<(), Box<dyn std::error::Error>> {
let config = ClientConfig::builder()
    .host("http://localhost:8181")
    .token(std::env::var("INFLUX_TOKEN")?)
    .database("sensors")
    .build()?;
let admin = InfluxAdmin::new(&config, Duration::from_secs(30))?;
admin.create_database("sensors", Retention::For(Duration::from_secs(86400))).await?;
let db = InfluxProvider::new(config).set_database("sensors").await?;
Ok(())
}
```

Use `Retention::Infinite` to omit expiry. Core retention is fixed at creation.
A finite period must be whole minutes and at least one hour. AlreadyExists is
returned for an existing database; creation never silently changes its policy.
The provisioning example is in `examples/influxdb/provision.rs`.

## Capabilities and ownership

| Entry point | Behavior |
| --- | --- |
| `db.series()` | Point writes, bounded time ranges, bucketed aggregation |
| `db.reader()` | Pull-based scans and keyset pages |
| `db.maintenance().drop_table(...)` | Removes the table schema AND all its data |
| `db.row_deletion()` | Unsupported on Core: no selective time-range deletion |
| `db.records()` | Unsupported: Influx points are not insert-only record CRUD |
| `InfluxAdmin` | Create databases with retention; explicitly drop databases |
| `Collection<T>` | Serde model views over the same descriptors and operations |
| `db.batch_writer(...)` | Bounded asynchronous ingestion; flush/close barriers |

Database selection is read-only. Config-based selection allocates a database-
bound SDK pool; cloning a DB shares it. `from_client(Arc<Client>)` accepts only
that client's existing database. Administration uses a separate HTTP pool for
endpoints absent from the SDK, respecting its token, headers, proxy and CA.
Dropping a DB does not explicitly shut down another owner's pools.

## Writes and background ingestion

Point identity is table + complete tag set + timestamp. Rewrites merge fields;
they do not provide insert-only uniqueness or rollback. Tags are nonempty
strings. All descriptor columns are required on writes. Observations use
explicit signed epoch nanoseconds; fields retain scalar integer precision.

Direct writes validate the complete batch, with at most 10,000 points per call.
The adapter pins nanosecond precision, synchronized WAL, no injected default
tags, and no partial-line acceptance. Multi-request batches are still not
atomic; a timeout or cancelled write can have committed on the server.

`Collection::batch_writer` returns a typed producer. `write` means queue
admission only. `flush` is a barrier covering records admitted before it;
`close` ends admission and drains remaining records. Flush/close return terminal
backend failures. The worker stops on failure rather than silently dropping
errors or retrying a whole partially committed batch beyond SDK policy.

IngestConfig bounds the queue, points per batch and bytes per record, and sets
flush interval and per-write timeout. Full queues apply backpressure. Cancelled
sends aren't admitted. Cancelling a close waiter doesn't abort the worker.
Dropping a writer starts draining while Tokio stays alive; explicit close is
needed to wait and observe errors. The queue is not a crash-durable spool.

## Ranges, pages and streams

Ranges use `[start, end)`, typed ANDed comparisons, and descriptor-checked
identifiers. Values are SQL parameters. Ordering is by time and the complete
declared tag set. A range/page holds at most 10,000 records (default 100).

`Collection::page(&range, token)` uses keyset continuation without OFFSET.
Equal timestamps advance through tag identity. Tokens bind the host, database,
descriptor and query; changed bounds, filters, order or page size are rejected.
The descriptor must include every tag in the actual table. Tokens are opaque,
not encrypted or authorization. Concurrent inserts/updates are not a snapshot;
points inserted before a cursor position aren't replayed by later pages.

`Collection::stream(&Scan::new(start, end))` scans the time window without a
row cap by default. Scan.limit optionally bounds the total rows. Polling pulls
SDK Arrow Flight batches; the adapter holds only one decoded server batch.
Drop cancels transport. The complete stream deadline includes consumer time;
slow consumers should configure the provider/driver timeout appropriately.
Broken streams return an error without retrying/replaying delivered rows.
Server query-window, retention and query-memory limits still apply.

Aggregations run on the server in fixed whole-second buckets aligned to the
Unix epoch. They return named reductions with group tags, with no gap filling.

## Deletion limits

Core supports whole-table/database deletion and automatic retention expiry.
Table deletion removes schema and all tag sets; it is never substituted for a
requested time range. Physical reclamation follows server soft-delete/grace
rules; retention filters expired points at query time before reclamation.

Selective row deletion and mutable retention require Enterprise features not
implemented by this Core adapter. Core 3.12.2 silently ignores the documented
Enterprise `data_only` flag: sending it drops the table. This crate NEVER sends
that flag or claims to clear data while preserving schema.

## InfluxDB examples

All examples live together in [examples/influxdb](examples/influxdb/README.md).
One runnable workflow creates its own database from Rust, exercises writes,
rewrites, filtering, aggregation, background ingestion, pagination, streaming
and retention, then deletes its table and database. Docker stays running.

```sh
docker compose -f database/docker/compose.yaml up -d --wait
set -a
source database/docker/.env
set +a
cargo run -p tpp-database --example influxdb
```

For a fresh server, obtain an administrative token through its onboarding flow
and store it locally as INFLUX_TOKEN. Never commit it. See the examples folder's
README for the ordered file guide and environment setup.

## Verification and sources

Unit and HTTP/Flight fixture tests cover input/cursor validation, equal-time
ordering, SDK transport, batch backpressure, timer/manual/close flushing,
terminal errors, cancelled sends/close, and read/write timeouts. Live tests
verify retention, rewrites, reads, aggregates, ingestion, pages in both time
directions, streamed decoding, and explicitly whole-table/database deletion.

- https://github.com/InfluxCommunity/influxdb3-rust
- https://docs.influxdata.com/influxdb3/core/reference/internals/data-retention/
- https://docs.influxdata.com/influxdb3/enterprise/admin/tables/delete/
- https://docs.influxdata.com/influxdb3/enterprise/admin/delete-data/
