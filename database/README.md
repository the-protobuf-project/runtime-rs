# Database

Descriptor-driven storage shaped after
[runtime-go/database](https://github.com/the-protobuf-project/runtime-go/tree/main/database),
so one store generator emits Go and Rust clients from the same template.
Backends: InfluxDB 3 (`influxdb3-client` 0.3.0, live-tested against Core
3.12.2) and Arrow IPC captures. Every Rust file stays under 200 lines; a test
enforces it.

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

```rust,ignore
impl Model for Reading {
    fn resource() -> &'static Resource { &READING }
    fn to_record(&self) -> Result<Record> { /* column values */ }
    fn from_record(record: Record) -> Result<Self> { /* back again */ }
}
let readings = For::<Reading>::new(&db);
readings.create(&reading).await?;
let (page, next_token) = readings.range(&RangeOptions { start, end, ..Default::default() }).await?;
```

## Sustained writes and recordings

Generated code calls `create` once per point. `database::buffered(db, config)`
queues each create into a bounded background batch writer and returns
`pending: true`, runtime-go's signal for an accepted but not yet durable write.
`database::mirror(primary, copy)` writes to both stores concurrently and fails a
write if either side fails; reads use the primary. `db.close()` drains every
queue and closes both stores.

```rust,ignore
let db = buffered(mirror(influx, arrow), IngestConfig { max_inflight: 8, ..Default::default() })?;
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
