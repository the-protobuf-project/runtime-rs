# InfluxDB end-to-end examples

All InfluxDB example code is in this folder. One command runs the complete
workflow against the local InfluxDB 3 Core service. Every Rust file stays below
200 lines, including documentation.

From the repository root:

```sh
docker compose -f database/docker/compose.yaml up -d --wait
set -a
source database/docker/.env
set +a
cargo run -p tpp-database --example influxdb
```

The ignored .env supplies INFLUX_HOST, INFLUX_TOKEN and the SDK's required
INFLUX_DATABASE setting. The runner creates its own uniquely named database
from Rust; it does not modify the database named in your environment. The token
needs administrative permission. No database-creation CLI command is required.

| Order | File | Demonstrates |
| --- | --- | --- |
| Runner | [main.rs](main.rs) | Runs the workflow and cleans up its database |
| Shared | [model.rs](model.rs) | Typed model and backend-neutral descriptor |
| 1 | [provision.rs](provision.rs) | Create/select a database with one-hour retention |
| 2 | [records.rs](records.rs) | Write, rewrite and read typed observations |
| 3–4 | [queries.rs](queries.rs) | Filtering, ordering and time-bucket aggregation |
| 5–6 | [ingest.rs](ingest.rs) | Background batching, flush/close, pages and streams |
| 7 | [retention.rs](retention.rs) | Verify expiry of old observations |
| 8 | [cleanup.rs](cleanup.rs) | Drop the table; main then drops the database |

The runner removes only the database it successfully created. It also attempts
that cleanup if a later stage fails. The Docker service remains running.

For application startup, copy provision.rs and choose your database name. An
existing name returns Error::AlreadyExists; handle that explicitly if startup
should reuse it. Existing Core retention cannot be changed. Use
Retention::Infinite instead of Retention::For(...) for no expiry.

Point rewrites use table + tag set + timestamp identity, not record CRUD.
Table deletion removes schema and all points. Selective historical-row deletion
is unavailable on Core and is never emulated by dropping a whole table.

## Metrics, logs and traces

All telemetry example code stays here in [observability.rs](observability.rs).
It adapts the linked telemetry-rs SDK to runtime's shared Telemetry bindings.
Without the feature, the workflow uses no-op bindings and starts no exporters.

```sh
docker compose -f database/docker/compose.yaml --profile telemetry up -d
set -a
source database/docker/.env
set +a
cargo run -p tpp-database --example influxdb --features telemetry-sdk
docker compose -f database/docker/compose.yaml logs otel-collector
```

The collector receives metrics, structured logs and spans on loopback port
16009 (configurable with OTEL_COLLECTOR_PORT). Signal setup is application-owned;
exporter flush/shutdown runs after all workers drain and operations finish.
Provider/admin operations, writes/reads/aggregation, pages, complete stream
lifetimes and ingestion admission/batches/flush/close share correlated signals.
Record bodies, queries, keys, page tokens and database names are not exported.
The collector logs are a development inspection sink; InfluxDB stays running.

## Reusing typed access

Generated-style model binding is independent of the provider:

```rust
use database::{IngestConfig, store::{DB, For, Model, RangeOptions}};

async fn ingest_then_read<T: Model>(db: &DB, values: &[T], start: i64, end: i64)
    -> database::Result<Vec<T>>
{
    let rows = For::<T>::new(db);
    let writer = rows.batch_writer(IngestConfig::default())?;
    for value in values { writer.write(value).await?; }
    writer.flush().await?;
    writer.close().await?;
    let (page, _next_token) = rows.range(&RangeOptions { start, end, ..Default::default() }).await?;
    Ok(page)
}
```

This helper returns one page; continue with the returned token for a complete
large window, or use the model's stream API. Provision/select the DB first
with the pattern in `provision.rs`. The descriptor/model implementation in
`model.rs` is the full concrete example behind the generic helper above.
