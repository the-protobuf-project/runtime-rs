# Runtime telemetry

`tpp-telemetry` is the shared signal interface for runtime packages, analogous
to runtime-go/telemetry. It reuses OpenTelemetry 0.33 API handles. The separate
[telemetry-rs SDK](https://github.com/the-protobuf-project/telemetry/tree/main/telemetry-rs)
owns exporters, configuration, resources and shutdown. Runtime instrumentation
never initializes that SDK, replaces global providers or shuts them down.

## Bind the application's signals

```rust
use telemetry::Telemetry;
// meter, tracer and logger come from application-owned OTel providers.
let signals = Telemetry::default()
    .with_meter(meter)
    .with_tracer(tracer)
    .with_logger(logger);
```

Default bindings emit nothing. Signals are independent: meter-only/log-only
observers never modify or end an application's parent span. Each Observer
constructs its instruments once and shares immutable handles across calls.

With the optional `tpp-sdk` feature:

```rust
let owner = telemetry::sdk::Telemetry::new()
    .with_service("my-service", "1.0.0")
    .with_local_otel_collector()
    .with_tracing()
    .build()?;
let signals = telemetry::Telemetry::from_tpp(&owner, "runtime");
```

`from_tpp` uses the SDK's public meter/log handles and explicitly snapshots the
global tracer installed by that high-level SDK owner. For multiple tracer
providers, use `from_tpp_provider(&sdk_provider, scope)` instead: it resolves all
three handles from that explicit TelemetryProvider without consulting globals.
The integration is pinned to inspected SDK revision `5b18b2cb02bb`.

## Instrument packages

```rust
let admin = InfluxAdmin::new(&config, timeout)?.with_telemetry(signals.clone());
let provider = InfluxProvider::new(config).with_telemetry(signals.clone());
let db = provider.set_database("sensors").await?;
// Or instrument any already-selected backend:
let db = database::instrument(db, signals.clone());
let document = cache::with_observability(document, signals.clone());
```

Database wrappers preserve every optional capability. Re-instrumenting a DB
replaces bindings instead of stacking wrappers; no-op bindings restore the raw
capabilities. Cache's older metrics/logging APIs remain compatible; the new
with_observability wrapper uses this common three-signal policy. Network/gRPC
retain their existing tracing/propagation APIs; applications can share the
same provider infrastructure and use Observer for additional runtime components.

## Signal contract

| Instrument | Unit | Meaning |
| --- | --- | --- |
| runtime.operation.count | {operation} | Completed logical calls by outcome |
| runtime.operation.duration | s | Entire operation/stream duration |
| runtime.operation.active | {operation} | Balanced in-flight operations |
| runtime.records.read | {record} | Successfully returned/delivered records |
| runtime.records.written | {record} | Acknowledged records, never queue admission |
| runtime.ingestion.queue.depth | {record} | Admitted points waiting for dequeue |

Metric labels are fixed component/backend/operation/outcome classifications.
Queue depth uses only component/backend. No tenant/database/table names, keys,
SQL, predicates, page tokens, record values or raw errors are exported by the
runtime wrappers. Caller-provided custom attributes/providers remain the
application's responsibility. Logs include classification, counts, elapsed
seconds and explicit trace/span correlation. Traces use component.operation
names, client/internal kinds and bounded error statuses. Missing records and
conflicts are distinct expected outcomes; errors/invalid input/timeouts are not.

A guard finishes exactly once on success, error, cancellation or panic. OTel
context is attached per Future poll, never by holding a thread-local guard over
an await. An active OTel context takes priority; a tracing-opentelemetry parent
is used when no OTel span is attached. No global subscriber is installed by the
runtime. Custom LogSink implementations must be fast and non-panicking.

## Database/ingestion lifetimes

Selection, database creation/deletion, record CRUD, point writes/ranges/
aggregation, pages and table/selective deletion (where available) share one
policy. Stream setup is not completion: EOF finishes success, an error finishes
failure, early Drop finishes cancellation, and partial delivered rows count.

Background workers instrument enqueue, flush, close and batch writes separately.
Admission is never counted as a committed record. Queue leases balance depth
on dequeue and on failure drops. A batch links at most 32 distinct producer
span contexts; its actual backend write is a child of the batch span. Cancelling
a close waiter ends that wait span, while the owned worker drains and records
its actual writes independently. Unknown partial-write outcomes are not counted
as confirmed records. Flush/close failures remain caller-visible.

Drain workers and finish reads before calling the application's SDK close/flush.
The runtime does not own that lifecycle. The InfluxDB example's observability.rs
performs blocking SDK shutdown on a Tokio blocking worker after the workflow.

## Run the complete example

From the workspace root, using the existing ignored database credentials:

```sh
docker compose -f database/docker/compose.yaml --profile telemetry up -d
set -a
source database/docker/.env
set +a
cargo run -p tpp-database --example influxdb --features telemetry-sdk
docker compose -f database/docker/compose.yaml logs otel-collector
```

The development collector binds loopback port 16009; set OTEL_COLLECTOR_PORT to
change it for both Compose and the example. It exports all three signals to its
own debug logs. This is a local verification sink, not a production retention or
query backend. The example uses a disposable database and leaves Docker running.

## Verification

In-memory SDK exporters verify metrics, logs, correlated parent/child spans,
async isolation, tracing-parent bridging, panic/cancellation, partial streams,
capability preservation, repeated wiring and queue balance after worker failure.
The linked SDK adapter compiles with its real provider types. The live InfluxDB
workflow exported all three signals to the local OTLP collector; tests separately
verify credentials, tenant-like names and raw error text never reach outputs.
Every new source/test file stays below 200 lines including documentation.
