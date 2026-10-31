# Runtime telemetry

`tpp-telemetry` is the shared signal interface for runtime packages, analogous
to runtime-go/telemetry. It reuses OpenTelemetry 0.33 API handles. The separate
[telemetry-rs SDK](https://github.com/the-protobuf-project/telemetry/tree/main/telemetry-rs)
owns exporters, configuration, resources and shutdown. Runtime instrumentation
never initializes that SDK, replaces global providers or shuts them down.

## Bind the application's signals

```rust
use opentelemetry::{logs::LoggerProvider, metrics::MeterProvider, trace::TracerProvider};
use opentelemetry_sdk::{logs::SdkLoggerProvider, metrics::SdkMeterProvider, trace::SdkTracerProvider};
use telemetry::Telemetry;

fn main() {
    // Configure your exporters on these builders before build().
    let meters = SdkMeterProvider::builder().build();
    let traces = SdkTracerProvider::builder().build();
    let logs = SdkLoggerProvider::builder().build();
    let signals = Telemetry::default()
        .with_meter(meters.meter("runtime"))
        .with_tracer(traces.tracer("runtime"))
        .with_logger(logs.logger("runtime"));
    let operation = signals.observer("example").start("memory", "read", telemetry::SpanKind::Internal);
    operation.finish(telemetry::Outcome::Ok);
    meters.shutdown().unwrap();
    traces.shutdown().unwrap();
    logs.shutdown().unwrap();
}
```

The binding example needs `tpp-telemetry`, `opentelemetry` 0.33 and
`opentelemetry_sdk` 0.33; configure exporters to send observations anywhere.
Default bindings emit nothing. Signals are independent: meter-only/log-only
observers never modify or end an application's parent span. Each Observer
constructs its instruments once and shares immutable handles across calls.

With the optional `tpp-sdk` feature:

```rust
async fn bind_tpp_sdk() -> Result<(), Box<dyn std::error::Error>> {
    let mut owner = telemetry::sdk::Telemetry::new()
        .with_service("my-service", "1.0.0")
        .with_local_otel_collector()
        .with_tracing()
        .build()?;
    let _signals = telemetry::Telemetry::from_tpp(&owner, "runtime");
    // Run and drain application work using the signals before this shutdown.
    tokio::task::spawn_blocking(move || owner.close()).await??;
    Ok(())
}
```

`from_tpp` uses the SDK's public meter/log handles and explicitly snapshots the
global tracer installed by that high-level SDK owner. For multiple tracer
providers, use `from_tpp_provider(&sdk_provider, scope)` instead: it resolves all
three handles from that explicit TelemetryProvider without consulting globals.
The integration is pinned to inspected SDK revision `5b18b2cb02bb`.

## Instrument packages

```rust
use std::sync::Arc;
use cache::core::Document;
use database::{DB, store};
use telemetry::Telemetry;

fn instrument(db: DB, document: Arc<dyn Document>, signals: Telemetry)
    -> (DB, Arc<dyn Document>)
{
    (store::instrument(db, signals.clone()), cache::with_observability(document, signals))
}
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

## Source and tests

[Source walkthrough and asynchronous guard example](src/README.md) ·
[Exporter tests](tests/README.md).

```bash
cargo test -p tpp-telemetry
cargo test -p tpp-telemetry --features tpp-sdk
```
