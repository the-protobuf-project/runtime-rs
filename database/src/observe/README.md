# Capability-preserving database observations

[Database guide](../../README.md) · [Shared signals](../../../telemetry/README.md)

Use the public `store::instrument` re-export to instrument an already selected
DB, or pass telemetry into a backend provider before selecting one.

```rust
use database::{DB, store};
use telemetry::Telemetry;

fn observe(db: DB, signals: Telemetry) -> DB {
    store::instrument(db, signals)
}
```

The application supplies meters, tracers, and loggers; no-op bindings emit
nothing. Reinstrumentation rebuilds wrappers from the DB's raw capabilities,
so new bindings replace old ones instead of stacking duplicate observations.
No-op bindings restore the raw capability paths. Backend results/errors remain
unchanged and every optional capability remains available/refusing as before.

`mod.rs` wires wrappers. `calls.rs` spans one awaited operation;
`classify.rs` maps errors to bounded outcomes without formatting sensitive
backend strings. `driver.rs` wraps CRUD/bulk; `series.rs` wraps schema and time
series; `reader.rs` / `stream.rs` retain the guard until a stream reaches EOF,
errors, or drops. They count only rows delivered to the consumer and never
poll ahead merely for telemetry. Cancellation/panic completes guards once.

Record bodies, names, keys, SQL, filters, cursors, and raw backend messages do
not become signal attributes. Background ingestion observes admission and
acknowledgment separately, so queueing does not inflate durable write counts.

```bash
cargo test -p tpp-database --test observations
```

The test uses real in-memory SDK exporters and complete fake capabilities to
verify exported signals, cancellation, stream errors, and unchanged behavior.
