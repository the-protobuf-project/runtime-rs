# Shared telemetry implementation

[Telemetry guide](../README.md) · [Signal verification](../tests/README.md)

`Telemetry` holds optional caller-supplied meter, tracer, and log sink handles.
`observer(component)` creates shared instruments once; `start` returns an
operation guard that spans the entire asynchronous operation or stream.

```rust
use telemetry::{FutureExt, Outcome, SpanKind, Telemetry};

async fn observed_work(signals: &Telemetry) {
    let observer = signals.observer("example");
    let mut operation = observer.start("memory", "read", SpanKind::Internal);
    let value = async { 42 }.with_context(operation.context()).await;
    operation.read_rows(1);
    operation.finish(Outcome::Ok);
    assert_eq!(value, 42);
}
```

`config.rs` constructs bindings; `observer.rs` creates guards and bridges the
current OTel or tracing parent. `metrics.rs` creates bounded operation counts,
durations, active-call, row, and queue instruments. `logging.rs` accepts explicit
context for correlation and supports the caller's logger or tracing sink.
`outcome.rs` defines stable classifications.

`operation.rs` stores context, elapsed time, and delivered/acknowledged row
counts. Explicit `finish` completes once; Drop reports cancellation or panic
and balances active operations. Without an injected tracer it never ends or
modifies the application's parent span. Attach context per future poll with
`FutureExt::with_context`, rather than holding `Context::attach` across await.

Optional `tpp.rs` adapts the pinned application SDK under `tpp-sdk`; it does
not initialize providers. `from_tpp` snapshots the SDK-installed global tracer;
`from_tpp_provider` resolves handles from an explicit provider. The caller
keeps the owner alive, drains runtime work, then flushes/shuts down providers.

```bash
cargo test -p tpp-telemetry
cargo test -p tpp-telemetry --features tpp-sdk
```
