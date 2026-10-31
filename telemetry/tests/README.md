# Telemetry signal and context verification

[Telemetry guide](../README.md) · [Exporter harness](support/README.md)

```bash
cargo test -p tpp-telemetry
cargo test -p tpp-telemetry --test signals --test parents
cargo test -p tpp-telemetry --features tpp-sdk
```

`signals.rs` uses real SDK exporters to check completion counters/duration,
row measurements, correlated logs/spans, cancellation when futures drop,
no-op/meter-only behavior, and concurrent parent separation. `parents.rs`
checks bridging from tracing application spans and panic completion with
balanced active-operation counts. `file_size.rs` enforces the under-200-line
Rust source/test rule.

The harness passes explicit provider handles, runs the operation/future under
a known parent, flushes providers, and then inspects exported values. It avoids
changing global providers so concurrent tests remain independent. Assert that
attributes are bounded classifications and do not include payloads/raw errors.
Cancellation and panic must finish exactly once, including the active gauge.
