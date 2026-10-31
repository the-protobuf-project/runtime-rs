# Storage and telemetry test support

[Database tests](../README.md)

Small modules here are included by integration targets rather than registered
as standalone Cargo tests. They provide fixtures for boundaries whose failure
and cancellation behavior must be controllable.

| File | Use |
| --- | --- |
| `arrow.rs`, `arrow_points.rs` | Temporary capture directories, descriptors, rows, typed point model |
| `sink.rs` | Controllable batch backend for queue capacity, stalled writes, and drain tests |
| `observed_backend.rs` | Complete capability bundle used to prove instrumentation preserves every operation |
| `observability.rs` | In-memory SDK providers and exported-signal inspection |
| `observation_ingest.rs` | Producer/batch span links and queue-depth release assertions |

```bash
cargo test -p tpp-database --test arrow_segments --test ingest --test observations
```

Consumers assemble a fixture, run the real wrapper/worker, trigger completion,
failure or drop, and then inspect storage results and signal exports. Keep
signal providers explicit so tests do not race process-global configuration.
Wait for worker drains before checking durable counts, and keep temp paths
isolated between independently running targets.
