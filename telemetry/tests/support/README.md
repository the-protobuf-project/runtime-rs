# In-memory telemetry harness

[Telemetry tests](../README.md)

`mod.rs` builds explicit SDK providers and in-memory exporters, exposes bound
`Telemetry`, and gives tests access to exported spans/logs/metrics. This uses
the real export path while avoiding network collectors and global providers.

```bash
cargo test -p tpp-telemetry --test signals --test parents
```

Consumers create a harness, pass its handles into observers, run work, flush,
and inspect the emitted signals. Keep the provider owner alive through future
or stream completion; dropping it before a worker drains can lose the
observations the test intends to check. The harness is test-only and does not
install production exporters or make runtime libraries own SDK shutdown.
