# Database contract, backend, and lifecycle tests

[Database guide](../README.md) · [Common fixtures](common/README.md) ·
[Support fixtures](support/README.md)

The default suite uses local files, controlled HTTP peers, an in-process
Flight SQL service, and in-memory signal exporters. It needs no live InfluxDB:

```bash
cargo test -p tpp-database
```

| Targets | Behavior |
| --- | --- |
| `core` | Descriptors, typed models, registries, named refusals |
| `write`, `query`, `admin` | Actual SDK encoding/HTTP/Flight requests and controlled failures |
| `ingest`, `compose` | Queue backpressure, flush/close, errors, buffered/mirrored writes |
| `arrow`, `arrow_query`, `arrow_aggregate`, `arrow_segments` | Capture recovery, pagination, reductions, rotation and bounds pruning |
| `observations` | Every capability, complete stream lifetime, cancellation, safe exported signals |
| `file_size` | Rust sources/tests stay below 200 lines |

The ignored `live`, `live_lifecycle`, and `live_rate` targets require explicit
real-server settings. They write data; lifecycle/rate tests create and delete
their own databases. Use [local setup](../docker/README.md) and load the env
before opting in:

```bash
cargo test -p tpp-database --test live -- --ignored --nocapture
cargo test -p tpp-database --test live_lifecycle -- --ignored --nocapture
cargo test --release -p tpp-database --test live_rate -- --ignored --nocapture
```

`arrow_throughput` is a separate ignored release-mode performance measurement:

```bash
cargo test --release -p tpp-database --test arrow_throughput -- --ignored --nocapture
```

Report which kind of evidence you ran: controlled SDK protocol tests verify
request/response behavior, while real-server tests additionally verify that
server version and configuration. Throughput measurements are environment
specific and are not normal correctness assertions.
