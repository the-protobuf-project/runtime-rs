# Database composition and backend routing

[Database guide](../README.md) · [Contracts](store/README.md)

`lib.rs` exports a backend-neutral facade. Resource descriptors and typed
models bind to a selected DB; backend implementations supply capabilities.
Composition wraps the same bundle without changing generated model code.

```rust
use database::{DB, IngestConfig, buffered, mirror};

fn recording_database(primary: DB, capture: DB) -> database::Result<DB> {
    buffered(mirror(primary, capture), IngestConfig {
        batch_size: 500, max_inflight: 4, ..Default::default()
    })
}
```

Call this on an active Tokio runtime after selecting both databases. Creates
on the returned DB admit records to bounded queues and return `pending: true`.
Explicitly call `close().await` to drain and observe write errors.

`collection.rs` adapts hand-written serde structs to `store::For<T>` using a
supplied descriptor. Generated models implement `Model` and use `For::new`.
`typed_ingest.rs` gives those views a typed queue writer with write/flush/close.

`buffered.rs` creates a worker per resource for single-record creates;
operations already expressed as batches and other capabilities pass through.
A failed worker rejects later admissions and reports the error on close.
`mirror.rs` writes and schema-mutates both databases concurrently, requiring
both to succeed; reads, streams, time windows, and transactions use the primary.
There is no distributed rollback. Retrying a partially successful mirror can
append duplicate rows to the Arrow copy while InfluxDB merges point identities.

[InfluxDB](influxdb/README.md) maps requests to SDK writes/Flight SQL;
[Arrow](arrow/README.md) captures local columnar segments;
[ingest](ingest/README.md) manages bounded background work;
[observe](observe/README.md) wraps every capability with shared signals.
Missing backend capabilities refuse by name rather than disappearing.

```bash
cargo test -p tpp-database --test core --test compose --test ingest
```
