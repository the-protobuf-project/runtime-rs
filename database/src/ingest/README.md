# Bounded background ingestion

[Database guide](../../README.md) · [Composition](../README.md)

`BatchWriter` queues records for one Resource, amortizing backend requests
while bounding memory and concurrent writes. Queue admission is not durable
storage. `flush` and `close` are the acknowledgment/error barriers.

```rust
use database::{DB, IngestConfig, store::{Record, Resource}};

async fn write_batch(db: &DB, resource: Resource, records: Vec<Record>) -> database::Result<()> {
    let writer = db.batch_writer(resource, IngestConfig {
        batch_size: 500, queue_capacity: 2048, max_inflight: 4, ..Default::default()
    })?;
    for record in records { writer.write(record).await?; }
    writer.flush().await?;
    writer.close().await
}
```

Construct on an active Tokio runtime. `For<T>::batch_writer` offers the same
workflow for typed models. The `buffered` DB wrapper uses these workers to
adapt ordinary `create` calls and marks returned writes `pending: true`.

`config.rs` validates positive capacities/timeouts, 1..=10,000 records per
batch, and 1..=64 batches in flight. Defaults are 500 records, 1024 queue
capacity, one-second flush interval, 30-second write budget, 65,536 serialized
bytes per record, and one in-flight batch. More concurrency allows completion
out of order; choose it for backend throughput rather than ordering guarantees.

`writer.rs` starts/owns the worker and shared terminal failure. `admission.rs`
validates a record and byte limit before reserving queue capacity, then sends
it with telemetry context. A full queue backpressures the producer; canceled
admission does not count as accepted work. `queue.rs` balances queue-depth
leases even when unprocessed records are dropped after a failure.

`worker.rs` flushes on count/timer/barrier, respects concurrency limits, and
drains after senders disappear. `batch.rs` owns each backend write budget and
links bounded producer spans. A terminal failure stops admission and is
reported by future writes/flush/close; partial backend writes remain possible.
Explicit close awaits the worker, and canceling that waiter does not abort
work already admitted. Dropping a writer can only drain while Tokio survives.

```bash
cargo test -p tpp-database --test ingest --test compose --test observations
```
