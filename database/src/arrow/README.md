# Append-only Arrow IPC captures

[Database guide](../../README.md) · [Capture example](../../examples/capture/README.md)

An `ArrowProvider` maps database/table names to local directories and writes
self-describing Arrow IPC stream segments. It provides time-series create,
bulk create, list/count/range/aggregate, and schema capabilities. Keyed record
operations, transactions, and SeriesReader streaming are not implemented.

```rust
use database::{Provider, arrow::ArrowProvider};

#[tokio::main]
async fn main() -> database::Result<()> {
    let provider = ArrowProvider::new("captures").with_sync(true);
    provider.create_database("demo").await?;
    let db = provider.set_database("demo").await?;
    println!("{}", provider.table_dir("demo", "readings").display());
    // Use collection::<T>(&db, resource) as shown in the database crate guide.
    db.close().await
}
```

The provider creates no files until creation/selection/write. Each selection
creates new segments rather than appending to old captures. The file layout is
`<root>/<database>/<table>/<created_ns>-<pid>-<seq>.arrows`.

## Write, close, and read flow

`provider.rs` selects a database; `driver.rs` owns table writers. `schema.rs`
translates resource columns and roles into Arrow schema metadata. `codec.rs`
validates/encodes records to batches and decodes batches back to scalar records.
`segment.rs` creates files exclusively and publishes them after the schema
header exists. Every batch flushes to the OS; optional sync waits for disk.
`rotation.rs` defaults to 1 GiB or 15 minutes and starts a new segment after
closing the old one.

Clean close writes the IPC end marker and a `.bounds` sidecar (`min max rows`)
through atomic rename. `load.rs` recovers complete batches from truncated files
and reports incomplete captures. Missing/corrupt bounds only disable pruning;
they must never hide rows. `bounds.rs` permits range scans to skip unrelated
closed segments. `filter.rs`, `crud.rs`, `series.rs`, and `aggregate.rs` implement
backend-neutral operations over captured rows. `errors.rs` translates failures.

Crash recovery retains complete batches before an interrupted batch; OS flush
alone is not a power-loss durability guarantee. Call DB close to finish open
segments. Arrow retains append history, so repeated point identities are not
merged as they are in InfluxDB. Range pagination uses offset tokens and totals;
queries scan relevant captured data rather than using an external SQL engine.

```python
import pyarrow as pa

with pa.memory_map("captures/demo/readings/your-segment.arrows", "r") as source:
    table = pa.ipc.open_stream(source).read_all()
print(table.to_pandas())
```

Replace the path with a created segment. This needs Python pyarrow (and pandas
for the final conversion), which are separate from the Rust dependencies.

```bash
cargo test -p tpp-database --test arrow --test arrow_query --test arrow_aggregate --test arrow_segments
```
