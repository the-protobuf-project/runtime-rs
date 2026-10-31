# Live capture example

Records a BMS point feed into InfluxDB and, at the same time, every point into
rotating Arrow IPC files. Producers are written as generated client code: one
`For::<Point>::create` per reading on `buffered(mirror(influx, arrow))`.

```sh
set -a; source database/docker/.env; set +a
cargo run --release -p tpp-database --example capture
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `CAPTURE_RATE` | 22000 | Points per second |
| `CAPTURE_SECONDS` | 900 | Run length; Ctrl-C stops early |
| `CAPTURE_DATABASE` | bms | InfluxDB database (created if missing) and Arrow directory |
| `CAPTURE_DIR` | captures | Root of the Arrow files |

Without `INFLUX_HOST` it records to Arrow only. When it stops it drains every
queued point into both stores and counts this run back from each one. Replace
`feed::run`'s point construction with real BACnet reads.

Each durable InfluxDB write waits for the server's WAL flush (about 1 s), so
the batch writer keeps 8 batches in flight (`IngestConfig::max_inflight`).
Open the files with `pyarrow.ipc.open_stream(path).read_all()` or
`polars.read_ipc_stream(path)`.

## Reusing the write path

```rust
use database::{DB, IngestConfig, buffered, mirror};

fn record_to_both(primary: DB, arrow: DB) -> database::Result<DB> {
    buffered(mirror(primary, arrow), IngestConfig {
        batch_size: 5_000, queue_capacity: 100_000, max_inflight: 8,
        ..Default::default()
    })
}
```

Select both DBs before calling this helper on a Tokio runtime. Write through a
collection/generated model and call the returned DB's `close().await` before
exit. [main.rs](main.rs) provisions/selects stores and assembles this pipeline;
[feed.rs](feed.rs) defines the point model and paced producer lanes. Startup
creates the capture database directory, producers tag each run, shutdown stops
producers then drains the DB, and verification counts that run in each store.
The InfluxDB database and Arrow captures remain afterward.

For a small Arrow-only run with no inherited server settings:

```bash
env -u INFLUX_HOST CAPTURE_RATE=100 CAPTURE_SECONDS=5 cargo run --release -p tpp-database --example capture
```

The program falls back to Arrow if SDK environment loading fails; check the
reported stores when you intend to record to both. These commands perform real
writes and count verification; compile checks alone do not establish throughput.
