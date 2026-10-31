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
