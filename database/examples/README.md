# Database end-to-end programs

[Database guide](../README.md) · [Local services](../docker/README.md)

Each subfolder is a Cargo example target with a `main.rs` and small helper
modules. Run from the repository root:

```bash
cargo run -p tpp-database --example influxdb
CAPTURE_RATE=100 CAPTURE_SECONDS=5 cargo run --release -p tpp-database --example capture
```

[InfluxDB](influxdb/README.md) provisions a disposable database, writes typed
points, rewrites identities, filters/ranges/aggregates, ingests in the
background, reads pages/streams, checks retention, and cleans up. It requires
server credentials and administrative permissions.

[Capture](capture/README.md) runs a paced multi-producer feed through
`buffered(mirror(influx, arrow))`, then drains and verifies counts in both
stores. It can record to Arrow alone without usable Influx configuration.
The short settings above avoid the default fifteen-minute/high-rate run.

For real InfluxDB, start Compose and load the ignored `.env` as documented in
the Docker guide. For Arrow-only capture, leave `INFLUX_HOST` unset. Captures
remain on disk for analysis after the program exits; use a separate
`CAPTURE_DIR` to separate recordings.
