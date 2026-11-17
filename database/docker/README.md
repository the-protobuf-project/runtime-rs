# InfluxDB and optional telemetry collector

[Database guide](../README.md) · [InfluxDB example](../examples/influxdb/README.md)

`compose.yaml` runs InfluxDB 3 Core on `127.0.0.1:8181`, with file storage in
its named `influxdb-data` volume. `INFLUXDB_IMAGE` can pin a release/digest.
The optional `telemetry` profile starts a collector using
`otel-collector.yaml`; its loopback OTLP gRPC port defaults to 16009 and can
be changed with `OTEL_COLLECTOR_PORT`.

```bash
cp database/docker/.env.example database/docker/.env
docker compose -f database/docker/compose.yaml up -d --wait influxdb
```

Edit the ignored `.env` with your local token and SDK settings. The template
contains `INFLUX_HOST`, `INFLUX_DATABASE`, and `INFLUX_TOKEN`; its placeholder
is not a usable token. Obtain a token with the
permissions needed by your local server; the disposable-database example needs
administrative access. Put the token in `.env`, then export settings before running Rust
(the Compose env file does not export variables into your shell):

```bash
set -a
source database/docker/.env
set +a
cargo run -p tpp-database --example influxdb
```

The healthcheck accepts HTTP 200 or 401 as liveness. The Rust provider's query
probe checks actual access; a healthy container alone does not establish
correct token permissions. The example provisions its own disposable database
and cleans it up; the environment database is only SDK configuration.

```bash
docker compose -f database/docker/compose.yaml --profile telemetry up -d
cargo run -p tpp-database --example influxdb --features telemetry-sdk
docker compose -f database/docker/compose.yaml logs otel-collector
docker compose -f database/docker/compose.yaml --profile telemetry down
```

The collector is a development inspection sink. Ordinary `down` preserves the
InfluxDB named volume; do not remove volumes when you intend to retain data.
Application shutdown drains workers before flushing SDK exporters.

## SQL engines for the `orm` backend

`compose.orm.live.yaml` runs PostgreSQL 17 on `127.0.0.1:15432` and MySQL 8.4
on `127.0.0.1:13306` (database `runtime`; users `postgres`/`postgres` and
`root`/`mysql`), on high ports so engines already on 5432/3306 are untouched.
No volumes: the data is disposable. Keep `-p` and `-f` the same on shutdown.

```bash
docker compose -p runtime-database-orm-live -f database/docker/compose.orm.live.yaml up -d --wait
cargo test -p tpp-database --features orm,live-tests --test orm_live
docker compose -p runtime-database-orm-live -f database/docker/compose.orm.live.yaml down
```
