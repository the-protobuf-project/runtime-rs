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
| `arrow`, `arrow_query`, `arrow_paging`, `arrow_aggregate`, `arrow_segments` | Capture recovery, pagination, keyset order against a reference sort, reductions, rotation and bounds pruning |
| `dependency_graph` | No aws-lc, parquet or zstd is linked, and a capture-only build has one Arrow major |
| `observations` | Every capability, complete stream lifetime, cancellation, safe exported signals |
| `cached`, `cached_capabilities` | `database::cached` over an in-memory store and tpp-cache's in-memory Aside (feature `cached`) |
| `file_size` | Rust sources/tests stay below 200 lines |
| `records` | The record contract: filter subset, page tokens, managed columns, message/column bridge |
| `orm_driver`, `orm_provider`, `orm_sqlite`, `orm_statements`, `orm_dialect` (feature `orm-sqlite`) | runtime-go's `orm` tests on in-process SQLite: CRUD, error mapping, managed columns, paging, schema, tenants, transactions, statement counts, quoting and default types |
| `records` | The record contract without a backend: filters, tokens, bridge, refusals |
| `mongodb_live` | MongoDB against a live replica set (needs `mongodb` + `live-tests`) |

The ignored `live`, `live_lifecycle`, and `live_rate` targets require explicit
real-server settings. They write data; lifecycle/rate tests create and delete
their own databases. Use [local setup](../docker/README.md) and load the env
before opting in:

```bash
cargo test -p tpp-database --test live -- --ignored --nocapture
cargo test -p tpp-database --test live_lifecycle -- --ignored --nocapture
cargo test --release -p tpp-database --test live_rate -- --ignored --nocapture
```

`orm_live` runs the same SQL scenarios against Postgres and MySQL. It is
selected with `live-tests` (not ignored), and an engine that is not up fails it:

```bash
docker compose -p runtime-database-orm-live -f database/docker/compose.orm.live.yaml up -d --wait
cargo test -p tpp-database --features orm,live-tests --test orm_live
docker compose -p runtime-database-orm-live -f database/docker/compose.orm.live.yaml down
```

`RUNTIME_DATABASE_POSTGRES_ADDRESS` (default `127.0.0.1:15432`) and
`RUNTIME_DATABASE_MYSQL_ADDRESS` (default `127.0.0.1:13306`) point it elsewhere.
`--all-features` includes `live-tests`, so it needs the engines too.

`cached_live` runs the cached decorator over a live Redis. Like tpp-cache's
live tests it is selected by the `live-tests` feature and fails without a
server; `RUNTIME_CACHE_REDIS_ADDRESS` defaults to `127.0.0.1:16379`:

```bash
docker compose -p runtime-cache-live -f cache/docker/compose.live.yaml up -d --wait redis
cargo test -p tpp-database --features cached,live-tests --test cached_live
docker compose -p runtime-cache-live -f cache/docker/compose.live.yaml down
```
`mongodb_live` ports runtime-go's `mongodb_test.go` case for case (CRUD,
round trips, duplicate keys, unique indexes, paging, filters, ordering,
transactions, schema, bulk, change streams with resume, tenant isolation) and
reads stored documents raw to pin Go's encoding. It builds only with the
`mongodb` and `live-tests` features; once built an unreachable server fails it,
as the cache live tests do. Each test works in its own database and drops it.
`RUNTIME_DATABASE_MONGODB_ADDRESS` defaults to `127.0.0.1:27117`:

```bash
docker compose -p runtime-database-mongodb-live -f database/docker/compose.mongodb.live.yaml up -d --wait mongodb
cargo test -p tpp-database --all-features
docker compose -p runtime-database-mongodb-live -f database/docker/compose.mongodb.live.yaml down
```

The codec, filter translation, config and name checks also run offline as unit
tests under `--features mongodb`.

`arrow_throughput` is a separate ignored release-mode performance measurement:

```bash
cargo test --release -p tpp-database --test arrow_throughput -- --ignored --nocapture
```

Report which kind of evidence you ran: controlled SDK protocol tests verify
request/response behavior, while real-server tests additionally verify that
server version and configuration. Throughput measurements are environment
specific and are not normal correctness assertions.
