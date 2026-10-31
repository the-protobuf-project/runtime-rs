# InfluxDB 3 storage and administration

[Database guide](../../README.md) · [Runnable workflow](../../examples/influxdb/README.md)

This backend uses `influxdb3-client` for V3 point writes and Arrow Flight SQL
queries. `InfluxAdmin` separately provisions/drops databases because selecting
a DB should not implicitly create one or change its retention policy.

```rust
use std::time::Duration;
use database::{Provider, influxdb::{ClientConfig, InfluxAdmin, InfluxProvider, Retention}};

async fn provision(mut config: ClientConfig) -> database::Result<database::DB> {
    let admin = InfluxAdmin::new(&config, Duration::from_secs(30))?;
    admin.create_database("demo_readings", Retention::For(Duration::from_secs(3600))).await?;
    config.database = "demo_readings".into();
    InfluxProvider::new(config).set_database("demo_readings").await
}
```

Supply `ClientConfig` from your application/environment with endpoint and token
permissions. This creates a new database; an existing name returns AlreadyExists.
Close the returned DB when done. Use the executable example for loading SDK
configuration, writing/querying models, and cleanup of a disposable database.

## End-to-end backend flow

`provider.rs` selects a database and verifies access with the SDK. A supplied
shared client cannot silently be rebound to another database. `driver.rs`
owns the selected client; `crud.rs` and `codec.rs` validate the entire input and
encode SDK points. Writes use nanosecond precision, synchronized WAL, disabled
partial-line acceptance, and chunks of at most 10,000 points. Multiple requests
are not one atomic write.

A point's identity is table + complete tag set + timestamp. Rewriting it merges
fields. Key-based get/update/delete/exists, bulk keyed reads, and transactions
are refused; create/list/count/time-series reads are supported. Tables appear
on first write rather than from eager schema creation.

`sql.rs` builds SQL only from validated descriptors, typed predicates, and fixed
operators. `reader.rs` handles incremental Flight results and keyset paging;
`cursor.rs` binds tokens to host/database/descriptor/query. `series.rs` adds
half-open ranges; `aggregate.rs` performs server-side bucket reductions. Paged
range totals are `-1`, since counting requires another scan. Midstream errors
surface to the reader rather than replaying partially delivered rows.

`admin.rs` and `admin_http.rs` implement explicit management requests;
`retention.rs` models database-wide expiry. Core retention is chosen at creation.
Dropping a schema drops the table and its points. Core does not offer selective
row deletion here; this adapter never substitutes a table drop for that request.
`errors.rs` keeps backend error mapping at the boundary. SQL/cursor unit tests
are beside their implementations.

```bash
cargo test -p tpp-database --test write --test query --test admin
cargo run -p tpp-database --example influxdb
```

The first command uses controlled local peers. The second needs a real server
and credentials; see [Docker setup](../../docker/README.md).
