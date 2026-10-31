# Resource descriptors and storage capabilities

[Database guide](../../README.md)

`Resource` declares table/column names and scalar roles. A `Provider` selects a
DB, and `DB::build` wires available capabilities while installing explicit
refusals for missing ones. `For<T>` binds either a generated `Model` or a
serde collection to that resource and DB.

```rust
use database::store::{Column, Kind, Resource, Role};

fn reading_resource() -> database::Result<Resource> {
    Resource::new("readings", vec![
        Column::new("device", Kind::String, Role::Tag),
        Column::new("value", Kind::Float, Role::Field),
        Column::new("time", Kind::Timestamp, Role::Timestamp),
    ])
}
```

A complete create/read/close example is in the [crate guide](../../README.md).
All declared columns are required on writes; unknown or wrongly typed values
are rejected. Time values are signed epoch nanoseconds. Enum values are i32
numbers. Record resources can have a string key without a timestamp, but
window operations require a time column.

```rust
use database::store::{DB, ListOptions, Resource};

async fn read_all(db: &DB, resource: &Resource) -> database::Result<()> {
    let mut options = ListOptions::default();
    loop {
        let page = db.list(resource, &options).await?;
        for row in page.items { println!("{row:?}"); }
        if page.next_page_token.is_empty() { break; }
        options.page_token = page.next_page_token;
    }
    Ok(())
}
```

## File map and validation flow

`resource.rs` validates identifiers/roles and describes schema. `record.rs`
validates scalar records. `driver.rs` supplies CRUD, page options/results, and
`WriteResult` (`pending` distinguishes admission from acknowledgment).
`capabilities.rs`, `timeseries.rs`, and `stream.rs` define transactions,
schema, batching, time windows/reductions, release, and pull-based streams.
`provider.rs` owns DB assembly; `refuses.rs` returns named Unimplemented errors.

`filter.rs` parses AND-conjoined comparisons (`= != > >= < <=`); OR is refused.
`window.rs` validates half-open `[start,end)` ranges and descriptor columns.
`reduce.rs` validates grouping, reductions, and bucket policy. Backends consume
these validated forms so query meaning stays consistent. `typed.rs` and
`typed_stream.rs` encode/decode one model at the boundary. `registry.rs` looks
up generated descriptors by message name. `errors.rs` provides stable failure
variants; match those rather than formatted backend messages.

Page tokens are opaque and backend-specific. Preserve the query when following
them, and use streams for large windows only on backends supplying SeriesReader.
`DB` clones share capabilities and release state; `close` drains/frees selected
resources without closing an externally supplied root SDK client.

```bash
cargo test -p tpp-database --test core --test query
```
