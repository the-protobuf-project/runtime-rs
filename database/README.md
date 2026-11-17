# Database

One storage contract, many providers. A program describes what it stores with a
descriptor, picks a provider for where it stores it, selects a database from that
provider, and then reads and writes through one `DB`, whichever provider is behind it.
This is the Rust port of
[runtime-go/database](https://github.com/the-protobuf-project/runtime-go/tree/main/database),
so a Go and a Rust program can share generated descriptors and, for most providers,
read each other's data.

There are two contracts, because records and time series need different things:

| Contract | Holds | Providers |
| --- | --- | --- |
| [`records`](src/records/mod.rs) | Proto messages keyed by a primary key: create, get, update, delete, list | SQL (Postgres, MySQL, SQLite), MongoDB, Redis, Tauri store; a read-through cache over any of them |
| [`store`](src/store/README.md) | Points keyed by tags and a timestamp: append, list, windows, aggregates | InfluxDB 3, Arrow captures |

Both follow the same steps and the same rules for filters, paging and errors.
The first part of this guide covers records and the second covers time series.

## Contents

- [Choosing a provider](#choosing-a-provider)
- [Installing](#installing)
- [The shape every provider shares](#the-shape-every-provider-shares)
- [Getting a DB from each provider](#getting-a-db-from-each-provider)
- [Sequencing: what happens in what order](#sequencing-what-happens-in-what-order)
- [Validation](#validation)
- [Filters, ordering and paging](#filters-ordering-and-paging)
- [Outcomes and errors](#outcomes-and-errors)
- [Transactions, bulk, schema and watching](#transactions-bulk-schema-and-watching)
- [Edge cases, provider by provider](#edge-cases-provider-by-provider)
- [Repositories](#repositories)
- [Time series: InfluxDB and Arrow](#time-series-influxdb-and-arrow)
- [Telemetry](#telemetry)
- [Testing](#testing)

## Choosing a provider

| Provider | Feature | Where data lives | Pick it when |
| --- | --- | --- | --- |
| SQL (`orm`) | `orm`, or one of `orm-postgres`, `orm-mysql`, `orm-sqlite` | One table per resource | You want relational storage, real transactions and queries other tools can run |
| MongoDB (`mongodb`) | `mongodb` | One collection per resource, one document per record | You want documents, change streams and tenant isolation by database |
| Redis (`redis`) | `redis` | One key per record, plus index and uniqueness keys | You want fast key access and already run Redis for the cache |
| Tauri store (`tauri_store`) | `tauri-store` | One JSON file in a Tauri app's data directory | You ship a desktop or mobile app and need local, embedded storage |
| Read-through cache (`cached`) | `cached` | In front of any provider above, in tpp-cache | Reads by key dominate and can tolerate a TTL's staleness |
| InfluxDB 3 (`influxdb`) | `influxdb` (default) | InfluxDB tables of points | You store metrics and sensor readings and query them by time |
| Arrow captures (`arrow`) | always on | Arrow IPC files on disk | You record time series locally, for replay or for pyarrow, polars or DuckDB |

What each records provider can do beyond create, get, update, delete, list, count and exists:

| | Transactions | Schema | Bulk | Watch | Filters | Order by |
| --- | --- | --- | --- | --- | --- | --- |
| SQL | yes, savepoints when nested | `CREATE TABLE` | refused | refused | AIP-160 | any columns |
| MongoDB | yes, replica set needed | collection + unique indexes | yes | change streams | AIP-160 | first column |
| Redis | refused | always present | yes | refused | refused | key only |
| Tauri store | yes, nesting refused | always present | yes, all or nothing | refused | AIP-160, in memory | any columns |
| Cache | the wrapped provider's | the wrapped provider's | the wrapped provider's | the wrapped provider's | passed through | passed through |

A capability a provider lacks is never missing from the `DB`: calling it returns
`Error::Unimplemented { backend, capability }`, which names both.

## Installing

```toml
[dependencies]
tpp-database = { git = "https://github.com/the-protobuf-project/runtime-rs", features = ["orm-postgres"] }
prost-reflect = "0.16"
```

The default features build only the time-series contract with InfluxDB. Turn on
each records provider you use, so a build links only the drivers it needs:

| Feature | Links | Notes |
| --- | --- | --- |
| `orm-postgres`, `orm-mysql`, `orm-sqlite` | sqlx 0.9 (rustls on ring), sea-query 1 | `orm` turns on all three; SQLite is bundled |
| `mongodb` | the official `mongodb` 3 driver, bson 3 (rustls on ring) | |
| `redis` | `redis` 1, the same crate tpp-cache uses | |
| `tauri-store` | `tauri` 2 and `tauri-plugin-store` 2 | On Linux, needs `libwebkit2gtk-4.1-dev` and `libgtk-3-dev`, as any Tauri app does |
| `cached` | tpp-cache | |
| `influxdb` | `influxdb3-client` 0.3 (Arrow 59) | Default. Turn it off for an Arrow-only build that links one Arrow major (60) and no HTTP client |

## The shape every provider shares

Every records provider works in the same five steps:

1. **Own a client.** Open a pool or connection the way its library does. The
   provider borrows it and never closes it, so one client can serve the cache,
   the database and streams.
2. **Make a provider** from the client.
3. **Select a database** with `set_database(name)`. This validates the name,
   checks the server is reachable and returns a `DB`. An empty name selects the
   provider's default.
4. **Use the `DB`.** CRUD is called on the `DB` itself (it dereferences to the
   driver). Capabilities are fields: `db.tx`, `db.schema`, `db.batch`, `db.watch`.
5. **Close the `DB`** with `db.close()`. This frees what the selection holds,
   never the client.

What gets stored is described by a `Resource`: the message type, the table, the key
and each column. Generated code emits these; written by hand, one looks like this:

```rust
use database::records::{Column, Generated, Kind, Resource};
use prost_reflect::MessageDescriptor;

fn books(message: MessageDescriptor) -> Resource {
    let mut created = Column::new("create_time", Kind::Timestamp).sql_type("TIMESTAMPTZ");
    created.auto_create = true; // the provider sets it on create
    let mut updated = Column::new("update_time", Kind::Timestamp).sql_type("TIMESTAMPTZ");
    updated.auto_update = true; // and this one on every write
    Resource::new(message, "books", "name", vec![
        Column::new("name", Kind::String).primary_key().sql_type("VARCHAR(255)"),
        Column::new("title", Kind::String).not_null().unique().sql_type("VARCHAR(255)"),
        Column::new("pages", Kind::Int).not_null().sql_type("INTEGER"),
        Column::new("subtitle", Kind::String), // optional: stored as null when unset
        created,
        updated,
    ])
}
```

A key the message does not carry can be generated: set `generated` to
`Some(Generated::Ulid)` or `Some(Generated::Uuid)` on that column and leave its
`field` empty.

Everything after `set_database` is the same code for every provider. This function
works unchanged on SQL, MongoDB, Redis, the Tauri store and the cache:

```rust
use database::records::{DB, ListOptions, Resource, Typed};
use prost_reflect::DynamicMessage;

async fn use_any_provider(db: &DB, books: &Resource, dune: &DynamicMessage) -> database::Result<()> {
    db.schema.ensure_schema(books).await?;                 // a no-op where there is no schema

    let stored = db.create(books, dune).await?.message;    // audit times filled in
    let again = db.get(books, "books/dune").await?;
    db.update(books, &again).await?;

    let page = db.list(books, &ListOptions::default()
        .filter("pages >= 300")                          // Redis refuses filters
        .order_by("pages desc")
        .page_size(20)).await?;
    for book in &page.items { /* ... */ }
    // page.next_page_token is empty on the last page; page.total is -1 with omit_total

    let n = db.count(books, &ListOptions::default()).await?;
    let there = db.exists(books, "books/dune").await?;
    db.delete(books, "books/dune").await?;
    let _ = (stored, n, there);
    Ok(())
}
```

With generated prost types, a `Typed` view drops the descriptor argument and the
conversions:

```rust,ignore
let books = Typed::<pb::Book>::new(&db, Arc::new(books))?; // fails if the types don't match
let dune: pb::Book = books.get("books/dune").await?;
let all: Vec<pb::Book> = books.all(&ListOptions::default()).await?; // follows every page
```

## Getting a DB from each provider

Each block below ends with a `DB`; from there, the code above applies.

### SQL: Postgres, MySQL, SQLite

```rust,ignore
use database::orm::{OrmProvider, sqlx::PgPool};      // sqlx is re-exported: open pools with it

let pool = PgPool::connect("postgres://app@localhost/app").await?;
let db = OrmProvider::new(pool).set_database("tenant_a").await?;
```

- **The selected name** becomes the resource's schema: a Postgres schema, a MySQL
  database or an attached SQLite database. It must already exist. `ensure_schema`
  creates tables, not schemas.
- **Tables** are created as runtime-go creates them: each column's `sql_type`, or
  the dialect's default for its kind (`BYTEA`, `LONGBLOB` or `BLOB` for bytes),
  then `PRIMARY KEY`, `UNIQUE` and `NOT NULL`. Identifiers are quoted per dialect,
  with backticks on MySQL.
- **Without a selection,** `orm::driver(pool)` returns a `DB` directly, with no
  name check and no ping.
- **Closing** is a no-op, since the pool is yours.

### MongoDB

```rust,ignore
use database::mongodb::{MongoClient, MongoConfig, MongoProvider};

let client = MongoClient::connect(MongoConfig::address("localhost:27017")).await?; // pings
let db = MongoProvider::new(client.clone()).set_database("tenant_a").await?;
// ...
client.close().await; // the client is yours
```

- **The selected name** is a real MongoDB database, so tenants are isolated by the
  server. `MongoProvider::drop_database` removes one.
- **`MongoConfig`** has every field of Go's `Config`: URI or address, credentials,
  auth source, connect timeout and pool bounds. Credentials go to the driver as a
  credential object, so passwords may contain `@`, `/` or `:`.
- **Transactions and change streams** need a replica set. A single-node one is
  enough.

### Redis

```rust,ignore
let client = Arc::new(cache::drivers::redis::RedisClient::connect(config).await?);
let db = database::redis::RedisProvider::new(client.connection_manager().await?)
    .with_prefix("app")
    .set_database("tenant_a")
    .await?;
```

- **The connection** is the cache's own. `RedisClient::connection_manager` (or
  Dragonfly's) lends it, so the cache and records share one multiplexed connection
  as Go's layers share one client. The provider never closes it.
- **The selected name** is a key segment, not a numbered Redis database. Keys follow
  Go's layout byte for byte, so Go and Rust read each other's records:

  ```text
  {prefix}:{database}:{table}:rec:{key}          one record, proto wire bytes
  {prefix}:{database}:{table}:ids.z              sorted set of keys, for list and count
  {prefix}:{database}:{table}:uniq:{col}:{value} a unique value's reservation
  ```

### Tauri store

```rust,ignore
tauri::Builder::default()
    .plugin(tauri_plugin_store::Builder::default().build())
    .setup(|app| {
        let provider = database::tauri_store::TauriStoreProvider::open(app.handle(), "records.json")?;
        let db = tauri::async_runtime::block_on(provider.set_database(""))?;
        app.manage(db);
        Ok(())
    });
```

- **The file** lives in the app's data directory, unless the path is absolute.
- **The selected name** is a key prefix, so one file holds many tenants. Keys follow
  the Redis layout, with each record stored as a readable JSON object.
- **Autosave:** `open` turns the plugin's autosave off and saves after every write,
  so a write that returned is on disk. `.durable(false)` leaves saving to the
  plugin or to `db.close()`.
- **Your own store:** `TauriStoreProvider::new` takes a store you built. With its
  autosave on, a transaction's writes can reach the file before it commits.
- **runtime-go has no counterpart,** since Tauri is Rust's.

### A read-through cache over any of them

```rust,ignore
use database::cached;

let cache = redis_cache.set_database("records").await?;                 // a tpp-cache database
let ttl = cache::core::Options::default().with_ttl(Duration::from_secs(60));
let db = cached::wrap(&db, cached::from_aside(Arc::new(cache), ttl));   // still a records DB
```

`wrap` returns a records `DB`, so the rest of your code does not change.

| Operation | Through the cache |
| --- | --- |
| `get` | Cached per record, keyed by its primary key. Concurrent misses collapse into one load, and `NotFound` is remembered for the cache's 30 s negative lease |
| `create`, `update`, `delete` | Written to the provider first, then the key is invalidated. A failed invalidation fails nothing |
| `list`, `count`, `exists` | Passed through: they depend on the whole set, or are cheaper than a cached record |
| `tx.run` | Reads inside go to the provider. Written keys are invalidated after a commit and left alone after a rollback |
| `batch`, `schema`, `watch` | Kept where the provider has them. `create_many` invalidates like `create` |

- **Always set a TTL.** It bounds what invalidation misses: a failed invalidation,
  or a writer that bypasses the cache.
- **An overloaded cache** (`cached::Overloaded`) is read through to the provider
  rather than failed.
- **Records are cached as proto wire bytes,** base64 in a JSON string, which is Go's
  encoding, so Go and Rust share entries.

## Sequencing: what happens in what order

**Starting up**
1. Open the client. For MongoDB, `connect` pings first, so a wrong address fails here.
2. `set_database` checks the name ([Validation](#validation)), then pings the server
   (SQL, MongoDB, Redis) before returning a `DB`. An unreachable server fails here,
   not on the first write.
3. Call `ensure_schema` for each resource before writing to it. On SQL it creates
   the table; on MongoDB the collection and one `uniq_<column>` index per unique
   column. Elsewhere it does nothing, and it is safe to call every start.

**Each write.** A `create` runs in this order, on every provider:
1. The message is read into columns through the descriptor. An unset optional field
   becomes null.
2. Columns the provider manages are filled: a generated key if it is empty, and the
   `auto_create` and `auto_update` times.
3. The key and unique values are checked; a taken one is `AlreadyExists` and nothing
   is written.
4. The record is written.
5. You get back what was stored, including the filled-in fields.

An `update` keeps the stored create time and refreshes the update time. It moves
unique reservations only for the values that changed.

**Atomicity of one write:**
- **SQL:** one statement.
- **MongoDB:** one command.
- **Redis:** one Lua script over the record, its index entry and its reservations.
  A refused or interrupted write leaves nothing behind.
- **Tauri store:** runs under the store's lock.

In every case, two concurrent creates of one unique value admit exactly one.

**Transactions.** `db.tx.run(work)` hands `work` a `DB` bound to the transaction.
Use that `DB`, not the outer one, inside the work.
- `Ok` commits and `Err` rolls back.
- The caller gets the work's own error back.
- The work runs once. On MongoDB only a commit whose outcome is unknown is retried.
- Using the transaction's `DB` after `run` returns is an error.

**Shutting down.** `db.close()` frees the selection, and on the Tauri store with
`.durable(false)` it saves. Close the client afterwards, if it is yours to close.

## Validation

Inputs are checked before anything reaches the server, and every refusal is
`Error::Invalid` with a message naming what was wrong.

| What | Rule |
| --- | --- |
| Database name | Letters, digits and underscores; no digit first; at most 63 characters; empty selects the default. It goes where an identifier goes, so `tenant-a` or `a;drop` are refused |
| Key | Typed by the key column's kind on SQL and MongoDB, so `"abc"` for an integer key is refused. An empty key is refused on Redis and the Tauri store |
| Values | Read through the descriptor. A value that does not fit its column's kind (a timestamp column on a non-`Timestamp` field, an unparseable stored time) is refused, naming the resource and column |
| Filter | Parsed before the query: an unknown column, a value that is not of its column's kind (`pages = many`), an unknown operator or `OR` is refused |
| `order_by` | Never refused: an unknown field orders by the key instead. Order is presentation, so the right records in an unexpected order beat an error |
| Page token | An unreadable token reads from the start rather than failing |
| `Typed` view | Built only when the resource's message is the view's type |

## Filters, ordering and paging

**Filters** are [AIP-160](https://google.aip.dev/160) as protoc-gen-store's generated Go
`filterx` reads it, so a Rust service accepts the filters a Go one does and matches the
same records. Every provider but Redis parses them into one tree,
`records::core::filter::Expr`, and runs the same conformance table.

```text
pages >= 300
title = "Dune" AND pages < 500
status = PUBLISHED OR (in_print = true AND NOT subtitle : novel)
-title = Dune pages > 100            # whitespace is AND, `-` is NOT
create_time >= 2026-01-01
dune                                 # free text over the search columns
```

- **Logic:** `AND` or whitespace, `OR`, `NOT` and `-`, with parentheses. `AND` is a
  keyword in any case, but `OR` and `NOT` only in capitals; lowercase `or` and `not` are
  ordinary words. `NOT` binds tightest, then `OR`, then `AND`, so `a AND b OR c` is
  `a AND (b OR c)`. Stray `AND`s are skipped. Parentheses and negations nest at most 32
  deep.
- **Values** are barewords (`SUMMER25`, `2026-01-01`) or double-quoted strings. A string
  has no escapes and ends at the next `"`. A single quote is an ordinary character, so
  `title = 'Dune'` compares with `'Dune'`, quotes included. Only spaces and tabs separate
  terms. `*`, `%` and `_` are literal: there are no wildcards.
- **Operators by column kind,** as Go's gorm engine applies them. Any other operator is
  refused as `unsupported operator`.

  | Kind | Operators | Value |
  | --- | --- | --- |
  | String | `=`, `!=`, and `:` for a case-insensitive contains | as written |
  | Integer, float | `=`, `!=`, `<`, `<=`, `>`, `>=` | base 10, or a number |
  | Timestamp | `=`, `<`, `<=`, `>`, `>=` | RFC 3339, quoted because of its colons, or a bare `YYYY-MM-DD` as midnight UTC |
  | Boolean | `=`, `!=` | as Go reads it: `1`, `t`, `true`, `TRUE`, `0`, `f`, `false` and so on |
  | Enum | `=`, `!=` | the number, or the value name in any case with or without its prefix (`PUBLISHED`, `status_published`) |
  | Bytes | `=`, `!=` | as written |

- **Free text:** a value with no operator after it, such as `dune` or `"a novel"`, matches
  a record when any of the resource's search columns contains it, ignoring case.
  `Resource::search` lists those columns. A resource without any refuses free text
  (`free-text search is not supported here`).
- **Nulls:** a comparison or contains on a null column is false, including `!=`. `NOT` is
  true wherever its operand is false, so a book without a subtitle matches
  `NOT subtitle = "x"` but not `subtitle != "x"`. This is Go's `NOT coalesce(..., false)`.
  There is no presence test in filter text: `field:*` contains a literal `*`.
- **Refused** as `Error::Invalid` in Go's words, with the filter and the position. That
  covers unknown fields, unsupported operators, malformed values, unbalanced or empty
  parentheses, an unterminated string, a lone `!` or `-`, a missing operand or value, and
  nesting past 32. A dotted field (`author.name`) is an unknown field.
- **Redis refuses any filter,** since it has no query language. List by key and
  narrow in your code.

| Provider | How a filter runs |
| --- | --- |
| SQL | A sea-query `Condition`: `all` and `any`, `NOT COALESCE(x, FALSE)` for NOT, and `LOWER(col) LIKE '%v%' ESCAPE '!'` for contains and free text |
| MongoDB | A query: `$and`, `$or` and `$nor`, `$nin: [v, null]` for `!=`, and a case-insensitive `$regex` of the escaped value for contains and free text |
| Tauri store | Evaluated in memory by `Expr::matches` |
| Redis | Refused |

`records::core::parse_filter` still returns runtime-go's flat list of `Clause`s for a
filter that is only `AND`s, and refuses `OR`, `NOT` and free text.

**Ordering** takes AIP-132 `field [desc], other`. The key always ends the order, so
records with equal values keep a stable order from page to page.

| Provider | Order by |
| --- | --- |
| SQL, Tauri store | Every field listed |
| MongoDB | The first field listed, then the key |
| Redis | The key only, either direction |

On the Tauri store, nulls sort first.

**Paging** uses tokens shaped after AIP-132:

```rust,ignore
let mut opts = ListOptions::default().page_size(50);
loop {
    let page = db.list(&books, &opts).await?;
    // ... page.items ...
    if page.next_page_token.is_empty() { break; }
    opts.page_token = page.next_page_token;   // keep the same filter and order
}
```

- **Page size:** zero or less means 50.
- **Tokens** are offsets. Follow a token with the same filter and order it came from.
- **Totals:** `page.total` counts every match. `omit_total(true)` skips that count,
  which halves the cost of a page on SQL and MongoDB. The provider then reads one
  row past the page to learn whether another follows, and reports `total` as -1.
- **`Typed::all`** follows every token for you and refuses a repeated token rather
  than looping.

## Outcomes and errors

Callers match on `database::Error`, never on a provider's own messages:

| Error | When |
| --- | --- |
| `NotFound` | `get`, `update` or `delete` of a key with no record |
| `AlreadyExists` | `create` of a taken key; any write of a value another record holds in a unique column |
| `Invalid(reason)` | A refused input ([Validation](#validation)), or a transaction's `DB` used after it ended |
| `Unimplemented { backend, capability }` | A capability the provider lacks, or any AIP-160 filter on Redis |
| `Backend { backend, source }` | The server or driver failed: connection lost, timeout, permission |

## Transactions, bulk, schema and watching

**Transactions**
- **SQL:** a database transaction, with savepoints when nested.
- **MongoDB:** a session. CRUD and bulk calls inside the work join it, and nesting is
  refused.
- **Tauri store:** the work holds the store's lock. A commit is one save, and a
  rollback restores the selected database's keys. Nesting is refused.
- **Redis:** refused.

**Bulk** (`db.batch`)
- **`create_many` on MongoDB** is one ordered insert. If one record is refused, the
  ones before it stay written, and you get `AlreadyExists`.
- **`create_many` on Redis** stops at the first failure.
- **`create_many` on the Tauri store** is all or nothing.
- **`get_many`** returns one entry per key, in order, with `None` for a missing key:
  one `$in` per 512 keys on MongoDB, and one round trip of `MGET`s on Redis.

**Schema** (`db.schema`)
- `ensure_schema` is safe to repeat.
- `has_schema` reports whether the table or collection exists. It is always true on
  Redis and the Tauri store.
- `drop_schema` removes the table, the collection, or every key of the resource.

**Watching** (`db.watch`) is MongoDB only: a change stream from now, or from a resume
token a previous change carried, with `Created`, `Updated` and `Deleted` changes. The
stream ends after its first error; resume from the last token you saw.

## Edge cases, provider by provider

**Every provider**
- An unset optional field is stored as null and comes back unset.
- A field the descriptor does not map survives a create, because only managed fields
  are written back into what you sent.
- Timestamps are UTC.

**SQL**
- Unique violations are `AlreadyExists` on update as well as create. Go returned the
  raw driver error there.
- An unsigned value past 2^63 is refused outside MySQL rather than wrapped.
- An in-memory SQLite database exists per connection, so use a pool of one.
- SQLite is bundled at 3.53 (`libsqlite3-sys` 0.37, the newest sqlx 0.9 allows),
  which carries CVE-2026-119375 (RUSTSEC-2026-0459): opening a database file with a
  crafted `-wal` file can return stale process memory. Open only database files your
  service wrote. The advisory is accepted in `.cargo/audit.toml` until sqlx allows
  the fixed `libsqlite3-sys` 0.39.

**MongoDB**
- The encoding matches Go's:
  - the key is `_id`;
  - integers are int64 and enums int32;
  - bytes are generic binary;
  - a `uint64` past `i64::MAX` is a Decimal128, so it reads back exactly.
- Times are BSON dates, so they keep milliseconds, not nanoseconds.
- A column named twice in a filter keeps both bounds (`pages >= 2 AND pages < 5`).
  Go kept only the last.
- A filter on a `uint64` past `i64::MAX` compares against the Decimal128 the value is
  stored as. Bytes compare as the binary they are stored as. An empty bytes value is
  stored as null, so it fails `cover != "x"`, where SQL's empty bytes pass.

**Redis**
- Records are proto wire bytes, so bytes and integers past 2^53 survive exactly.
- An update or delete compares the stored bytes it read, and retries up to 16 times
  if another writer changed the record in between.
- Transactions are refused, and so are filters: Redis cannot run AIP-160.

**Tauri store**
- The plugin writes its file in place, not through a rename, so a crash mid-save can
  truncate it.
- Uniqueness is race-free within the process. Two processes writing one file are
  not coordinated.
- Non-finite floats are stored as `"NaN"`, `"Infinity"` and `"-Infinity"`, and bytes
  as base64.

**Cache**
- Keys are the record key alone, as in Go, so resources sharing one cache database
  must not share keys. AIP resource names never collide.

## Repositories

A repository is what a generated service persists one resource through: typed
messages, resource names, etags, field masks and soft delete, on top of any records
provider. `RecordsRepository<M>` implements the `Repository<M>` trait for any `DB`, as
protoc-gen-store's Go `target=repository` does for GORM. Generated code supplies the
`Resource` and the `Rules`; written by hand:

```rust,ignore
use database::repository::{Hooks, ListInput, RecordsRepository, Reference, Repository, Rules};

let rules = Rules {
    etag: Some("etag".into()),
    delete_time: Some("delete_time".into()),        // turns on soft delete
    expire_time: Some("expire_time".into()),
    retention: Duration::from_secs(30 * 24 * 3600),
    output_only: vec!["create_time".into(), "update_time".into(), "delete_time".into(),
                      "expire_time".into(), "etag".into()],
    pattern: "publishers/{publisher}/books/{book}".into(),
    parent_column: Some("publisher_id".into()),     // field-less column, filled from the name
    references: vec![Reference { field: "author".into(), pattern: "authors/{author}".into() }],
};
let books = RecordsRepository::<pb::Book>::new(&db, Arc::new(books_resource), rules)?
    .with_hooks(Hooks::default().before_create(|mut b| async move { b.title = b.title.trim().into(); Ok(b) }));

let book = books.create(&draft).await?;                          // etag minted, times set
let book = books.update(&edit, &["title".into()]).await?;        // edit.etag guards it
let page = books.list(&ListInput { parent: "publishers/p1".into(), ..Default::default() }).await?;
books.delete(&book.name, Some(&book.etag)).await?;               // soft: delete_time set
books.undelete(&book.name, None).await?;
books.purge(&book.name).await?;
```

| Call | What it does |
| --- | --- |
| `create` | Runs `before_create`, mints the etag, writes, and returns the record read back. A root resource without a name gets `<collection>/<ULID>`; a taken name or unique value is `AlreadyExists` |
| `get` | The record, soft-deleted or not |
| `list`, `count` | `ListInput` as `ListOptions`; page tokens are the provider's. Soft-deleted records are left out unless `show_deleted`, and `parent` keeps one parent's records |
| `update(m, paths)` | Merges the masked fields of `m` onto the stored record and mints an etag (below) |
| `update_output_only(m, paths)` | The same for output-only fields, which `update` ignores; `paths` is required |
| `delete(name, if_match)` | Soft delete, or a removal without a `delete_time` rule |
| `undelete(name, if_match)` | Clears the soft-delete times; `AlreadyExists` on a live record |
| `purge(name)` | Removes the record, deleted or not |

**Etags** are ULIDs, monotonic within the process, minted on every write. A write
carrying one (`m`'s etag field, or `if_match`) succeeds only while it is still the
stored etag, else `Conflict`; an empty etag carries no precondition.

**Field masks** (AIP-134):
- an empty mask, or `*`, writes every mutable field;
- an output-only path, the etag or an audit time is ignored (AIP-203);
- a path naming no field is `Invalid`;
- a path into a message field (`publish_time.seconds`) writes the whole field, and
  one arm of a oneof the whole oneof.

**Soft delete** (AIP-164), with a `delete_time` rule:
- `delete` sets `delete_time`, moves `update_time` to the same instant, sets
  `expire_time` to `delete_time` plus the retention (unset when zero), and mints an etag;
- deleting a deleted record is `NotFound`;
- `undelete` clears both times; `list` and `count` skip deleted records through a
  `delete_time = null` clause ANDed onto the caller's filter.

**Atomicity of a guarded write.** The etag check and the write run in one
transaction where the provider has one:

| Provider | How two writers holding one etag are kept apart |
| --- | --- |
| SQL | The read is `SELECT ... FOR UPDATE` (Postgres, MySQL): the second writer waits, then sees the new etag. SQLite locks the database |
| MongoDB | A session transaction: the later writer's write conflict is `Conflict` |
| Tauri store | The transaction holds the store's lock |
| Redis | No transactions: the check and the write are two steps, so a writer landing between them is overwritten |

**Names, parents and references**, as Go's adapter stores them, so Go and Rust share
a table:
- every name is checked against `pattern`, and a misfit is `Invalid`;
- a root resource's field-less generated key column (`id`) is its name's last id; a
  nested one's is a fresh ULID, since its id is unique only under its parent;
- `parent_column` is filled from the parent's id in the name;
- each of `references` is stored as the bare id and read back as the name;
- an enum column with `labels` stores the label (`EBOOK`), not the number. On Postgres
  a `sql_type` names its enum type, which `ensure_schema` creates.

**Listing under a parent.** `ListInput.parent` narrows to one parent's records, as
Go's `parentScope`:
- with a `parent_column` and the parent's own id, by the clause `publisher_id = "p1"`;
- with ids all `-` (AIP-159), or empty, every record;
- otherwise (no parent column, or a `-` below a named ancestor) the repository matches
  the records' names itself, reading every match.

**Hooks** (`Hooks<M>`, Go's `<Model>Hooks`): `before_create`, `after_read`,
`before_update` (inside the write's transaction, after the mask is applied) and
`before_delete` (an error vetoes `delete` and `purge`).

**Differences from Go.**
- An unknown mask path is `Invalid`, where Go ignores it.
- `*` is a full mask.
- `create` takes the parent in the name rather than as an argument
  (`publishers/p1/books/` mints the id).
- Filters on a reference compare the stored bare id.
- A parent column under a nested parent (Go looks up its surrogate) is refused.
- Without a filter language (Redis), soft-deleted records and parents are filtered by
  the repository, reading every record, and a caller's filter is refused as before.

## Time series: InfluxDB and Arrow

The `store` contract has the same steps (`Provider`, `set_database`, a `DB`, `close`)
for points instead of records. A point has no key: it is a table, a complete set of
tags and a timestamp, and rewriting the same identity merges its fields. Columns have
a role: `Key`, `Tag`, `Field` or `Timestamp`. Every declared column is required on a
write, times are signed epoch nanoseconds, and enums are `i32` numbers.

This example needs no server: it writes Arrow files under `captures/`.

```rust,no_run
use database::{Provider, arrow::ArrowProvider, collection,
    store::{Column, Kind, ListOptions, Resource, Role}};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct Reading { device: String, value: f64, time: i64 }

#[tokio::main]
async fn main() -> database::Result<()> {
    let readings = Resource::new("readings", vec![
        Column::new("device", Kind::String, Role::Tag),
        Column::new("value", Kind::Float, Role::Field),
        Column::new("time", Kind::Timestamp, Role::Timestamp),
    ])?;
    let provider = ArrowProvider::new("captures");
    provider.create_database("demo").await?;
    let db = provider.set_database("demo").await?;
    let rows = collection::<Reading>(&db, readings);
    rows.create(&Reading { device: "ahu1".into(), value: 21.5, time: 1_000_000_000 }).await?;
    let (page, next, total) = rows.list(&ListOptions::default().page(10)).await?;
    println!("{page:?}; next={next}; total={total}");
    db.close().await
}
```

To store in InfluxDB instead, change only the provider line:
`InfluxProvider::new(ClientConfig::from_env()?)`. The descriptor and collection calls
stay the same.

| | InfluxDB 3 | Arrow captures |
| --- | --- | --- |
| Writes | `create` and `create_many`; nanosecond precision, synchronized WAL, at most 10,000 points per request | Append-only IPC segments under `<root>/<database>/<table>/`, rotated at 1 GiB or 15 minutes |
| Reads | `list`, `count`, `series.range` and `series.aggregate`; filters with the shared subset | The same, skipping segments outside a range by their `.bounds` sidecar |
| Refused | `get`, `update`, `delete`, `exists`, transactions | The same |
| Paging | Keyset tokens bound to the host, database, descriptor and query; `total` is -1 | Offset tokens, with totals |
| Databases | `InfluxAdmin` creates them, with retention fixed at creation, and drops them | `create_database` makes the directory |
| Dropping a table | `schema.drop_schema` removes it and every point in it | Removes its directory |

- **Page size** is at most 10,000 in this contract.
- **Large windows:** a time window too large for pages streams through `db.streams`.
- **Generated models:** generated code implements `store::Model`, and
  `For::<T>::new(&db)` binds it, as runtime-go's `For[T]`.

**Sustained writes**
- `database::buffered(db, config)` queues each create into bounded background batches
  and returns `pending: true`, runtime-go's signal for "accepted, not yet durable".
  Queued points live in memory, so call `close` before exit; it drains them.
- `database::mirror(primary, copy)` writes to both stores at once and fails a write
  if either side fails. Reads use the primary.

```rust,ignore
let db = database::buffered(database::mirror(influx, arrow), IngestConfig { max_inflight: 8, ..Default::default() })?;
```

**Throughput:** a durable InfluxDB write waits for the server's WAL flush, about 1 s.
So throughput scales with the batches in flight. Against Core 3.12.2 with 5,000-point
batches:

| Batches in flight | Points per second |
| --- | --- |
| 1 | ~7k |
| 4 | ~20k |
| 8 | ~40k |

A 60 s capture at 22,000 points/s stored all 1,320,220 points in both InfluxDB and
Arrow.

**InfluxDB Core caveats:**
- Core has no selective row deletion.
- This crate never sends Enterprise's `data_only` flag, because Core ignores it and
  drops the table.

The [influxdb example](examples/influxdb/README.md) runs the whole workflow in a
disposable database, and the [capture example](examples/capture/README.md) records
22k/s for 15 minutes into both InfluxDB and Arrow.

## Telemetry

`with_telemetry(signals)` on a provider, or `store::instrument(db, signals)`, reports
every capability call. It reports no names, keys, filters or raw errors, and is a
no-op by default. See [runtime telemetry](../telemetry/README.md).

## Testing

```bash
cargo test -p tpp-database                                    # contracts, Arrow, InfluxDB mocks
cargo test -p tpp-database --features orm                     # SQLite, in process
cargo test -p tpp-database --features tauri-store-test --test tauri_store   # Tauri's mock runtime
cargo test -p tpp-database --features orm --test repository_sqlite          # repositories, in process
cargo test -p tpp-database --features tauri-store-test --test repository_tauri
```

The live suites need a server. Each has its own Compose file on a high port, so they
can run beside services you already have on the default ports:

| Suite | Start | Run |
| --- | --- | --- |
| SQL | `docker compose -f docker/compose.orm.live.yaml up -d --wait` (Postgres 15432, MySQL 13306) | `cargo test -p tpp-database --features orm,live-tests --test orm_live` |
| MongoDB | `docker compose -f docker/compose.mongodb.live.yaml up -d --wait` (a single-node replica set on 27117) | `cargo test -p tpp-database --features mongodb,live-tests --test mongodb_live` |
| Redis | `docker compose -f docker/compose.redis.live.yaml up -d --wait` (16381) | `cargo test -p tpp-database --features redis,live-tests --test redis_live` |
| Cache | the cache's Redis, `../cache/docker/compose.live.yaml` (16379) | `cargo test -p tpp-database --features cached,live-tests --test cached_live` |
| Repositories | the SQL, MongoDB and Redis files above | `cargo test -p tpp-database --features orm,mongodb,redis,live-tests --test repository_sql --test repository_mongodb --test repository_redis` |

With `live-tests` on, an unreachable server fails the suite rather than skipping it,
so a passing run means the provider was really exercised. CI runs every suite.

More detail: [source layout](src/README.md), [time-series contracts](src/store/README.md),
[InfluxDB](src/influxdb/README.md), [Arrow](src/arrow/README.md),
[ingestion](src/ingest/README.md), [observations](src/observe/README.md),
[test layout](tests/README.md) and [docker](docker/README.md).
