# runtime-rs

[![CI](https://github.com/the-protobuf-project/runtime-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/the-protobuf-project/runtime-rs/actions/workflows/ci.yml)
[![docs](https://github.com/the-protobuf-project/runtime-rs/actions/workflows/docs.yml/badge.svg)](https://the-protobuf-project.github.io/runtime-rs/network/)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A comprehensive Rust workspace providing unified GraphQL, HTTP, and WebSocket clients, plus
predicate/filter helpers for generated CRUD clients — a Rust port of
[`runtime-go/network`](https://github.com/the-protobuf-project/runtime-go/tree/main/network),
built the Rust way rather than as a line-for-line translation. See
[Differences from the Go implementation](#differences-from-the-go-implementation) for exactly
where and why this diverges from the original.

## Features

- **HTTP client** — GET/POST/PUT/PATCH/DELETE with per-attempt timeouts, configurable retries,
  and cooperative cancellation.
- **GraphQL client** — raw queries, typed queries/mutations, dynamic field-based operations,
  transactional batched mutations, and live `graphql-transport-ws` subscriptions.
- **WebSocket client** — send/receive, ping/pong keepalive, auto-reconnect, and a cancellable
  message stream.
- **One shape, three clients** — [`HttpClient`], [`GraphQLClient`], and [`WebSocketClient`] are
  each constructed with [`Default`] and configured with a single [`ConnectionOptions`] struct
  shared across all of them; no factory or common wrapper type stands between you and the client
  you want.
- **Connectivity verification** — `connect` checks the target is reachable before returning
  (opt-out per connection), so failures surface immediately instead of on the first real request.
- **Async-native** — every operation is a plain `async fn` returning `Result<T, Error>` on
  `tokio`; no callback or channel plumbing.
- **Distributed tracing** — an optional `opentelemetry` `TextMapPropagator` injects the active
  span into every outgoing HTTP/GraphQL request.
- **CRUD helpers** ([`network-graphql`](network-graphql)) — `where`-clause predicate building,
  typed column handles, a three-state `Nullable<T>` for masked updates, and cursor pagination, for
  whatever code generator emits your resource types.

## Workspace layout

Three crates, mirroring the three Go packages 1:1:

| Crate | Published as | Go source | Purpose |
|---|---|---|---|
| [`network`](network) | `tpp-network` | `runtime-go/network` | Core clients: HTTP, WebSocket, GraphQL |
| [`network-graphql`](network-graphql) | `tpp-network-graphql` | `runtime-go/network/graphql` | Predicate/filter/query-builder helpers — independent of `network` |
| [`runtime`](runtime) | `tpp-runtime` | `runtime-go/network/runtime` | Stable facade re-exporting `network`'s surface, plus `Tx` (atomic batched mutations) |

`runtime` depends on `network`; `network-graphql` depends on neither, matching the Go dependency
graph. Each crate is imported under its short name (`network`, `network_graphql`, `runtime`)
regardless of its published package name — see the `[lib] name` override in each `Cargo.toml`.

## Installation

```bash
cargo add tpp-network        # HTTP, GraphQL, WebSocket clients
cargo add tpp-network-graphql # CRUD predicate/filter helpers (optional, standalone)
cargo add tpp-runtime         # stable facade + Tx (optional, depends on tpp-network)
```

## Quick start

### HTTP

```rust,no_run
use network::{ConnectionOptions, HttpClient, HttpMethod, UrlOptions, UrlScheme};

# async fn example() -> network::Result<()> {
let url = UrlOptions {
    scheme: UrlScheme::Https,
    host: "api.example.com".into(),
    paths: vec!["/users".into()],
    params: Default::default(),
};
let mut http = HttpClient::default();
http.connect(ConnectionOptions {
    url: url.clone(),
    headers: [("Authorization".into(), "Bearer token".into())].into(),
    ..Default::default()
}).await?;

let body = http.request(HttpMethod::Get, &url, Vec::new(), &Default::default(), 0, 3, None).await?;
# Ok(())
# }
```

### GraphQL

```rust,no_run
use network::{ConnectionOptions, GraphQLClient, UrlOptions, UrlScheme};
use serde::Deserialize;

# async fn example() -> network::Result<()> {
let mut gql = GraphQLClient::default();
gql.connect(ConnectionOptions {
    url: UrlOptions {
        scheme: UrlScheme::Https,
        host: "api.example.com".into(),
        paths: vec!["/graphql".into()],
        params: Default::default(),
    },
    ..Default::default()
}).await?;

#[derive(Deserialize)]
struct Data { user: User }
#[derive(Deserialize)]
struct User { id: String, name: String }

let data: Data = gql.query(r#"query { user(id: "123") { id name } }"#, None).await?;
# Ok(())
# }
```

### WebSocket

```rust,no_run
use network::{ConnectionOptions, Message, UrlOptions, UrlScheme, WebSocketClient};
use std::time::Duration;

# async fn example() -> network::Result<()> {
let ws = WebSocketClient::default();
ws.connect(ConnectionOptions {
    url: UrlOptions {
        scheme: UrlScheme::Wss,
        host: "ws.example.com".into(),
        paths: vec!["/ws".into()],
        params: Default::default(),
    },
    ..Default::default()
}).await?;

ws.set_auto_reconnect(true, Some(Duration::from_secs(5))).await;
let mut updates = ws.listen(None);
ws.send(Message::Text("hello".into())).await?;
while let Some(msg) = updates.recv().await {
    match msg {
        Ok(m) => println!("received: {m:?}"),
        Err(e) => { eprintln!("connection error: {e}"); break; }
    }
}
# Ok(())
# }
```

## Configuration

[`ConnectionOptions`], shared by all three client types:

| Field | Type | Meaning |
|---|---|---|
| `url` | `UrlOptions` | Scheme, host, candidate paths, query params |
| `timeout` | `Duration` | Connection + per-request timeout; zero uses `DEFAULT_TIMEOUT` (10s) |
| `headers` | `HashMap<String, String>` | Sent on every request / the WebSocket handshake |
| `retries` | `usize` | Max retries for `HttpClient::request` |
| `retry_delay` | `Duration` | Pause between retries; zero uses a 2s default |
| `skip_connectivity_check` | `bool` | Skip the initial reachability check (see below) |
| `graphql_connectivity_query` | `Option<String>` | Override the GraphQL reachability query |
| `trace_propagator` | `Option<Arc<dyn TextMapPropagator + Send + Sync>>` | Injects the active span into outgoing requests when set |

### Connectivity verification

By default, each client's `connect` verifies the target is reachable before returning:

- **HTTP** — sends a `HEAD` request; if the server returns `405 Method Not Allowed`, a `GET` is
  sent instead and its body is drained (up to 1 MiB) so the connection can be reused.
- **GraphQL** — sends a small introspection query (`query { __typename }` by default, or
  `graphql_connectivity_query` if set).
- **WebSocket** — the handshake itself is the check; `skip_connectivity_check` is ignored.

Set `skip_connectivity_check: true` to skip the extra round trip when you know the server is up.

### URL options

```rust
use network::{UrlOptions, UrlScheme};

let opts = UrlOptions {
    scheme: UrlScheme::Https,      // Http, Https, Ws, or Wss
    host: "example.com:8443".into(), // may include a port
    paths: vec!["/v1".into(), "/v2".into()], // a client selects one by index
    params: Default::default(),     // query parameters
};
```

## Error handling

Every fallible operation returns `network::Result<T>` (`Result<T, network::Error>`), one flat
error enum whose `Display` mirrors Go's error strings and whose `source()` chain walks the same
causal path Go's `errors.Unwrap` does:

```rust,no_run
# use network::{GraphQLClient, Error};
# async fn example(gql: &GraphQLClient) {
match gql.query::<serde_json::Value>("query { user(id: \"1\") { name } }", None).await {
    Ok(data) => println!("{data}"),
    Err(Error::GraphQLErrors(errors)) => eprintln!("server returned errors: {errors:?}"),
    Err(Error::RetryExhausted { retries, source }) => {
        eprintln!("gave up after {retries} retries: {source}")
    }
    Err(e) => eprintln!("request failed: {e}"),
}
# }
```

## Advanced features

### Cancellation

Every retrying/long-lived operation (`HttpClient::request`, `WebSocketClient::listen`,
`GraphQLClient::subscribe_fields`) takes an optional
[`tokio_util::sync::CancellationToken`](https://docs.rs/tokio-util) instead of Go's
`context.Context`:

```rust,no_run
use tokio_util::sync::CancellationToken;
# use network::{HttpClient, HttpMethod, UrlOptions};
# async fn example(http: &HttpClient, url: &UrlOptions) {
let cancel = CancellationToken::new();
let handle = cancel.clone();
tokio::spawn(async move {
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    handle.cancel();
});
let _ = http.request(HttpMethod::Get, url, Vec::new(), &Default::default(), 0, 0, Some(&cancel)).await;
# }
```

### WebSocket auto-reconnect

```rust,no_run
# use network::WebSocketClient;
# async fn example(ws: &WebSocketClient) {
ws.set_auto_reconnect(true, Some(std::time::Duration::from_secs(5))).await;
// listen() now reconnects (after the configured delay) on a read error instead of terminating.
# }
```

### Atomic batched mutations (`tpp-runtime`)

```rust,no_run
use runtime::{BatchOp, Tx};

# async fn example(gql: &runtime::GraphQLClient) -> runtime::Result<()> {
let mut tx = Tx::new(gql);
tx.add(BatchOp { field: "insertUser".into(), args: vec![], selection: "{ id }".into() });
tx.add(BatchOp { field: "insertPost".into(), args: vec![], selection: "{ id }".into() });
let results = tx.commit().await?; // all-or-nothing: one GraphQL transaction
# Ok(())
# }
```

## Examples

Complete, runnable programs against **live public services** (no local server needed):

```bash
cargo run -p tpp-network --example rickandmorty_graphql  # https://rickandmortyapi.com/graphql
cargo run -p tpp-network --example httpbin_http           # an httpbin-compatible HTTP service
cargo run -p tpp-network --example websocket_echo         # a public WebSocket echo service
cargo run -p tpp-network-graphql --example basic_usage    # predicate + masked-update patch
```

## Testing

```bash
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```

HTTP and GraphQL tests use [`wiremock`](https://docs.rs/wiremock); WebSocket and GraphQL
subscription tests spin up a real, hand-rolled `tokio-tungstenite` server in-process (no mocking
of the transport layer itself).

## Architecture

### Design patterns

- **Direct construction, no factory** — [`HttpClient`], [`GraphQLClient`], and
  [`WebSocketClient`] are built with [`Default`] and configured with `connect`; there's no common
  wrapper type or enum standing between construction and use, since the client type you want is
  always known at the call site (see [Differences from the Go implementation](#differences-from-the-go-implementation)
  for why Go needed one and Rust doesn't).
- **Interior mutability for shared handles** — `WebSocketClient` and `GraphQLClient`'s live
  connection state sits behind `tokio::sync` locks so a client can be cloned cheaply and driven
  concurrently (e.g. a background `listen` task alongside foreground `send`s).
- **One error type per crate** — a single `thiserror` enum per crate rather than a taxonomy of
  per-domain error types, mirroring Go's single untyped `error` return.

## Differences from the Go implementation

Go's `network` package leans on `hasura/go-graphql-client`'s runtime reflection (over Go struct
tags) to build GraphQL query documents and decode responses, and on `reflect` elsewhere (e.g.
`SetColumns`). Rust has no runtime reflection, and this port deliberately avoids introducing a
proc-macro system to replace it. The consequences, by area:

- **Typed GraphQL operations** (`GraphQLClient::query`/`mutation`) take an explicit,
  caller-supplied query/mutation string plus a `T: DeserializeOwned` target type, instead of
  inferring the query shape from a tagged struct. Since this workspace's own code generator knows
  the schema at generation time, it can emit the full query text directly — reflection was never
  strictly necessary for that use case.
- **`QueryFields`/`MutateFields`/`BatchMutate`** keep their field-name + arguments ergonomics, but
  each argument is a [`FieldArg`](network/src/graphql/named.rs) `{ name, value, gql_type }` triple
  instead of a bare `map[string]interface{}` — Go's variable *types* in the operation signature
  were inferred by `go-graphql-client`'s reflection; Rust needs them supplied explicitly. The
  leaf selection set is likewise a caller-supplied string rather than inferred from a result
  struct's shape.
- **`BatchMutate`/`Tx::commit`** return the decoded per-op results directly
  (`Vec<serde_json::Value>`, in input order) instead of writing into pre-registered result
  pointers via reflection — Rust's type system can't represent "a `Vec` of ops with per-op
  distinct result types" the way Go's `interface{}` can.
- **`network::SetColumns`** becomes the [`ColumnPatch`](network-graphql/src/columns.rs) trait:
  generated patch structs implement `set_columns()` by hand (with `Nullable::to_set_entry()` doing
  the heavy lifting per field) instead of a single generic function walking arbitrary struct
  fields via `reflect`.
- **GraphQL scalar wrapper types** (`Boolean`/`Float`/`Int`/`String` in Go) existed only to drive
  `go-graphql-client`'s type inference and are not ported — use `bool`/`f64`/`i32`/`String`
  directly. Only `Id` (GraphQL's `ID` scalar, which always serializes as a string) is kept.
- **Scalar/argument pointer-constructor helpers** (Go's `graphql.String`, `graphql.Bool`, `Ptr`,
  etc.) are dropped — `Option<T>`/`Some(v)` already does that job natively in Rust.
- **GraphQL subscriptions** (`graphql-transport-ws`) are hand-rolled on `tokio-tungstenite`
  (see [`network/src/graphql/subscription.rs`](network/src/graphql/subscription.rs) and
  [`subscription_task.rs`](network/src/graphql/subscription_task.rs)) rather than wrapping an
  external GraphQL-WS client crate, keeping this workspace's dependency footprint and protocol
  behavior fully in its own control.
- **No `Client` interface, no factory** — Go's `Client` interface plus `NewConnection` factory
  and `AsGraphQLConnectionType`-style type assertions exist because Go has no other way for one
  constructor to return "any of three concrete types" and let the caller recover the concrete one
  afterward. That's not a runtime need here: the client type is a compile-time constant at every
  real call site in both languages (Go's own generated code always calls
  `NewConnection(GraphQLConnClient)` with a literal constant). So this port skips the indirection
  entirely — `HttpClient`, `GraphQLClient`, and `WebSocketClient` are constructed directly via
  `Default`, with no wrapper type, no enum, and no `TypeCast` error variant to encounter.
- **Async model**: Go's goroutine-plus-channel pattern (`<-chan HTTPResponse`, `<-chan
  GraphQLResult`) becomes plain `async fn` returning `Result<T, Error>` throughout. Cancellation,
  where Go used `context.Context`, is an explicit `Option<&tokio_util::sync::CancellationToken>`
  parameter.
- **Naming**: Rust keywords force a few renames — Go's `Where`/`.Where()` → `where_`/`.where_()`;
  `In(...)` → `is_in(...)`. Go's exported `PascalCase` free functions (`And`, `Or`, `Not`,
  `Relation`) become Rust's conventional `snake_case` (`and`, `or`, `not`, `relation`).

Everything else — connectivity-check semantics, retry/backoff behavior, ping/pong keepalive,
auto-reconnect, and the `BatchMutate`/`SetColumns` argument-namespacing and
omit/null/value-distinction semantics — is a faithful, tested port.

## Continuous integration

Every push and pull request runs, via [`.github/workflows/ci.yml`](.github/workflows/ci.yml):

- **test** — `cargo test` per crate, plus the full workspace
- **lint** — `cargo clippy --all-targets -- -D warnings` and `cargo fmt --check`
- **docs** — `cargo doc --no-deps` with warnings promoted to errors (catches missing docs and
  broken intra-doc links)
- **msrv** — a build against the pinned minimum Rust version (`rust-version` in the workspace
  manifest)
- **audit** — [`cargo-audit`](https://github.com/rustsec/rustsec) against the RustSec advisory
  database

Rendered API docs are published to GitHub Pages on every push to `main` (see
[`.github/workflows/docs.yml`](.github/workflows/docs.yml)). Tagged releases get a GitHub Release
with an auto-generated changelog (see
[`.github/workflows/release.yml`](.github/workflows/release.yml)); publishing to crates.io is a
separate, manually-triggered workflow (see
[`.github/workflows/publish.yml`](.github/workflows/publish.yml)) — nothing publishes
automatically.

## License

Apache-2.0 — see [LICENSE](LICENSE).

[`HttpClient`]: network/src/http/mod.rs
[`GraphQLClient`]: network/src/graphql/mod.rs
[`WebSocketClient`]: network/src/websocket/mod.rs
[`ConnectionOptions`]: network/src/options.rs
[`Default`]: https://doc.rust-lang.org/std/default/trait.Default.html
