# runtime-rs

A Cargo workspace providing GraphQL, HTTP, and WebSocket clients, plus generated-CRUD-client
helpers — a Rust port of [`runtime-go/network`](../runtime-go/network), built the Rust way rather
than a line-for-line translation.

## Workspace layout

Three crates, mirroring the three Go packages 1:1:

| Crate | Go source | Purpose |
|---|---|---|
| [`network`](network) | `runtime-go/network` | Core clients: `Network` factory, HTTP, WebSocket, GraphQL (queries, mutations, batches, subscriptions) |
| [`network-graphql`](network-graphql) | `runtime-go/network/graphql` | Predicate/filter/query-builder helpers for generated CRUD clients — independent of `network` |
| [`runtime`](runtime) | `runtime-go/network/runtime` | Stable facade re-exporting `network`'s public surface, plus `Tx` (atomic batched mutations) |

`runtime` depends on `network`; `network-graphql` depends on neither, matching the Go dependency
graph.

## Quick start

### HTTP

```rust
use network::{ClientType, ConnectionOptions, HttpMethod, Network, UrlOptions, UrlScheme};

let url = UrlOptions {
    scheme: UrlScheme::Https,
    host: "api.example.com".into(),
    paths: vec!["/users".into()],
    params: Default::default(),
};
let mut conn = Network::new_connection(ClientType::Http)?;
conn.with_opts(ConnectionOptions {
    url: url.clone(),
    headers: [("Authorization".into(), "Bearer token".into())].into(),
    ..Default::default()
}).await?;

let http = conn.as_http()?;
let body = http.request(HttpMethod::Get, &url, Vec::new(), &Default::default(), 0, 3, None).await?;
```

### GraphQL

```rust
use network::{ClientType, ConnectionOptions, Network, UrlOptions, UrlScheme};
use serde::Deserialize;

let mut conn = Network::new_connection(ClientType::GraphQL)?;
conn.with_opts(ConnectionOptions {
    url: UrlOptions { scheme: UrlScheme::Https, host: "api.example.com".into(), paths: vec!["/graphql".into()], params: Default::default() },
    ..Default::default()
}).await?;
let gql = conn.as_graphql()?;

#[derive(Deserialize)]
struct Data { user: User }
#[derive(Deserialize)]
struct User { id: String, name: String }

let data: Data = gql.query(r#"query { user(id: "123") { id name } }"#, None).await?;
```

### WebSocket

```rust
use network::{ClientType, ConnectionOptions, Message, Network, UrlOptions, UrlScheme};
use std::time::Duration;

let mut conn = Network::new_connection(ClientType::WebSocket)?;
conn.with_opts(ConnectionOptions {
    url: UrlOptions { scheme: UrlScheme::Wss, host: "ws.example.com".into(), paths: vec!["/ws".into()], params: Default::default() },
    ..Default::default()
}).await?;
let ws = conn.as_websocket()?;

ws.set_auto_reconnect(true, Some(Duration::from_secs(5))).await;
let mut updates = ws.listen(None);
ws.send(Message::Text("hello".into())).await?;
while let Some(msg) = updates.recv().await {
    match msg {
        Ok(m) => println!("received: {m:?}"),
        Err(e) => { eprintln!("connection error: {e}"); break; }
    }
}
```

## Configuration

`ConnectionOptions` (shared by all three client types):

- `url: UrlOptions` — scheme, host, paths, query params.
- `timeout: Duration` — connection + per-request timeout; zero uses `DEFAULT_TIMEOUT` (10s).
- `headers: HashMap<String, String>` — sent on every request / the WebSocket handshake.
- `retries: usize`, `retry_delay: Duration` — HTTP retry policy (default delay 2s).
- `skip_connectivity_check: bool` — skip the initial HTTP/GraphQL reachability check. Ignored for
  WebSocket (the handshake itself is the check).
- `graphql_connectivity_query: Option<String>` — override the GraphQL reachability query.
- `trace_propagator: Option<Arc<dyn opentelemetry::propagation::TextMapPropagator + Send + Sync>>`
  — when set, injects the active span into outgoing HTTP/GraphQL headers.

## Testing

```bash
cargo test --workspace
cargo clippy --workspace --all-targets
```

HTTP and GraphQL tests use [`wiremock`](https://docs.rs/wiremock); WebSocket and GraphQL
subscription tests spin up a real, hand-rolled `tokio-tungstenite` server in-process.

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
  [`ws_protocol.rs`](network/src/graphql/subscription.rs)) rather than wrapping an external
  GraphQL-WS client crate, keeping this workspace's dependency footprint and protocol behavior
  fully in its own control.
- **The `Client` interface** becomes a closed `NetworkClient` enum (`GraphQL`/`Http`/`WebSocket`)
  instead of a `Box<dyn Trait>` — there are exactly three implementations, known at compile time,
  so enum dispatch avoids both `async-trait` boxing and the async-fn-in-dyn-trait problem while
  giving exhaustive-match compile-time coverage Go's type switch can't.
- **Async model**: Go's goroutine-plus-channel pattern (`<-chan HTTPResponse`, `<-chan
  GraphQLResult`) becomes plain `async fn` returning `Result<T, Error>` throughout. Cancellation,
  where Go used `context.Context`, is an explicit `Option<&tokio_util::sync::CancellationToken>`
  parameter.
- **Naming**: Rust keywords force a few renames — Go's `Where`/`.Where()` → `where_`/`.where_()`;
  `In(...)` → `is_in(...)`. Go's exported `PascalCase` free functions (`And`, `Or`, `Not`,
  `Relation`) become Rust's conventional `snake_case` (`and`, `or`, `not`, `relation`).

Everything else — connectivity-check semantics (HEAD→GET-on-405 for HTTP, an introspection query
for GraphQL, dial-is-the-check for WebSocket), retry/backoff behavior, ping/pong keepalive,
auto-reconnect, and the `BatchMutate`/`SetColumns` argument-namespacing and
omit/null/value-distinction semantics — is a faithful, tested port.
