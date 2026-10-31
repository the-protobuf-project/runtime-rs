# tpp-network

GraphQL, HTTP, and WebSocket clients, each constructed directly and configured with consistent
connection options and optional connectivity verification. A Rust port of
[`runtime-go/network`](https://github.com/the-protobuf-project/runtime-go/tree/main/network),
built around async clients and explicit lifecycle/error handling.

## Install

```bash
cargo add tpp-network
```

The library crate is imported as `network` regardless of the published package name — see
`[lib] name` in `Cargo.toml`.

## Quick start

```rust,no_run
use network::{ConnectionOptions, GraphQLClient, UrlOptions, UrlScheme};
use serde::Deserialize;

# async fn example() -> network::Result<()> {
let mut gql = GraphQLClient::default();
gql.connect(ConnectionOptions {
    url: UrlOptions {
        scheme: UrlScheme::Https,
        host: "rickandmortyapi.com".into(),
        paths: vec!["/graphql".into()],
        params: Default::default(),
    },
    ..Default::default()
}).await?;

#[derive(Deserialize)]
struct Data { character: Character }
#[derive(Deserialize)]
struct Character { name: String, status: String }

let data: Data = gql.query("query { character(id: 1) { name status } }", None).await?;
println!("{} is {}", data.character.name, data.character.status);
# Ok(())
# }
```

`HttpClient` and `WebSocketClient` follow the same shape: build with `Default`, then `connect`.
See [`examples/`](examples) for complete, runnable programs against live public services (Rick
and Morty's GraphQL API, an httpbin-compatible HTTP service, and a WebSocket echo server):

```bash
cargo run -p tpp-network --example rickandmorty_graphql
cargo run -p tpp-network --example httpbin_http
cargo run -p tpp-network --example websocket_echo
```

## What's here

- **HTTP** (`HttpClient`): GET/POST/PUT/PATCH/DELETE with per-attempt timeouts, retries, and
  cancellation via `tokio_util::sync::CancellationToken`.
- **WebSocket** (`WebSocketClient`): send/receive, ping/pong keepalive, auto-reconnect, and a
  cancellable `listen` stream.
- **GraphQL** (`GraphQLClient`): raw queries, typed queries/mutations (explicit query string +
  `serde` deserialization), dynamic field-based operations, transactional batched mutations, and
  `graphql-transport-ws` subscriptions.

Full crate documentation: `cargo doc --open -p tpp-network` (or [docs.rs](https://docs.rs/tpp-network)
once published).

## License

Apache-2.0

## Execution and shutdown

Connection settings -> URL validation -> independent pooled client or WebSocket
handshake -> optional HTTP/GraphQL reachability check -> operations -> close.
The factory and direct constructors share this flow. Reconnect uses remembered
options; errors return to the caller. The ten-second default is applied when
the configured timeout is zero. Retry counts and cancellation are explicit on
HTTP request calls; generic WebSocket listening can opt into reconnect.

Use [HTTP](src/http/README.md), [GraphQL](src/graphql/README.md), and
[WebSocket](src/websocket/README.md) for complete copyable programs, method
semantics, and failure behavior. [Source map](src/README.md) explains common
options/factory/pooling, while [tests](tests/README.md) describes controlled
local verification and [examples](examples/README.md) describes live programs.

```bash
cargo test -p tpp-network
cargo doc -p tpp-network --no-deps
```
