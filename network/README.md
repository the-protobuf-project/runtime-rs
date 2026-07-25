# tpp-network

GraphQL, HTTP, and WebSocket clients behind a single factory ([`Network::new_connection`]) with
consistent connection options and optional connectivity verification. A Rust port of
[`runtime-go/network`](https://github.com/the-protobuf-project/runtime-go/tree/main/network),
built the Rust way — see the
[workspace README](https://github.com/the-protobuf-project/runtime-rs#differences-from-the-go-implementation)
for the full list of deliberate deviations from the Go original.

## Install

```bash
cargo add tpp-network
```

The library crate is imported as `network` regardless of the published package name — see
`[lib] name` in `Cargo.toml`.

## Quick start

```rust,no_run
use network::{ClientType, ConnectionOptions, Network, UrlOptions, UrlScheme};
use serde::Deserialize;

# async fn example() -> network::Result<()> {
let mut conn = Network::new_connection(ClientType::GraphQL)?;
conn.with_opts(ConnectionOptions {
    url: UrlOptions {
        scheme: UrlScheme::Https,
        host: "rickandmortyapi.com".into(),
        paths: vec!["/graphql".into()],
        params: Default::default(),
    },
    ..Default::default()
}).await?;
let gql = conn.as_graphql()?;

#[derive(Deserialize)]
struct Data { character: Character }
#[derive(Deserialize)]
struct Character { name: String, status: String }

let data: Data = gql.query("query { character(id: 1) { name status } }", None).await?;
println!("{} is {}", data.character.name, data.character.status);
# Ok(())
# }
```

HTTP and WebSocket clients follow the same `Network::new_connection` → `with_opts` → `as_http` /
`as_websocket` shape. See [`examples/`](examples) for complete, runnable programs against live
public services (Rick and Morty's GraphQL API, an httpbin-compatible HTTP service, and a
WebSocket echo server):

```bash
cargo run --example rickandmorty_graphql
cargo run --example httpbin_http
cargo run --example websocket_echo
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
