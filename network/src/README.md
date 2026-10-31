# Network client construction and lifecycle

[Crate guide](../README.md) · [Examples](../examples/README.md)

`lib.rs` exports the three clients plus the factory/options/error vocabulary.
`network.rs` stores a concrete `Client` inside `Network`, records connection
options, and dispatches connect/reconnect/close to the selected implementation.

```rust
use network::{ClientType, ConnectionOptions, Network, UrlOptions, UrlScheme};

#[tokio::main]
async fn main() -> network::Result<()> {
    let mut connection = Network::new_connection(ClientType::Http)?;
    connection.with_opts(ConnectionOptions {
        url: UrlOptions { scheme: UrlScheme::Http, host: "127.0.0.1:8080".into(), ..Default::default() },
        skip_connectivity_check: true,
        ..Default::default()
    }).await?;
    let _http = connection.as_http_connection_type()?;
    connection.close().await?;
    Ok(())
}
```

The factory selects a type, not an active connection. `with_opts` configures and
connects; casting to the wrong concrete client returns an error. Direct client
construction with `Default` and `connect` is equally supported.

`options.rs` defines URL parts, headers, retry delay, optional propagation, and
the ten-second default timeout. `url.rs` validates/builds indexed paths and
converts HTTP schemes for WebSocket use. `transport.rs` creates independent
reqwest pools with up to 100 idle connections per host and a header injection
adapter. `error.rs` preserves lifecycle/transport/decode classifications.

Protocol execution lives in [HTTP](http/README.md), [GraphQL](graphql/README.md),
and [WebSocket](websocket/README.md). HTTP/GraphQL connectivity checks can be
skipped; WebSocket still must complete a handshake. Closing releases local
client resources; reconnect uses stored options. The library returns errors
and leaves logging/subscriber ownership to the application.

```bash
cargo test -p tpp-network --test network --test transport
```
