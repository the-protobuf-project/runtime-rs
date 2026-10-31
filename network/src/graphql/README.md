# GraphQL queries, mutations, and subscriptions

[Network guide](../../README.md)

The client uses pooled HTTP for operations and a separate graphql-transport-ws
connection for subscriptions. Connect once, select the operation shape that
matches your schema, deserialize the returned `data`, then close.

```rust
use network::{ConnectionOptions, GraphQLClient, UrlOptions, UrlScheme};
use serde::Deserialize;

#[derive(Deserialize)]
struct Data { character: Character }
#[derive(Deserialize)]
struct Character { name: String }

#[tokio::main]
async fn main() -> network::Result<()> {
    let mut client = GraphQLClient::default();
    client.connect(ConnectionOptions {
        url: UrlOptions {
            scheme: UrlScheme::Https, host: "rickandmortyapi.com".into(),
            paths: vec!["/graphql".into()], ..Default::default()
        }, ..Default::default()
    }).await?;
    let variables = serde_json::json!({"id": "1"});
    let data: Data = client.query(
        "query($id: ID!) { character(id: $id) { name } }", Some(&variables)
    ).await?;
    println!("{}", data.character.name);
    client.close().await?;
    Ok(())
}
```

This example makes real network requests. `mod.rs` holds client state and the
connectivity query. `raw.rs` sends operation documents/variables and extracts
data or GraphQL errors. `typed.rs` deserializes data for `query` / `mutation`
and supports struct-based inputs. `named.rs` builds field operations with
explicit GraphQL argument types; `helpers.rs` provides conversion/scalar tools.

`batch.rs` aliases operations (`m0`, `m1`, ...) and namespaces variables into
one mutation document, returning results in input order. Transactional
commit/rollback across those fields depends on the server's execution model;
this client performs no transaction protocol or rollback itself.

`subscription.rs` opens a WebSocket, sends connection initialization, waits
for acknowledgment under the connection timeout, then subscribes. The task in
`subscription_task.rs` decodes `next`, responds to protocol ping, and delivers
results over a bounded channel. Completion, socket failure, or cancellation
ends the subscription. `ws_protocol.rs` contains the message framing. This is
separate from the generic WebSocket client's reconnect configuration.

```bash
cargo test -p tpp-network --test graphql --test subscription
cargo run -p tpp-network --example rickandmorty_graphql
```

The tests use controlled local HTTP/WebSocket peers; external availability is
required only for the example.
