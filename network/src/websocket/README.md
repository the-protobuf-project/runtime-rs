# Full-duplex WebSocket client

[Network guide](../../README.md)

`WebSocketClient` is cloneable shared state around a split socket. Connect to a
`ws`/`wss` endpoint, send frames, receive or listen, then close explicitly.

```rust
use network::{ConnectionOptions, Message, UrlOptions, UrlScheme, WebSocketClient};

#[tokio::main]
async fn main() -> network::Result<()> {
    let mut client = WebSocketClient::default();
    client.connect(ConnectionOptions {
        url: UrlOptions { scheme: UrlScheme::Ws, host: "127.0.0.1:9001".into(), ..Default::default() },
        ..Default::default()
    }).await?;
    client.send(Message::Text("hello".into())).await?;
    println!("{:?}", client.receive().await?);
    client.close().await?;
    Ok(())
}
```

This needs a local echo peer on port 9001. For the repository's public echo
walkthrough, run `cargo run -p tpp-network --example websocket_echo`.

`mod.rs` manages connection state, handshake headers, keepalive, reconnect, and
closing. `io.rs` implements send/receive, bounded retry-send, and `listen`.
Receive answers Ping with Pong, ignores Pong, and returns application frames;
a closed socket is an error. Socket halves are separately locked.

`listen` forwards results on a bounded channel. With auto-reconnect enabled,
a receive failure waits the reconnect delay, reconnects with saved options,
and continues. Otherwise it sends a terminal error and closes the channel.
External cancellation terminates listening. `retry_send` is an explicit
send-attempt loop; reconnect does not provide durable message replay or
exactly-once delivery. Keep one logical reader per connection when order matters.

```bash
cargo test -p tpp-network --test websocket
```

These tests start local echo peers and verify frames, close, reconnect, and
cancellation without a public server.
