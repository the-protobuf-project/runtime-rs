# HTTP, GraphQL, and WebSocket walkthroughs

[Network guide](../README.md)

These complete Tokio programs use public services with no embedded credentials.
Run from the repository root:

```bash
cargo run -p tpp-network --example httpbin_http
cargo run -p tpp-network --example rickandmorty_graphql
cargo run -p tpp-network --example websocket_echo
```

`httpbin_http.rs` connects to the httpbin-compatible `httpbingo.org`, then
performs verb/query/body operations. It supplies a User-Agent for example
requests and demonstrates error/cancellation behavior. `rickandmorty_graphql.rs`
connects to the Rick and Morty GraphQL endpoint and exercises typed/field-based
queries and helpers. `websocket_echo.rs` opens `wss://echo.websocket.org`,
receives the greeting, sends/receives echo frames, then demonstrates listening
and auto-reconnect configuration.

Each follows configure -> connect/probe -> operations -> close. Copy the
configuration and call shapes into your application, replace public endpoints
with your own, and choose timeouts/retries for your write semantics. Public
service availability or responses may change; these examples are not the
controlled correctness suite.

```bash
cargo test -p tpp-network
```

The tests use local HTTP/WebSocket peers and need no public service access.
