# Network client verification

[Network guide](../README.md) · [Implementation](../src/README.md)

```bash
cargo test -p tpp-network
cargo test -p tpp-network --test http --test graphql --test websocket --test subscription
```

`network.rs` checks factory selection, casts, recorded options, and uniform
lifecycle dispatch. `http.rs` uses controlled HTTP responses for connectivity
fallback, skip-probe, retries, cancellation, result/error packaging, and
propagation behavior. `graphql.rs` checks connectivity queries, typed decoding,
errors, argument ordering, and batch aliases/variables.

`websocket.rs` starts local echo peers to check frames, close, reconnect, and
cancellable listening. `subscription.rs` serves the GraphQL WebSocket handshake,
updates, completion, and cancellation. `transport.rs` checks concurrent
single-host traffic through the configured pool and independent client state;
reqwest's consumed builder settings are not directly introspected.

These are integration tests of the crate against local peers, not probes of
the public services in `examples`. Add protocol regressions to the relevant
local peer and assert requests/results, keeping endpoint availability outside
ordinary test correctness.
