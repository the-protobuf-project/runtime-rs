# Hybrid server integration tests

[Crate guide](../README.md) · [Shop helpers](common/README.md) ·
[Fixtures](fixtures/README.md) · [Protocol sources](proto/README.md)

Tests start local servers and exercise behavior with tonic/HTTP/MCP clients.
They need no external API or service credentials. Use the workspace Rust
version and install `protoc` for dependency build scripts and regeneration.

```bash
cargo test -p tpp-grpc
cargo test -p tpp-grpc --no-default-features
```

`gateway`, `http_surface`, and `cors` verify in-process REST dispatch, listener
lifecycle, route replacement, preflight, and response headers. `intercept`,
`stream_intercept`, and `validation_last` verify reflected messages, ordering,
refusal, stream completion, and cancellation on TCP and loopback.

`errors` covers request validation, problem translation, request-ID replay,
concurrent/abandoned claims, and descriptor-derived hints. `mcp` verifies that
MCP credentials reach the same gRPC interceptor chain and checks combined and
stateless modes. `a2a` verifies the shared gRPC binding and HTTP discovery/task
path. `observe` checks actual OTel spans/metrics with process-global providers
inside its own integration-test binary.

Banner targets compare figures/tables against Go-derived golden files and
check full summaries. `fixtures_regen` is ignored by default. After changing
shop protos, regenerate and verify explicitly:

```bash
cargo test -p tpp-grpc --test fixtures_regen -- --ignored
cargo test -p tpp-grpc --test errors --test intercept --test validation_last
```

Review the generated `shop.v1.rs` and `shop.binpb` together. Ordinary tests use
those checked-in artifacts; regeneration is not part of ordinary compilation.
