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

`ephemeral_ports` holds port 0 to meaning a free port on every listener, reported
as bound. `grpc_listener` serves REST through the loopback with no gRPC port
open. `telemetry_providers` proves the runtime's signals stay off the global
providers when told to, in its own binary for the same reason as `observe`.
`local_listener` serves gRPC on a Unix socket: unary and streaming calls,
reflection, health, interceptors and caller layers, stale-socket replacement,
live-socket and regular-file refusal, a `0600` socket in a created `0700`
directory, the banner's address, graceful drain, and a drain bound that cuts a
call that never finishes. `local_limits` covers refusing and queueing past the
connection limit, the idle timeout, the per-connection stream limit, and TCP
clients keeping `remote_addr`. `no_tcp` runs gRPC on a socket with REST off and
MCP on a socket, then MCP on stdio in a child process. In both cases an MCP tool
reaches gRPC in-process, and the process has no TCP listener; the count first
proves it can see one. `local_pipe` (Windows only) serves unary and streaming
calls over a named pipe and refuses a held name.
`dependency_graph` reads Cargo's resolution for every target to prove a build
without `a2a` links no A2A SDK, reqwest or aws-lc; that one without `tls` links
no rustls, ring or aws-lc, and nor does `tpp-agents` with only `mcp`; and that the
`tls` build does link ring, so the check is seen to bite. Each fails if the graph
it read is too small to be real.

Banner targets compare figures/tables against Go-derived golden files and
check full summaries; `banner_output` reads a child process's two streams. `fixtures_regen` is ignored by default. After changing
shop protos, regenerate and verify explicitly:

```bash
cargo test -p tpp-grpc --test fixtures_regen -- --ignored
cargo test -p tpp-grpc --test errors --test intercept --test validation_last
```

Review the generated `shop.v1.rs` and `shop.binpb` together. Ordinary tests use
those checked-in artifacts; regeneration is not part of ordinary compilation.
