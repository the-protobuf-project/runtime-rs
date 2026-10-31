# Agent runtime integration tests

[Module guide](../README.md) · [Common fixtures](common/README.md) ·
[Generated MCP fixture](fixtures/README.md)

These tests drive runtime lifecycles and real local protocol conversations.
They do not need an external MCP/A2A server. Start with:

```bash
cargo test -p tpp-agents
cargo test -p tpp-agents --test mcp_hosting
cargo test -p tpp-agents --no-default-features --features a2a
```

`runtime.rs` checks defaults, empty/repeated startup, missing gRPC routes,
readiness timeouts, and early service failure. `placement.rs` checks shared,
separate, and host-owned listeners. `lifecycle.rs` checks draining/released ports.
`protocols.rs` serves MCP and A2A together under one runtime identity.

MCP targets cover tool schemas/results/errors (`mcp_tools`), interceptor order
(`mcp_interceptors`), helper parity (`mcp_parity`, `mcp_helpers`), hosting policy,
legacy SSE/elicitation/combined endpoints (`mcp_hosting`), and actual generated
handlers (`generated_mcp`). `mcp_client.rs` is a small wire client used by
other tests; `mcp_probe.rs` supplies inspectable test handlers.

A2A targets check cards/endpoints (`a2a_card`), executor events/errors
(`a2a_executor`), and standalone/shared/gRPC serving (`a2a_serve`). Tests are
feature-gated for their protocols. The no-default-feature suite verifies that
the runtime's common lifecycle remains usable without either protocol module.

Add a regression to the layer that owns the behavior, assert from the client
when protocol behavior is at issue, and shut down the runtime before asserting
port reuse. Preserve generated fixtures as generator output.
