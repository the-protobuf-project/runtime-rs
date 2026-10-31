# Runtime test service

[Integration tests](../README.md)

`mod.rs` defines the stand-in `Service` and common helpers for runtime-only
tests. Tests configure its requirements/readiness/failure behavior, register
it, start the runtime, and inspect outcomes. This isolates placement and
lifecycle guarantees from the MCP/A2A SDK implementations.

Use it through `mod common;` in sibling integration targets. Run its consumers:

```bash
cargo test -p tpp-agents --test runtime --test placement --test lifecycle
```

A helper is allowed to be unused in one integration binary while another uses
it. Keep assertions in the consuming test and make fixtures honor readiness
and cancellation contracts; a fixture that binds independently would bypass
the runtime behavior being verified.
