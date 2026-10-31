# Shop API namespace

This namespace holds the versioned application API used by hybrid-server
integration tests. Versioned proto inputs generate both the tonic service and
the descriptors used by middleware.

See [v1](v1/README.md) for the definitions and their role. Run from
the repository root after changing an input:

```bash
cargo test -p tpp-grpc --test fixtures_regen -- --ignored
cargo test -p tpp-grpc --test errors
```

Regeneration updates `grpc/tests/fixtures/shop.v1.rs` and `shop.binpb` together.
These folders are compiler inputs, not independently served APIs or crates.
