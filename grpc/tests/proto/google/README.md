# Google API option namespace

This namespace contains Google API annotation inputs used by the shop
fixture. They supply HTTP bindings and resource identities to descriptor-driven
error hints and contract tests.

See [api](api/README.md) for the definitions and their role. Run from
the repository root after changing an input:

```bash
cargo test -p tpp-grpc --test fixtures_regen -- --ignored
cargo test -p tpp-grpc --test errors
```

Regeneration updates `grpc/tests/fixtures/shop.v1.rs` and `shop.binpb` together.
These folders are compiler inputs, not independently served APIs or crates.
