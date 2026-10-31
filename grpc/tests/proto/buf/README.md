# Validation option namespace

This namespace contains the test copy of buf.validate option definitions.
The shop proto imports them so its descriptor carries validation rules that
the reflected validation engine can read.

See [validate](validate/README.md) for the definitions and their role. Run from
the repository root after changing an input:

```bash
cargo test -p tpp-grpc --test fixtures_regen -- --ignored
cargo test -p tpp-grpc --test errors
```

Regeneration updates `grpc/tests/fixtures/shop.v1.rs` and `shop.binpb` together.
These folders are compiler inputs, not independently served APIs or crates.
