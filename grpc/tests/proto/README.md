# Shop test API and annotation imports

[Hybrid tests](../README.md) · [Generated outputs](../fixtures/README.md)

This is the include root for test protobuf compilation. The
[shop API](shop/README.md) imports [Google API options](google/README.md) and
[validation options](buf/README.md). Keeping imports beside the fixture makes
descriptor-driven test behavior reproducible.

```bash
cargo test -p tpp-grpc --test fixtures_regen -- --ignored
cargo test -p tpp-grpc --test errors --test validation_last
```

`fixtures_regen.rs` compiles `shop/v1/shop.proto` with this directory as the
include path, emits tonic code plus an encoded descriptor set, and removes
unused generated import modules. Runtime tests consume those outputs rather
than invoking protoc per build. Preserve annotation field numbers and package
names; middleware finds extensions by their protobuf identity.
