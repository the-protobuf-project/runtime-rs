# buf.validate test schema

[Proto include root](../../README.md)

`validate.proto` defines the validation extensions imported by the shop API.
The compiler records field rules in `shop.binpb`; at runtime `ProtoValidator`
reads them through prost reflection and rejects invalid requests before the
handler (or after user message middleware with validation-last enabled).

```bash
cargo test -p tpp-grpc --test fixtures_regen -- --ignored
cargo test -p tpp-grpc --test errors --test validation_last
```

Preserve the extension names/numbers and imported schema structure. This is
fixture support, not the validation engine implementation. Change application
rules in the shop proto, then regenerate descriptors, rather than patching
compiled fixture bytes. The test generator discards the unused generated
`buf.validate.rs` module while retaining descriptor information.
