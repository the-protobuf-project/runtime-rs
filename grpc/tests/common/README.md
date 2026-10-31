# Shared shop service

[Hybrid tests](../README.md)

`mod.rs` includes the checked-in shop tonic code and descriptor set, implements
a configurable service, and provides server/client setup shared by validation,
error, and interceptor tests. Requests select behavior so tests can observe
which layers ran and what status reached the wire.

```bash
cargo test -p tpp-grpc --test errors --test intercept --test validation_last
```

The flow is test request -> generated tonic client -> running HybridServer ->
registered middleware -> fixture handler -> status/reply assertions. Keep this
fixture simple enough to distinguish middleware rejection from handler work.
Edit contract fields in `tests/proto/shop/v1/shop.proto`, then regenerate the
artifacts; do not hand-edit generated message types here.
