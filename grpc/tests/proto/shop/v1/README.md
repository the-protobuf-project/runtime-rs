# Version 1 shop test contract

[Proto include root](../../README.md) · [Generated outputs](../../../fixtures/README.md)

`shop.proto` is the application fixture for validation, reflected unary
interceptors, rich errors, idempotency, and resource hints. It deliberately
contains the request fields and annotations those policies inspect, including
request identity and validation-only behavior.

To change the contract, edit this proto, regenerate both outputs, then run its
consumers from the repository root:

```bash
cargo test -p tpp-grpc --test fixtures_regen -- --ignored
cargo test -p tpp-grpc --test errors --test intercept --test validation_last
```

The common test service implements the generated tonic trait. Tests send
requests through clients and assert final replies/status details. Descriptor
changes must travel with generated Rust because validation and field decoding
otherwise inspect a different contract from the service being called. This is
a local test API, not a separate deployable shop server.
