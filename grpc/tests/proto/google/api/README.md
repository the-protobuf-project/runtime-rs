# Google HTTP and resource annotations

[Proto include root](../../README.md)

`annotations.proto` declares the HTTP method option; `http.proto` defines HTTP
rules; `resource.proto` declares resource descriptors/references. The shop proto
uses these options, and its compiled descriptor set carries their values into
middleware tests.

```bash
cargo test -p tpp-grpc --test fixtures_regen -- --ignored
cargo test -p tpp-grpc --test errors
```

The flow is shop method/resource options -> descriptor set -> annotation
inspection -> resource-aware errors naming relevant Get/List RPCs and REST
paths. Preserve canonical protobuf package names and extension numbers. Do not
add service business logic or standalone generated clients to this import-only
folder. The regeneration target discards unused generated Google API Rust
modules after building the shop artifacts.
