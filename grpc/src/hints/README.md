# Descriptor-derived error hints

[Hybrid server](../../README.md) · [Implementation and usage](mod.rs)

Hints enrich generic missing/duplicate/conflict failures after the handler
returns. Descriptors supply `google.api.resource` identity and
`google.api.http` routes, so advice can name the affected resource and the
related Get/List operation without maintaining a second route table.

Use hints as message middleware on a server that has the API's descriptor set.
The exact constructors and a complete registration example are documented in
[mod.rs](mod.rs). Run the examples and their service-level verification with:

```bash
cargo test -p tpp-grpc --doc
cargo test -p tpp-grpc --lib hints
cargo test -p tpp-grpc --test errors
```

The interceptor sees the request, calls the next handler, then rewrites eligible
errors. INTERNAL/UNKNOWN messages are concealed and replaced with advice
carrying a trace ID, while the original error is available to tracing/logging.
Do not depend on exposing backend error strings as public API content.

`mod.rs` contains resource lookup, related-method discovery, message formatting,
and the interceptor. `tests.rs` checks the descriptor/formatting behavior. The
integration test's shop API verifies the final wire status. APIs lacking the
annotations cannot receive the same resource-specific hints; update their
contract rather than hardcoding names here.

## Installing the interceptor

```rust
use grpc::{HybridServer, hints::{Hinter, Options}};

fn with_hints(server: HybridServer, descriptors: &'static [u8]) -> HybridServer {
    server.with_reflection(descriptors).with_interceptor(
        Hinter::new(Options { product: "Shop".into(), ..Default::default() }).interceptor()
    )
}
```

Pass the encoded descriptor set generated for your API; use the same set for
validation/reflection so resource and method identities agree.
