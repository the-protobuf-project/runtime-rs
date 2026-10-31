# Request-ID claim and replay

[Hybrid server](../../README.md) · [Interceptor implementation](mod.rs)

Register this message interceptor and your service's descriptors to deduplicate
unary RPCs with a nonempty singular string `request_id`. Calls without a usable
descriptor/field/ID and calls with `validate_only = true` pass through.

```rust
use grpc::{HybridServer, idempotency::{self, MemoryStore, Options}};

fn add_idempotency(server: HybridServer, descriptors: &'static [u8]) -> HybridServer {
    server.with_reflection(descriptors).with_interceptor(idempotency::interceptor(
        MemoryStore::new(),
        Options { domain: "shop.example.com".into(), ..Default::default() },
    ))
}
```

`mod.rs` defines the store contract and policy; `memory.rs` supplies a
process-local reference store. The key is a SHA-256 digest of method plus ID:

```text
create unique IN_FLIGHT key -> run handler -> store DONE + protobuf response
                       error -> delete claim so a later retry can execute
existing DONE -> replay response; existing IN_FLIGHT -> ABORTED
stale IN_FLIGHT -> clear claim + ABORTED; next retry may claim it
```

The default stale threshold is ten minutes; keep it longer than legitimate
handler execution. Replays use encoded response bytes and the method's output
descriptor. Claim failures are distinguished from duplicate races by reading
the key. Settle failures are logged without turning a successful application
write into an error response; replay is then not guaranteed.

`MemoryStore` loses claims/responses on restart and coordinates only one
process. Implement `Store` with shared unique creation for multiple replicas.
This middleware does not make claim storage and application writes one atomic
transaction, and keys are scoped to `(method, request_id)`, not caller identity.
Choose store isolation and ID policy accordingly.

```bash
cargo test -p tpp-grpc --test errors
cargo test -p tpp-grpc --lib idempotency
```

The integration target covers successful replay, failed-call release,
concurrent duplicates, and abandoned claims.
