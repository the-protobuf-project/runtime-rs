# Typed problems and rich gRPC failures

[Hybrid server](../../README.md)

A service declares its own error vocabulary on a `Translator`: sentinel kinds,
status codes, reason names, and a domain. Handlers can return a `Problem` or
wrap registered errors; translation produces `tonic::Status` with
`google.rpc` details that both gRPC and gateways can preserve.

```rust
use grpc::problem::{Kind, Problem, Translator};
use tonic::Code;

fn main() {
    let errors = Translator::new("shop.example.com", [
        Kind::new("missing", Code::NotFound, "BOOK_NOT_FOUND"),
    ]);
    let status = errors.to_status(&Problem::new("missing")
        .resource("shop.example.com/Book", "books/1")
        .what("book books/1 does not exist").fix("Choose an existing book"));
    assert_eq!(status.code(), Code::NotFound);
}
```

See the compiled examples in [mod.rs](mod.rs) for building field violations,
resource details, and translated statuses with the application's error values.
`mod.rs` owns `Kind`, `Problem`, reason declarations, and detail construction.
`interceptor.rs` adapts the translator to message middleware; `tests.rs` verifies
translation and stable details directly.

The flow is application error -> translator lookup / explicit problem ->
status code and message -> ErrorInfo plus detail appropriate to the problem.
Unknown application errors need deliberate fallback handling; this module
cannot infer a service-specific reason. The runtime supplies no universal
application reason vocabulary.

```bash
cargo test -p tpp-grpc --lib problem
cargo test -p tpp-grpc --test errors
```

The integration tests verify translation through a running tonic service, not
only construction of local status values.
