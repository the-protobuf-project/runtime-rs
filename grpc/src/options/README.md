# Hybrid server settings

[Hybrid server](../../README.md) · [Environment overrides](../environment.rs)

Build `Options`, pass it to `HybridServer::new`, register services, then start.
Zero ports are configuration defaults, not requests for an ephemeral port.

```rust
use grpc::{HybridServer, options::{Endpoint, Options, ServerEnvironment}};

fn main() {
    let options = Options {
        service_name: "shop".into(),
        version: "1.0.0".into(),
        grpc: Endpoint::new("127.0.0.1", 50051),
        http: Endpoint::new("127.0.0.1", 8080),
        enable_http: true,
        enable_health: true,
        environment: ServerEnvironment::Production,
        ..Default::default()
    };
    let _server = HybridServer::new(options);
    // Add services and call start().await or serve().await on a Tokio runtime.
}
```

`mod.rs` defines identity, `Endpoint`, component toggles, and `Options`.
`agents.rs` defines MCP/A2A binding and placement settings. `environment.rs`
defines the four modes; reflection is exposed only in Development/Debug and
only when descriptors were supplied. `opentelemetry.rs` names metric types;
it does not create providers or enable exporters.

At construction, the uppercased service name prefixes overrides such as
`SHOP_HYBRID_GRPC_HOST` and `SHOP_HYBRID_GRPC_PORT`. GRPC/HTTP/MCP/A2A support
host and port overrides; `SHOP_HYBRID_A2A_TRANSPORT` accepts a comma-separated
binding list. Invalid override values are ignored with debug logging. No
blanket environment override exists for every `Options` field.

At startup, `validate.rs` requires a gRPC host, defaults gRPC to 65020 and HTTP
to 6502, and requires an HTTP host when HTTP is enabled. Explicit listening
MCP transport settings default to `0.0.0.0` and HTTP port + 1; stdio needs no
listener. Enabled A2A defaults its host/base path and chooses the next port
after MCP (or HTTP + 2 when MCP has no assigned port). Explicitly assign ports
when deployment topology needs different addresses. HTTP/3 is a UDP socket at
HTTP + 1, requires TLS, and rejects port overflow.

```bash
cargo test -p tpp-grpc --lib
cargo test -p tpp-grpc --test a2a --test mcp --test validation_last
```
