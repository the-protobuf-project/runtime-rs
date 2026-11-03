# Hybrid server settings

[Hybrid server](../../README.md) · [Environment overrides](../environment.rs)

Build `Options`, pass it to `HybridServer::new`, register services, then start.
Port 0 asks the operating system for a free port; `Options::default()` carries
the default ports instead. `grpc_addr`, `http_addr` and `agent_endpoints` report
what was bound.

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
`Endpoint::unix(path)` and `Endpoint::pipe(name)` name local endpoints. They are
stored in `host` as `unix:PATH` or `pipe:NAME`, so the host override works for
them and existing struct literals still compile. The port is ignored for them.
MCP and A2A accept the same `unix:` host, and then get no derived port.
`limits.rs` defines `GrpcLimits` and `Overflow`, which `with_grpc_limits`
applies to the gRPC listener. Every setting defaults to unset, which leaves
behaviour as it was.
`agents.rs` defines MCP/A2A binding and placement settings. `environment.rs`
defines the four modes; reflection is exposed only in Development/Debug and
only when descriptors were supplied. `opentelemetry.rs` names metric types;
it does not create providers or enable exporters.

At construction, the uppercased service name prefixes overrides such as
`SHOP_HYBRID_GRPC_HOST` and `SHOP_HYBRID_GRPC_PORT`. GRPC/HTTP/MCP/A2A support
host and port overrides; `SHOP_HYBRID_A2A_TRANSPORT` accepts a comma-separated
binding list. Invalid override values are ignored with debug logging. No
blanket environment override exists for every `Options` field.

At startup, `validate.rs` requires a gRPC host, unless `without_grpc_listener`
leaves nothing to bind, and an HTTP host when HTTP is enabled. It refuses a
local HTTP endpoint, because the gateway serves TCP only. The defaults of
65020 and 6502 come from `Options::default()`. Explicit listening MCP transport
settings default to `0.0.0.0` and HTTP port + 1; stdio needs no listener. Enabled
A2A defaults its host/base path and chooses the next port after MCP (or HTTP + 2
when MCP has no assigned port). With HTTP on port 0 there is nothing to derive
from, so MCP and A2A are ephemeral too. Note the consequence: an MCP or A2A port
of 0 beside a fixed HTTP port still means "follow HTTP", not "any free port".
Assign ports explicitly where several servers share a host. HTTP/3 uses HTTP + 1,
requires TLS and a fixed HTTP port, and rejects port overflow.

```bash
cargo test -p tpp-grpc --lib
cargo test -p tpp-grpc --test a2a --test mcp --test validation_last
```
