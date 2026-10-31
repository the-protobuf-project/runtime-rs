# Hybrid server implementation

[Crate guide](../README.md) · [Tests](../tests/README.md)

`HybridServer` stores registrations until startup, builds fresh route registries
for each start, and owns the resulting listener tasks and drain tokens.

## Control and request flows

`hybrid.rs` defines configuration/state. `builder.rs`, `mcp_options.rs`, and
`a2a.rs` collect registrations. `environment.rs` applies supported environment
overrides at construction; `validate.rs` resolves defaults and checks options
at start. `tls.rs` loads certificates before listeners are created.

`server.rs` binds gRPC, registers health/reflection/user services, and starts the
agent runtime before consuming tonic routes. It builds equivalent transport
layers for TCP and `Loopback`; deferred MCP loopback access is resolved once
those routes exist. `lifecycle.rs` then builds the HTTP router, starts optional
HTTP/3 and HTTP/1.1, and prints the runtime summary. A component failure drains
what already started. Stop retains the in-process service path through the
HTTP drain, then waits for gRPC completion under the shutdown deadline.

## Implementation map

| Files / folders | Responsibility |
| --- | --- |
| [hybrid.rs](hybrid.rs), [builder.rs](builder.rs), [types.rs](types.rs) | Server state, fluent registration, and callback contracts |
| [server.rs](server.rs), [lifecycle.rs](lifecycle.rs), [agent_runtime.rs](agent_runtime.rs) | Component startup, route ordering, ownership, and drain |
| [options](options/README.md), [environment.rs](environment.rs), [validate.rs](validate.rs) | Settings, overrides, defaults, and configuration checks |
| [http](http/README.md), [tls.rs](tls.rs), [cors.rs](cors.rs) | HTTP listeners, certificate adapters, and cross-origin policy |
| [rest.rs](rest.rs), [loopback.rs](loopback.rs) | Gateway dispatch and in-process tonic transport |
| [intercept.rs](intercept.rs), [stream_intercept.rs](stream_intercept.rs) | Reflected unary messages and stream middleware |
| [validation.rs](validation.rs), [validation_hints.rs](validation_hints.rs) | Protovalidate adapter and field-specific rich failures |
| [problem](problem/README.md), [hints](hints/README.md), [annotations.rs](annotations.rs) | Typed errors and descriptor-derived advice |
| [idempotency](idempotency/README.md) | Claim/settle/replay policy for request IDs |
| [codes.rs](codes.rs), [status.rs](status.rs), [gateway_errors.rs](gateway_errors.rs), [error.rs](error.rs) | Status vocabulary, HTTP mappings, and runtime errors |
| [observe.rs](observe.rs), [otel.rs](otel.rs) | Application observer and transport-level OpenTelemetry signals |
| [mcp_options.rs](mcp_options.rs), [a2a.rs](a2a.rs) | Agent protocol builders and hybrid integration |
| [banner](banner/README.md) | Resolved startup summary and rendering |
| [lib.rs](lib.rs) | Public exports |

For a request-policy change, inspect both TCP and loopback paths. REST and MCP
must reach the same service middleware, while A2A executor behavior belongs to
[agents](../../agents/README.md). Use the corresponding targets in
`cargo test -p tpp-grpc`; rendering has independent golden tests.

## Registering generated services

For a concrete generated tonic server, the registration shape is identical
to this standard health-service example:

```rust
use grpc::{HybridServer, options::{Endpoint, Options}};

fn build_server() -> HybridServer {
    let (_reporter, service) = tonic_health::server::health_reporter();
    HybridServer::new(Options {
        service_name: "example".into(), grpc: Endpoint::new("127.0.0.1", 50051),
        ..Default::default()
    }).with_service(service)
}
```

Run `build_server().serve().await` from a Tokio main function; the complete
crate-guide example also mounts an HTTP route.
