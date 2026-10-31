# tpp-grpc

`grpc::HybridServer` hosts tonic services, optional REST/JSON gateways,
experimental HTTP/3, and MCP/A2A agent protocols under one configuration and
lifecycle. The Cargo package is `tpp-grpc`; the Rust library name is `grpc`.

## Runnable server

This complete program serves tonic health on port 50051 and an HTTP route on
8080. It waits for SIGINT/SIGTERM and drains automatically.

```rust
use axum::{Router, routing::get};
use grpc::{HybridServer, options::{Endpoint, Options}};

#[tokio::main]
async fn main() -> grpc::Result<()> {
    let (_reporter, health) = tonic_health::server::health_reporter();
    let options = Options {
        service_name: "shop".into(), version: "1.0.0".into(),
        grpc: Endpoint::new("127.0.0.1", 50051),
        http: Endpoint::new("127.0.0.1", 8080),
        enable_http: true,
        ..Default::default()
    };
    HybridServer::new(options)
        .with_service(health)
        .with_http_handler(Router::new().route("/hello", get(|| async { "hello" })))
        .serve().await
}
```

Application dependencies are `tpp-grpc`, `tokio` (macros/rt-multi-thread),
`axum`, and `tonic-health` at the versions used in the workspace. Then:

```bash
curl http://127.0.0.1:8080/hello
curl http://127.0.0.1:8080/health
```

Replace `health` with your generated tonic server for application RPCs. For
generated REST transcoding, pass a generated gateway built with a client over
the supplied loopback to `with_gateway` instead of using a replacement router.

## Build and run a service

1. Construct `options::Options` with a service name, version, endpoints, and
   component toggles. `HybridServer::new` applies service-prefixed environment
   overrides; it opens no listeners.
2. Register generated tonic servers with `with_service`, or use a route-builder
   closure through `with_grpc_services`. Add descriptor sets with
   `with_reflection` for reflection and descriptor-driven middleware.
3. Register generated REST gateways through `with_gateway`. The closure receives
   a `Loopback` that a generated tonic client can use as its transport. The
   gateway translates HTTP/JSON; the loopback dispatches through the same
   services and interceptor layers used by TCP gRPC.
4. Add metadata/message/stream interceptors, optional validation, CORS, agent
   services, and certificate paths. Certificate files are loaded at startup.
5. Call `start().await` to bind and return, or `serve().await` to wait for SIGINT
   or SIGTERM and drain automatically. Startup reports invalid options,
   certificate failures, and bind errors as ordinary `grpc::Error` results.
6. Call `stop` / `close` when the host exits, or `restart` to replay registrations
   onto fresh listeners. Flush application-owned telemetry after draining.

```text
gRPC TCP -> metadata / stream / message layers -> tonic service
REST HTTP/1.1 or HTTP/3 -> generated gateway -> Loopback -> same layers/service
MCP -> hosted tool -> generated gRPC client over Loopback -> same layers/service
A2A -> SDK bindings/executor (gRPC binding registers beside tonic services)
```

A gateway registered by `with_http_gateways` receives a dial target and builds
its own routes. `with_http_handler` replaces gateway routes with an application
router. The HTTP surface also mounts `GET /health`; `enable_health` separately
controls the standard gRPC health service.

## Interceptors and errors

Metadata interceptors registered with `with_unary_interceptor` run before the
message chain. Message interceptors registered with `with_interceptor` can
inspect/rewrite reflected requests and replies. Register the appropriate
encoded descriptors; without them middleware cannot decode application bodies.
`with_validation` checks `buf.validate` rules before user message interceptors;
`with_validation_last` moves validation behind them. It does not move validation
past or before the metadata layer. Streaming interceptors have their own chain
and completion/cancellation observation.

The [problem translator](src/problem/README.md) converts application failures to
rich gRPC status details. [Hints](src/hints/README.md) use protobuf resource/HTTP
annotations to explain generic errors and conceal internal error text. The
[idempotency interceptor](src/idempotency/README.md) claims a request ID before
execution and replays successful responses. These are explicit middleware
choices; declaring options does not add every middleware automatically.

## Transports, configuration, and ownership

The `http3` Cargo feature is enabled by default; the runtime option
`experimental_http3` must also be set to start it. HTTP/3 uses QUIC/TLS and binds
UDP at `http.port + 1`; it requires a certificate. HTTP/1.1 and HTTP/3 share one
router. Ordinary gRPC uses its tonic HTTP/2 TCP listener. A certificate supplies
gRPC and gateway TLS. Agent HTTP listeners are driven by the agent runtime;
review its listener behavior separately when configuring deployment TLS.

MCP and A2A are enabled through their options and registrations. Agent startup
finishes before tonic takes its route registry, allowing A2A gRPC to share the
gRPC port. HTTP agent bindings use the configured agent ports. MCP can combine
multiple handlers at one endpoint or preserve service-specific paths.

Environment variables follow `SERVICENAME_HYBRID_COMPONENT_SETTING`, for example
`SHOP_HYBRID_GRPC_PORT`. See [options](src/options/README.md) and
[environment.rs](src/environment.rs) for supported overrides and defaults.
Reflection is gated by environment mode and supplied descriptors.

Startup brings up gRPC/agents, HTTP/3, then HTTP/1.1, and prints the banner only
afterward. Partial failures trigger cleanup. Shutdown cancels TCP gRPC
acceptance, drains HTTP/agent components while loopback calls can still reach
services, then waits for in-flight gRPC work. The overall default deadline is
20 seconds and can be changed with `with_shutdown_timeout`.

## Develop and verify

Run from the workspace root using the Rust version in [Cargo.toml](../Cargo.toml).
Install `protoc` for dependencies that compile validation protos and for
explicit fixture regeneration. Checked-in shop fixtures avoid regenerating
application test code during ordinary builds.

```bash
cargo test -p tpp-grpc
cargo test -p tpp-grpc --no-default-features
cargo run -p tpp-grpc --example banner
cargo doc -p tpp-grpc --no-deps
```

[Source map](src/README.md) · [Tests and fixture regeneration](tests/README.md) ·
[Examples](examples/README.md) · [Banner font](assets/README.md).
