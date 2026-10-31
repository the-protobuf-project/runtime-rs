# A2A agents, discovery, and transports

[Agent runtime](../../README.md) · [Source map](../README.md)

This module adapts the pinned A2A SDK to `agents::Service`. The SDK implements
protocol requests and task handling; this module supplies identity, endpoints,
card construction, transport registration, and convenient executor events.

## End-to-end request flow

1. Create `Service::new(executor, skills)` or `with_task_store` when tasks must
   use an application-owned store. The default task store is in memory.
2. Runtime placement fills a `ServerConfig` with the common identity, address,
   mux, public URL, and optional gRPC routes. An alternative `from_fn` entry
   receives the assembled config for generated/custom hosting.
3. Resolve endpoints/base paths once, then construct the card from those same
   settings. A caller-supplied card is returned unchanged. Configure
   `public_url` when clients cannot dial the bind address directly.
4. Build the SDK request handler with the shared executor and stores. Mount
   JSON-RPC or REST below the resolved base path (default `/a2a`); register
   the `lf.a2a.v1.A2AService` tonic binding on `GrpcRoutes` when requested.
5. The request handler invokes the executor. It produces messages, status
   changes, and artifacts as an event stream; transport adapters serialize
   those events for the client. `TextAgent` provides a smaller text-in/text-out
   adapter, while `ExecutorFunc` supports explicit event-based implementations.
6. Cancellation and shutdown follow the owning runtime, or `start_server`
   when this module is served standalone.

The default binding is JSON-RPC. gRPC registers on the host's tonic server and
requires a route registry before that server starts; it does not create its
own gRPC listener. A gRPC-only runtime service needs no HTTP listener. REST
nesting strips the base prefix; a root base path merges routers instead.

Discovery uses the SDK's well-known agent card path. On a shared mux, the first
agent claiming that path owns it; later agents avoid a duplicate mount.
`serve_agent_card(false)` disables the claim. Do not assume that every agent
sharing a host gets a different well-known card URL.

## File map and failure handling

| File | Responsibility |
| --- | --- |
| [service.rs](service.rs) | Registration builders and placement-to-config conversion |
| [config.rs](config.rs) | Agent identity, skills, handler options, stores, and readiness callback |
| [endpoint.rs](endpoint.rs) | Path normalization and advertised/bound addresses |
| [card.rs](card.rs) | Card assembly from identity, skills, and transports |
| [handler.rs](handler.rs) | SDK handler and adapters around shared executor/stores |
| [serve.rs](serve.rs) | Standalone/shared mounting, card conflict handling, and drain |
| [grpc.rs](grpc.rs) | SDK tonic binding and protobuf re-exports |
| [transport.rs](transport.rs) | Binding names and comma-separated transport parsing |
| [executor.rs](executor.rs) | Executor adapters, parts, messages, task states, and event constructors |
| [error.rs](error.rs) | Startup failures and gRPC status-to-task-state conversion |
| [mod.rs](mod.rs) | Public API and SDK types |

Executor failures become terminal task events with stable status-code spelling;
cancellation maps to the canceled task state. Configuration/placement failures
return errors before readiness. Use the event helpers to retain the execution's
task and context IDs in responses.

Run `cargo test -p tpp-agents --no-default-features --features a2a` from the
workspace root. The card, executor, and serving tests cover endpoint agreement,
wire gRPC/JSON-RPC behavior, shared muxes, and standalone ownership.

## Usage

This text agent echoes messages over the default A2A JSON-RPC binding. The
application owns the stop token; the example runs for 30 seconds and drains.

```rust
use agents::{Config, Identity, Runtime, a2a};
use tokio_util::sync::CancellationToken;
use std::time::Duration;

#[tokio::main]
async fn main() -> agents::Result<()> {
    let agent = a2a::TextAgent::new(|text: String| async move { Ok(format!("echo: {text}")) });
    let runtime = Runtime::new(Config {
        identity: Identity { name: "echo".into(), version: "1.0.0".into(), ..Default::default() },
        host: "127.0.0.1".into(), port: 9000, ..Default::default()
    }).register(a2a::Service::new(agent, vec![]));
    let stop = CancellationToken::new();
    runtime.start(stop.clone()).await?;
    println!("{:?}", runtime.endpoints());
    tokio::time::sleep(Duration::from_secs(30)).await;
    stop.cancel();
    runtime.shutdown().await
}
```

Add `tpp-agents`, `tokio` (macros/rt-multi-thread/time), and `tokio-util`
(with `rt`) to your application's dependencies. For richer task events and
skills, replace `TextAgent` with your own executor. For a process controlled
by an external shutdown signal, pass its token to `runtime.serve(stop)`.
