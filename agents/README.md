# tpp-agents

`agents` hosts MCP tool servers and A2A agents under one identity, placement
policy, and shutdown lifecycle. The package is `tpp-agents`; Rust imports use
`agents`. Both protocol features (`mcp`, `a2a`) are enabled by default; select
only the ones you need with `default-features = false` in your dependency.

## Runnable text agent

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

## End-to-end lifecycle

1. Build `Config` with the service `Identity`, shared host/port, and optional
   public URL. Constructing `Runtime` opens no sockets.
2. Wrap an MCP handler in `mcp::Service`, or an A2A executor and its skills in
   `a2a::Service`, and register it with `Runtime::register`. Use `register_dyn`
   for an already shared trait object.
3. Each service declares `Requirements`: whether it needs HTTP, gRPC routes,
   and an address of its own. The runtime groups services by listen address.
   Services with no address share the runtime's listener and separate paths.
4. `start(cancel)` supplies each service a `Placement`, waits for its `Ready`
   signal, then opens owned HTTP listeners. All handlers mount before traffic
   is accepted. A host-supplied `Mux` is mounted into without opening a server.
5. Read `endpoints()` after start to discover the mounted protocol endpoints.
   A2A card identity and advertised addresses derive from the same settings.
6. Cancel the service token and call `shutdown()` to drain owned listeners.
   Alternatively, `serve(cancel)` performs start, cancellation wait, and drain.
   Host-owned HTTP servers and gRPC listeners remain the host's responsibility.

```text
application Config + registered Services
  -> Requirements -> address groups -> Placement
  -> protocol handlers mount -> Ready -> owned listeners accept
  -> cancellation -> service stop + listener drain
```

## Choosing a protocol

| Need | Entry point | Request path |
| --- | --- | --- |
| Expose tools, prompts, or resources | `mcp::Service::from_handler(handler)` | MCP transport -> hosted policies -> generated/handwritten handler |
| Delegate work to an agent | `a2a::Service::new(executor, skills)` | A2A binding -> SDK request handler/task store -> executor events |
| Serve both in an existing process | `Runtime::start` with `Config::mux` / `grpc_routes` | Host starts its listeners after registration completes |

MCP defaults to streamable HTTP at `/mcp`. SSE and stdio are available;
stdio needs no HTTP listener. A2A defaults to JSON-RPC under `/a2a`; REST and
gRPC are optional bindings. A2A gRPC requires `Config::grpc_routes`, and the
host must finish runtime startup before consuming the tonic routes.

Use [the MCP guide](src/mcp/README.md) for factories, combined servers,
elicitation, resources, and tool middleware. Use [the A2A guide](src/a2a/README.md)
for cards, task stores, transport selection, and executor events. The
[gRPC crate](../grpc/README.md) drives this runtime when a process also serves
ordinary gRPC and REST endpoints.

## Ownership and failures

The default bind host is `0.0.0.0`; set a port explicitly. A public URL is an
advertisement for clients behind a proxy, not a different bind address. A
service's explicit address creates a separate group; services naming the same
address still share a listener.

Empty registration, a second start, missing gRPC routes, mount failures,
readiness timeout, cancellation during mounting, and listener bind errors
return `agents::Error`. Readiness defaults to five seconds. Failed startup
cancels/aborts work already started. Shutdown is safe before startup and when
called twice; owned listener drains use the five-second shutdown bound. A
runtime is single-start; construct another runtime for another startup cycle.

Do not set short read/write limits on listeners carrying long-running streams.
The owned-listener implementation uses a whole-exchange timeout with the
larger configured duration, since hyper does not expose Go's separate phases.

## Develop and verify

Run from the workspace root with the Rust toolchain declared in
[Cargo.toml](../Cargo.toml):

```bash
cargo test -p tpp-agents
cargo test -p tpp-agents --no-default-features
cargo test -p tpp-agents --no-default-features --features mcp
cargo test -p tpp-agents --no-default-features --features a2a
cargo doc -p tpp-agents --no-deps
```

[Source map](src/README.md) · [Test coverage and fixtures](tests/README.md).
Crate-level rustdoc contains compiled usage examples; these READMEs explain
how the pieces connect.
