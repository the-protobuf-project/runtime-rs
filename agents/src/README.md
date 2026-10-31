# Agent runtime implementation

[Module guide](../README.md) · [Tests](../tests/README.md)

`lib.rs` exports the runtime vocabulary and feature-gated protocols. Runtime
code decides where and when a service runs; protocol modules decide what
requests mean.

## Startup and shutdown flow

`Runtime::new` normalizes host/readiness defaults. Registration stores shared
`Service` values without executing them. `start.rs` rejects empty or repeated
starts and checks gRPC requirements before mounting services. `placement.rs`
groups by address, chooses an owned `Mux` or the host's mux, and builds each
`Placement`. Each spawned service must publish its endpoints through `Ready`.
Only after all mounts succeed does the runtime bind owned HTTP groups.

The running state holds endpoint reports, listener tasks, service tasks, and
separate service-cancellation and listener-drain tokens. `serve` waits on the
caller's token; `shutdown` takes the running handles once, cancels services,
drains listeners, then observes service completion. A host's server is never
closed here. Startup failures stop previously admitted work instead of leaving
partially bound listeners.

## File map

| File / folder | Responsibility |
| --- | --- |
| [agents.rs](agents.rs) | `Service`, `Requirements`, `Placement`, `Ready`, identity, endpoints, and constants |
| [config.rs](config.rs) | Shared identity, placement, public URL, host-owned registries, and timeouts |
| [runtime.rs](runtime.rs) | Construction, registration, accessors, and running-state ownership |
| [start.rs](start.rs) | Mount ordering, readiness waits, and failed-start cleanup |
| [placement.rs](placement.rs) | Address grouping, listener ownership, binding, and HTTP timeouts |
| [lifecycle.rs](lifecycle.rs) | Blocking serve and idempotent drain |
| [mux.rs](mux.rs) | Mutable shared seams around consuming axum/tonic route builders |
| [error.rs](error.rs) | Typed configuration, readiness, listener, and drain failures |
| [mcp](mcp/README.md) | MCP transport hosting and generated-handler support |
| [a2a](a2a/README.md) | A2A cards, execution, and bindings |
| [shared](shared/README.md) | Per-call forwarding from HTTP headers to gRPC metadata |

To add a service, implement `Service`, state its actual listener requirements,
mount through the supplied placement, signal readiness only after mounting,
and honor cancellation. Verify placement and failure cleanup through the
integration tests rather than opening listeners in the runtime constructor.

## Using runtime lifecycle

```rust
use agents::Runtime;
use tokio_util::sync::CancellationToken;

async fn run_until_stopped(runtime: &Runtime, stop: CancellationToken) -> agents::Result<()> {
    runtime.start(stop.clone()).await?;
    for endpoint in runtime.endpoints() { println!("{endpoint:?}"); }
    stop.cancelled().await;
    runtime.shutdown().await
}
```

Register services before calling this helper. The caller cancels `stop` from
its own signal/task lifecycle. See the crate guide for complete construction.
