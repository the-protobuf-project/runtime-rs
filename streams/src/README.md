# Source layout

[Streams guide](../README.md) · [Drivers](drivers/README.md)

| Path | Role |
| --- | --- |
| `core/` | The contract: `Streams`, `Manager`, `Publisher`, `Subscriber`, the optional `Durable`, `Positioned`, and `Batch`, and the values that cross it (`Stream`, `Message`, `Delivery`, `Options`, `Position`). |
| `codec.rs` | `Codec<T>`, the `Json` and `Protobuf` codecs, `Payload`, and the `Registry` of codec names a receiver accepts. |
| `typed.rs` | `Typed<T, C>`: a compile-checked view that encodes on publish and decodes on subscribe. |
| `provider/` | Building blocks drivers share: the wire frame (`pack`/`unpack`), subject wildcards, ULID ids, batch helpers, prefetch defaults, and delivery metrics. Applications never need it. |
| `drivers/` | One provider per backend, each behind its own feature. |
| `middleware.rs`, `retry.rs`, `logging.rs`, `telemetry.rs` | Publisher wrappers and `chain`. |
| `error.rs` | `StreamError` and `Result`. |

## Middleware

Wrappers take and return `Arc<dyn Publisher>`, so they compose without knowing
the backend. `chain` makes the last one outermost:

```rust
use std::{sync::Arc, time::Duration};
use streams::{Manager, chain, typed, with_logging_middleware, with_retry_middleware};

fn wire(manager: Arc<dyn Manager>) -> streams::Typed<String> {
    let publisher = chain(manager.clone(), [
        with_retry_middleware(3, Duration::from_millis(25)),
        with_logging_middleware(), // outermost: one log line per logical publish
    ]);
    typed::<String>(manager).with_publisher(publisher)
}
```

- `with_retry` doubles its backoff between attempts and returns a settled
  failure (undeclared subject, unsupported request, codec error) at once. Every
  attempt carries the same message id, so consumers can drop a duplicate left
  by a lost acknowledgement.
- `with_logging` and `with_subscriber_logging` emit `tracing` events and never
  log payload bytes.
- `with_telemetry` records `streams_published_total` and
  `streams_publish_duration_seconds` on a Meter you supply.

## Settling deliveries

A `Delivery` is a lent message. `ack` and `nak` consume it, so a delivery is
settled at most once by construction. Ack after the work is done: acking on
receipt turns at-least-once into at-most-once. A delivery dropped unsettled is
redelivered by the backend.
