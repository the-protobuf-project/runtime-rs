//! Backend-agnostic publish/subscribe streams over Kafka, NATS, RabbitMQ,
//! Redis, MQTT, and ZeroMQ.
//!
//! A Rust port of runtime-go's `streams` module. Providers live under
//! [`drivers`], one Cargo feature each, and take a client you own, so nothing
//! here dials or closes a connection you handed over.
//!
//! # Lifecycle
//!
//! A provider implements [`Streams`]: create a stream with the subjects it
//! accepts, then bind it to get a [`Manager`] that publishes and subscribes.
//! Subjects are fixed at creation so a typo fails loudly with
//! [`StreamError::UnknownSubject`] instead of producing a topic nobody reads.
//!
//! # Your model, not ours
//!
//! There is no message type on the publish path. A value is encoded by a
//! [`Codec`] into a [`Payload`], and a delivered [`Message`] stays encoded until
//! you decode it into your own type. The codec's name travels in the frame, so
//! a message decodes the way it was written. [`typed()`] puts a compile-checked
//! view over any Manager:
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use futures::StreamExt;
//! # async fn example(manager: Arc<dyn streams::Manager>) -> streams::Result<()> {
//! #[derive(serde::Deserialize, serde::Serialize)]
//! struct Order { user: String, total: u32 }
//!
//! let orders = streams::typed::<Order>(manager);
//! let mut placed = orders.subscribe("order.placed", &Default::default()).await?;
//! orders.publish("order.placed", &Order { user: "ada".into(), total: 42 }, &Default::default()).await?;
//! let order = placed.next().await;
//! # Ok(()) }
//! ```
//!
//! # Capabilities
//!
//! [`Publisher`] and [`Subscriber`] are what every backend can honestly do.
//! [`Durable`] consumers with ack/nak, [`Positioned`] replay, and native
//! [`Batch`] publishing are reached through [`as_durable`], [`as_positioned`],
//! and [`as_batch`], which fail with [`StreamError::Unsupported`] naming the
//! gap rather than silently downgrading delivery.
//!
//! | Capability | Kafka | NATS | RabbitMQ | Redis | MQTT | ZeroMQ |
//! | --- | :---: | :---: | :---: | :---: | :---: | :---: |
//! | Publish / Subscribe | Yes | Yes | Yes | Yes | Yes | Yes |
//! | Durable (ack / nak) | Yes | JetStream | Yes | Streams | Persistent session | No |
//! | Positioned replay | Yes | JetStream | No | Streams | No | No |
//! | Batch | Native | Each | Each | Pipelined | Each | Each |
//!
//! # Lifetime is the stream's
//!
//! A [`Subscription`] is a `futures::Stream`. Dropping it is how a subscription
//! ends and releases its server-side state; there is no separate cancel to
//! forget.
//!
//! # Middleware
//!
//! [`with_retry`], [`with_logging`], and [`with_telemetry`] wrap a
//! [`Publisher`]; [`chain`] composes them, last outermost.
//! [`with_subscriber_logging`] wraps a [`Subscriber`].
//!
//! # Wire compatibility
//!
//! Every provider frames payloads with [`provider::pack`], byte-for-byte
//! runtime-go's frame, so Go and Rust services can share a broker.

#![warn(missing_docs)]

pub mod codec;
pub mod core;
pub mod drivers;
pub mod error;
pub mod logging;
pub mod middleware;
pub mod provider;
pub mod retry;
pub mod telemetry;
pub mod typed;

#[cfg(feature = "protobuf")]
pub use codec::Protobuf;
pub use codec::{Codec, Json, Payload, Registry};
pub use core::{
    Acker, Batch, Deliveries, Delivery, Durable, Manager, Message, Options, Position, Positioned,
    Publisher, Stream, Streams, Subscriber, Subscription, as_batch, as_durable, as_positioned,
};
pub use error::{Result, StreamError};
pub use logging::{with_logging, with_logging_middleware, with_subscriber_logging};
pub use middleware::{Middleware, chain};
pub use retry::{with_retry, with_retry_middleware};
pub use telemetry::{with_telemetry, with_telemetry_middleware};
pub use typed::{Typed, typed, typed_with};
