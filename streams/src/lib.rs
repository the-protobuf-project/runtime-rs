//! Backend-agnostic publish/subscribe streams over Kafka, NATS, RabbitMQ,
//! Redis, MQTT, ZeroMQ, and OPC UA.
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
//! # Generated topics
//!
//! [`topic`] is Typed's richer sibling: a [`Topic`] is fixed to one subject by
//! a static [`TopicSpec`] carrying its defaults (consumer name, headers, a
//! partition key read from each message, batching), and hands out a typed
//! publisher, subscriber, durable consumer and [`BatchSink`].
//! [`topic::bind`] declares a stream, adding subjects it lacks, and checks the
//! [`Capability`] set a caller [`require`]s. protoc-gen-streams generates one
//! stream type per proto service over it, so generated code holds names and
//! defaults and the behaviour lives here.
//!
//! # Capabilities
//!
//! [`Publisher`] and [`Subscriber`] are what every backend can honestly do.
//! [`Durable`] consumers with ack/nak, [`Positioned`] replay, and native
//! [`Batch`] publishing are reached through [`as_durable`], [`as_positioned`],
//! and [`as_batch`], which fail with [`StreamError::Unsupported`] naming the
//! gap rather than silently downgrading delivery. [`require`] checks a set of
//! them at once, so a service that needs one can refuse to start without it.
//!
//! | Capability | Kafka | NATS | RabbitMQ | Redis | MQTT | ZeroMQ | OPC UA |
//! | --- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
//! | Publish / Subscribe | Yes | Yes | Yes | Yes | Yes | Yes | Write / monitored item |
//! | Durable (ack / nak) | Yes | JetStream | Yes | Streams | Persistent session | No | No |
//! | Positioned replay | Yes | JetStream | No | Streams | No | No | No |
//! | Batch | Native | Pipelined (JetStream) | Pipelined | Pipelined | Pipelined | Each | Sequential writes |
//! | Headers | Record headers | NATS headers | AMQP headers | Stream fields; frame v2 on pub/sub | MQTT 5 user properties | Frame v2 | PubSub only (user properties) |
//! | Expiry | No | JetStream (whole seconds) | Yes (at the head of the queue) | No | Yes (whole seconds) | No | PubSub only |
//! | Retain (last value) | No | No | No | No | Yes | No | No |
//! | Delayed nak | No | JetStream | No | Streams (up to the reclaim interval) | No | No | No |
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
pub mod instrument;
pub mod logging;
pub mod middleware;
pub mod provider;
pub mod retry;
pub mod telemetry;
pub mod topic;
pub mod typed;

#[cfg(feature = "protobuf")]
pub use codec::Protobuf;
pub use codec::{Codec, Json, Payload, Registry};
pub use core::{
    Acker, Batch, Capability, Deliveries, Delivery, Durable, Headers, MAX_KEY, MAX_TOTAL,
    MAX_VALUE, Manager, Message, Options, Position, Positioned, Publisher, Stream, Streams,
    Subscriber, Subscription, as_batch, as_durable, as_positioned, require,
};
pub use error::{Result, StreamError};
pub use instrument::{SENT_AT_HEADER, instrument};
pub use logging::{with_logging, with_logging_middleware, with_subscriber_logging};
pub use middleware::{Middleware, chain};
pub use retry::{with_retry, with_retry_middleware};
pub use telemetry::{Telemetry, with_telemetry, with_telemetry_middleware};
// The topic's Publisher, Subscriber, Consumer and Delivery stay under `topic::`:
// the first two would shadow the contract's traits of the same name.
pub use topic::{BatchPolicy, BatchSink, FieldFn, Topic, TopicSpec};
pub use typed::{Typed, typed, typed_with};
