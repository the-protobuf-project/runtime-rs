//! Stream providers, one per backend, each behind a Cargo feature of its name.
//!
//! Every provider takes a client the caller built and owns, and implements
//! [`Streams`](crate::Streams) for lifecycle and [`Manager`](crate::Manager)
//! for each bound stream, exposing only the optional capabilities its backend
//! really has.

#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "mqtt")]
pub mod mqtt;
#[cfg(feature = "nats")]
pub mod nats;
#[cfg(feature = "rabbitmq")]
pub mod rabbitmq;
#[cfg(feature = "redis")]
pub mod redis;
#[cfg(feature = "zeromq")]
pub mod zeromq;
