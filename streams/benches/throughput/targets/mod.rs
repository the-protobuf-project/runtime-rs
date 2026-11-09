//! How the bench reaches each backend. One file per backend, so each can grow
//! its own profiles (`plain`, `tls`, ...) without touching the others.

use futures::future::BoxFuture;
use streams::{Registry, Streams};

#[cfg(feature = "kafka")]
mod kafka;
#[cfg(feature = "mqtt")]
mod mqtt;
#[cfg(feature = "nats")]
mod nats;
#[cfg(feature = "opcua")]
mod opcua;
#[cfg(feature = "opcua")]
mod opcua_pubsub;
#[cfg(feature = "rabbitmq")]
mod rabbitmq;
#[cfg(feature = "redis")]
mod redis;
#[cfg(feature = "zeromq")]
mod zeromq;

/// A connected provider, plus a second one for backends whose publishing and
/// subscribing sides are separate objects.
pub struct Conn {
    pub publisher: Box<dyn Streams>,
    pub subscriber: Option<Box<dyn Streams>>,
    /// Pushes out whatever the publishing connection still buffers. A
    /// publish-only run awaits it before stopping the clock, since a backend
    /// whose publish returns once the message is queued in the client (core
    /// NATS) would otherwise be timed before its bytes left.
    pub flush: Option<Flush>,
}

/// A publishing connection's flush.
pub type Flush = Box<dyn Fn() -> BoxFuture<'static, Result<(), Error>> + Send + Sync>;

impl Conn {
    /// A backend whose one provider both publishes and subscribes, with no flush.
    #[allow(dead_code)] // every backend but NATS uses it
    pub fn one(streams: impl Streams + 'static) -> Self {
        Self {
            publisher: Box::new(streams),
            subscriber: None,
            flush: None,
        }
    }
}

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Connects `backend` with `profile`. Backend names:
/// kafka, nats (core), jetstream, rabbitmq, redis (pub/sub), redis-durable,
/// mqtt, zeromq, opcua, opcua-pubsub.
pub async fn connect(backend: &str, profile: &str, registry: Registry) -> Result<Conn, Error> {
    let _ = (profile, &registry);
    match backend {
        #[cfg(feature = "kafka")]
        "kafka" => kafka::connect(profile, registry).await,
        #[cfg(feature = "nats")]
        "nats" => nats::connect(false, profile, registry).await,
        #[cfg(feature = "nats")]
        "jetstream" => nats::connect(true, profile, registry).await,
        #[cfg(feature = "rabbitmq")]
        "rabbitmq" => rabbitmq::connect(profile, registry).await,
        #[cfg(feature = "redis")]
        "redis" => redis::connect(false, profile, registry).await,
        #[cfg(feature = "redis")]
        "redis-durable" => redis::connect(true, profile, registry).await,
        #[cfg(feature = "mqtt")]
        "mqtt" => mqtt::connect(profile, registry).await,
        #[cfg(feature = "zeromq")]
        "zeromq" => zeromq::connect(profile, registry).await,
        #[cfg(feature = "opcua")]
        "opcua" => opcua::connect(profile, registry).await,
        #[cfg(feature = "opcua")]
        "opcua-pubsub" => opcua_pubsub::connect(profile, registry).await,
        other => Err(format!("backend {other:?} is unknown or its feature is not enabled").into()),
    }
}

/// The refusal every backend gives for a profile it does not have yet.
pub fn unknown_profile(backend: &str, profile: &str) -> Error {
    format!("{backend}: no {profile:?} profile").into()
}

/// An environment variable, or a default.
pub fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}
