//! Streams over NATS, through the official `async-nats` client, in two forms.
//!
//! [`NatsProvider`] is core NATS: a message goes to whoever is listening at
//! that moment and is then gone. It implements [`Publisher`](crate::Publisher)
//! and [`Subscriber`](crate::Subscriber), plus [`Batch`](crate::Batch) by
//! publishing each payload in turn, and nothing beyond them, because that is all
//! core NATS does: there is no log behind it to replay and no record of what
//! anyone handled.
//!
//! [`JetStreamProvider`] is JetStream: the same subjects, backed by a stored
//! log. Its manager implements [`Durable`](crate::Durable) and
//! [`Positioned`](crate::Positioned) as well, so a consumer can be named,
//! resume where that name left off, and have a message redelivered until it
//! says the work is done.
//!
//! # Two types, not a flag
//!
//! The difference is not a setting. A program that believes it has
//! at-least-once delivery and actually has at-most-once loses messages under
//! exactly the conditions it added the durability for. [`as_durable`] is what
//! tells the two apart: it succeeds on the JetStream manager and refuses, by
//! name, on the core one. runtime-go makes the same split with `Use` and
//! `UseJetStream`.
//!
//! # The client is yours
//!
//! Both providers take an [`async_nats::Client`] the caller connected, with
//! whatever credentials, TLS, and reconnect handling it needs. Nothing here
//! dials it, drains it, or closes it.
//!
//! # Wire compatibility
//!
//! Payloads are [`pack`](crate::provider::pack)ed frames, a JetStream publish
//! carries the message id in `Nats-Msg-Id`, and a JetStream stream keeps the
//! fields its config has no room for under runtime-go's metadata keys. A Go
//! service and a Rust one share a server, a stream, and a durable consumer.
//!
//! [`as_durable`]: crate::as_durable

mod jetstream;
mod plain;

use opentelemetry::metrics::Meter;
use tracing::error;

use crate::{Registry, Result, Stream, provider};

pub use jetstream::{
    JetStreamManager, JetStreamProvider, META_ACTIVE, META_NAME, META_USER, safe_name,
};
pub use plain::{NatsManager, NatsProvider};

/// Settings shared by [`NatsProvider`] and [`JetStreamProvider`].
///
/// There is no codec setting, unlike runtime-go's `WithCodec`: a
/// [`Payload`](crate::Payload) already carries the codec that wrote it, so
/// what this provider needs to know is only which codecs it may *read*.
#[derive(Clone, Default)]
pub struct NatsConfig {
    /// The codecs a subscriber accepts. A frame naming any other is dropped,
    /// or terminated on a durable consumer, rather than delivered as bytes
    /// nobody can read. JSON is always accepted.
    pub registry: Registry,

    /// Where durable deliveries and settlements are counted. `None` records
    /// nothing.
    pub meter: Option<Meter>,

    /// A NATS queue group every core subscription joins, so several
    /// subscribers share a subject instead of each receiving every message.
    ///
    /// [`Options::group`](crate::Options::group) on a subscribe call wins over
    /// it: the call is the more specific statement, and this only sets the
    /// default. [`JetStreamProvider::new`] refuses it, because a named durable
    /// consumer already shares a subject, and a queue group layered on top
    /// would be a second answer to the same question.
    pub queue_group: Option<String>,
}

impl NatsConfig {
    /// Accepts frames written by the codecs in `registry`.
    pub fn with_registry(mut self, registry: Registry) -> Self {
        self.registry = registry;
        self
    }

    /// Counts durable deliveries and settlements on `meter`.
    pub fn with_meter(mut self, meter: Meter) -> Self {
        self.meter = Some(meter);
        self
    }

    /// Joins every core subscription to the queue group `name`.
    pub fn with_queue_group(mut self, name: impl Into<String>) -> Self {
        self.queue_group = Some(name.into());
        self
    }

    /// The delivery instruments this configuration asks for.
    fn metrics(&self) -> provider::Metrics {
        self.meter
            .as_ref()
            .map(provider::Metrics::new)
            .unwrap_or_default()
    }
}

/// Rejects a subject `stream` does not declare.
///
/// A NATS subject is a pattern, so a stream declaring `orders.*` accepts
/// `orders.placed`: it is matched with
/// [`declares_pattern`](provider::declares_pattern) rather than by name.
fn check_subject(stream: &Stream, subject: &str) -> Result<()> {
    if provider::declares_pattern(&stream.subjects, subject) {
        return Ok(());
    }
    error!(
        subject,
        stream = %stream.id,
        declared = ?stream.subjects,
        "subject is not declared by this stream"
    );
    Err(provider::unknown_subject(
        &stream.id,
        subject,
        &stream.subjects,
    ))
}

/// Rejects a publish to a subject that is a wildcard.
///
/// A stream may declare a wildcard, but a message has to land somewhere
/// specific. NATS would reject this at the server; refusing here names the
/// call that did it.
fn check_concrete(stream: &Stream, subject: &str) -> Result<()> {
    if !is_wildcard(subject) {
        return Ok(());
    }
    error!(
        subject,
        stream = %stream.id,
        "cannot publish to a wildcard subject, only subscribe to it"
    );
    Err(provider::unknown_subject(
        &stream.id,
        subject,
        &stream.subjects,
    ))
}

/// Whether `subject` contains a NATS wildcard token.
fn is_wildcard(subject: &str) -> bool {
    subject.contains(['*', '>'])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StreamError;

    fn stream(subjects: &[&str]) -> Stream {
        Stream {
            id: "s-1".into(),
            subjects: subjects.iter().map(|s| (*s).to_owned()).collect(),
            ..Stream::default()
        }
    }

    #[test]
    fn subjects_are_patterns() {
        let s = stream(&["orders.*", "events.>"]);
        assert!(check_subject(&s, "orders.placed").is_ok());
        assert!(check_subject(&s, "orders.*").is_ok());
        assert!(check_subject(&s, "events.us.signup").is_ok());
        let error = check_subject(&s, "orders.us.placed").unwrap_err();
        assert!(
            matches!(error, StreamError::UnknownSubject { .. }),
            "{error}"
        );
    }

    #[test]
    fn wildcards_cannot_be_published_to() {
        let s = stream(&["orders.*"]);
        assert!(check_concrete(&s, "orders.placed").is_ok());
        for subject in ["orders.*", "orders.>", ">"] {
            let error = check_concrete(&s, subject).unwrap_err();
            assert!(
                matches!(error, StreamError::UnknownSubject { .. }),
                "{subject}"
            );
        }
    }

    #[test]
    fn config_builders() {
        let config = NatsConfig::default()
            .with_queue_group("workers")
            .with_registry(Registry::new().with("protobuf"));
        assert_eq!(config.queue_group.as_deref(), Some("workers"));
        assert!(config.registry.lookup("protobuf").is_ok());
        assert!(config.meter.is_none());
    }
}
