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
//! # Headers
//!
//! [`Headers`] travel as NATS headers, on core publishes and JetStream ones
//! alike, so `nats sub --headers-only` and `nats stream view` show them and a
//! Go peer still reads the payload untouched. A message without headers goes
//! out exactly as before: a plain `PUB` on core NATS, and only `Nats-Msg-Id`
//! on JetStream. The `Nats-` prefix is the server's (`Nats-Msg-Id`,
//! `Nats-TTL`, `Nats-Expected-*` change what it does with a message), so a
//! key starting with it is refused with
//! [`InvalidHeader`](crate::StreamError::InvalidHeader) at
//! [`create`](crate::Streams::create), [`update`](crate::Streams::update),
//! and every publish, and `Nats-` headers on a received message are left
//! off [`Message::headers`](crate::Message::headers).
//!
//! A stream's default headers live where the rest of its declaration does:
//! in the process for core NATS, and in the JetStream stream's metadata under
//! [`META_HEADERS`] as a JSON object, so they survive a server restart and
//! another process binding the stream stamps the same ones.
//!
//! # Expiry, retain, and delayed redelivery
//!
//! - [`Options::expires`](crate::Options::expires): JetStream sends it as the
//!   per-message `Nats-TTL` header, which the server (2.11 or later) honours
//!   on a stream with `allow_msg_ttl`, which every stream this provider
//!   creates or updates has. The server counts it in whole seconds, so an
//!   expiry is rounded up to the next second and one under a second is
//!   refused. Core NATS refuses it: nothing is held, so nothing can expire.
//! - [`Options::retain`](crate::Options::retain) is refused on both. Core
//!   NATS keeps nothing; JetStream's only last-value form is a stream capped
//!   at one message per subject, which would discard the log every durable
//!   consumer is reading.
//! - [`Delivery::nak_after`](crate::Delivery::nak_after) on JetStream is a
//!   `-NAK` with a delay: the server holds the redelivery back. Core NATS has
//!   no deliveries to settle.
//!
//! [`as_durable`]: crate::as_durable
//! [`Headers`]: crate::Headers

mod acks;
mod jetstream;
mod plain;

use std::{sync::Arc, time::Duration};

use opentelemetry::metrics::Meter;
use tracing::error;

use async_nats::HeaderMap;

use crate::{Headers, Options, Registry, Result, Stream, StreamError, provider};

pub use jetstream::{
    JetStreamManager, JetStreamProvider, META_ACTIVE, META_HEADERS, META_NAME, META_USER,
    safe_name,
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

    /// How long a JetStream durable consumer waits for a delivery to be
    /// settled before it sends the message again. `None` leaves it to the
    /// server, whose default is 30 seconds.
    ///
    /// Like [`max_deliver`](Self::max_deliver), it is written when the
    /// consumer is created: a durable consumer that already exists on the
    /// server keeps the settings it was created with.
    pub ack_wait: Option<Duration>,

    /// How many times a JetStream durable consumer delivers one message
    /// before it gives up on it. `None` (or `Some(0)`) leaves it to the server,
    /// whose default is no limit. Applies at creation; see
    /// [`ack_wait`](Self::ack_wait).
    pub max_deliver: Option<u32>,

    /// How long the server keeps the ordered consumer behind a JetStream
    /// [`subscribe`](crate::Subscriber::subscribe) once nobody pulls from it,
    /// which is how a dropped subscription's consumer is cleaned up. `None`
    /// keeps async-nats' default of 30 seconds.
    ///
    /// It applies to the consumer a subscription starts with. When the
    /// ordered consumer has to replace itself (after a sequence gap or two
    /// missed heartbeats), async-nats creates the replacement with its own
    /// 30-second default, because its ordered configuration has no field for
    /// this.
    pub inactive_threshold: Option<Duration>,
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

    /// Redelivers an unsettled durable delivery after `wait`.
    pub fn with_ack_wait(mut self, wait: Duration) -> Self {
        self.ack_wait = Some(wait);
        self
    }

    /// Stops redelivering a durable message after `attempts` deliveries.
    pub fn with_max_deliver(mut self, attempts: u32) -> Self {
        self.max_deliver = Some(attempts);
        self
    }

    /// Lets the server discard an idle subscription's ordered consumer after
    /// `threshold`.
    pub fn with_inactive_threshold(mut self, threshold: Duration) -> Self {
        self.inactive_threshold = Some(threshold);
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

/// The subject of the messages a subscription receives, held once.
///
/// A subscription to a literal subject sees the same subject on every message,
/// and one to a pattern usually sees a few. Each message is unpacked against
/// the last one seen, so it shares that copy instead of allocating its own.
#[derive(Default)]
struct LastSubject(Option<Arc<str>>);

impl LastSubject {
    fn of(&mut self, subject: &str) -> &Arc<str> {
        if self.0.as_deref() != Some(subject) {
            self.0 = Some(Arc::from(subject));
        }
        self.0.as_ref().expect("just set")
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

/// The header prefix the NATS server keeps for itself.
const RESERVED: &[&str] = &["Nats-"];

/// Refuses stream headers that are invalid or use the server's prefix, so a
/// bad default fails at the call that set it.
fn check_stream_headers(stream: &Stream) -> Result<()> {
    stream.headers.validate()?;
    stream.headers.reject_reserved("NATS", RESERVED)
}

/// The headers a publish sends, validated, with the server's prefix refused.
fn outgoing(stream: &Stream, opts: &Options) -> Result<Headers> {
    let headers = provider::outgoing(stream, opts)?;
    headers.reject_reserved("NATS", RESERVED)?;
    Ok(headers)
}

/// Refuses [`Options::retain`]: neither form of NATS has a last value that
/// would not change what the stream is.
fn refuse_retain(opts: &Options, why: &str) -> Result<()> {
    if opts.retain {
        return Err(StreamError::Unsupported(format!(
            "NATS cannot retain a message as its subject's last value: {why}"
        )));
    }
    Ok(())
}

/// `headers` as a NATS header map. Every key has passed
/// [`Headers::validate`], whose rules are the client's own, so none panics.
fn to_header_map(headers: &Headers) -> HeaderMap {
    let mut map = HeaderMap::new();
    for (key, value) in headers.iter() {
        map.insert(key, value);
    }
    map
}

/// The headers a message arrived with, without the server's own `Nats-` ones.
/// A key sent more than once keeps its first value.
fn from_header_map(map: Option<&HeaderMap>) -> Headers {
    let Some(map) = map else {
        return Headers::new();
    };
    map.iter()
        .filter(|(name, _)| {
            let name: &str = name.as_ref();
            !RESERVED.iter().any(|p| name.starts_with(p))
        })
        .filter_map(|(name, values)| {
            let name: &str = name.as_ref();
            values.first().map(|v| (name.to_owned(), v.as_str().to_owned()))
        })
        .collect()
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
    fn last_subject_is_shared_until_it_changes() {
        let mut last = LastSubject::default();
        let a = last.of("orders.placed").clone();
        assert!(Arc::ptr_eq(&a, last.of("orders.placed")));
        assert_eq!(&**last.of("orders.shipped"), "orders.shipped");
        assert!(!Arc::ptr_eq(&a, last.of("orders.placed")));
    }

    #[test]
    fn headers_map_both_ways_without_the_servers_own() {
        let headers: Headers = [("lang", "rust"), ("traceparent", "00-ab")]
            .into_iter()
            .collect();
        let mut map = to_header_map(&headers);
        assert_eq!(map.get("lang").map(|v| v.as_str()), Some("rust"));
        map.insert("Nats-Msg-Id", "m-1");
        map.insert("Nats-TTL", "5");
        assert_eq!(from_header_map(Some(&map)), headers);
        assert!(from_header_map(None).is_empty());
    }

    #[test]
    fn the_nats_prefix_is_reserved() {
        let s = Stream::named("s").subjects(["a"]).header("Nats-Msg-Id", "x");
        let error = check_stream_headers(&s).unwrap_err();
        assert!(matches!(error, StreamError::InvalidHeader(_)), "{error}");
        let s = Stream::named("s").subjects(["a"]).header("bad key", "x");
        assert!(matches!(
            check_stream_headers(&s),
            Err(StreamError::InvalidHeader(_))
        ));
        let s = Stream::named("s").subjects(["a"]).header("lang", "rust");
        assert!(check_stream_headers(&s).is_ok());
        let error = outgoing(&s, &Options::default().header("Nats-TTL", "1")).unwrap_err();
        assert!(matches!(error, StreamError::InvalidHeader(_)), "{error}");
        let sent = outgoing(&s, &Options::default().header("lang", "go")).unwrap();
        assert_eq!(sent.get("lang"), Some("go"));
    }

    #[test]
    fn retain_is_refused_by_name() {
        assert!(refuse_retain(&Options::default(), "x").is_ok());
        let error = refuse_retain(&Options::default().with_retain(), "why").unwrap_err();
        assert!(
            matches!(&error, StreamError::Unsupported(e) if e.contains("retain") && e.contains("why")),
            "{error}"
        );
    }

    #[test]
    fn config_builders() {
        let config = NatsConfig::default()
            .with_queue_group("workers")
            .with_registry(Registry::new().with("protobuf"));
        assert_eq!(config.queue_group.as_deref(), Some("workers"));
        assert!(config.registry.lookup("protobuf").is_ok());
        assert!(config.meter.is_none());

        let config = NatsConfig::default()
            .with_ack_wait(Duration::from_secs(2))
            .with_max_deliver(3)
            .with_inactive_threshold(Duration::from_secs(4));
        assert_eq!(config.ack_wait, Some(Duration::from_secs(2)));
        assert_eq!(config.max_deliver, Some(3));
        assert_eq!(config.inactive_threshold, Some(Duration::from_secs(4)));
    }

    #[test]
    fn jetstream_settings_default_to_the_server() {
        let config = NatsConfig::default();
        assert!(config.ack_wait.is_none());
        assert!(config.max_deliver.is_none());
        assert!(config.inactive_threshold.is_none());
    }
}
