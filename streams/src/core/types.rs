//! Values that cross the stream contract: stream metadata and delivered messages.

use std::{fmt, ops::Deref, sync::Arc, time::Duration};

use bytes::Bytes;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{Codec, Headers, Json, Result, StreamError};

/// The metadata describing a stream and the subjects it accepts.
///
/// This is configuration, not payload: every provider needs the same facts to
/// create a stream, and the JSON field names match runtime-go so a stream one
/// runtime created can be read by the other.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct Stream {
    /// Identifies the stream. Providers assign one when `create` is given an
    /// empty id.
    #[serde(default)]
    pub id: String,
    /// Human-readable name, folded into a generated id.
    #[serde(default)]
    pub name: String,
    /// Free-form description.
    #[serde(default)]
    pub description: String,
    /// The subjects this stream accepts; nothing else can be published or
    /// subscribed to through it.
    #[serde(default)]
    pub subjects: Vec<String>,
    /// Owner of the stream, for providers that partition by user.
    #[serde(default)]
    pub user_id: String,
    /// Whether the stream is accepting traffic.
    #[serde(default)]
    pub active: bool,
    /// Headers stamped on every message published through this stream. A
    /// publish's own [`Options::headers`](crate::Options::headers) are laid on
    /// top, a key set there winning.
    ///
    /// Left out of the stored metadata when empty, so a stream without
    /// headers is stored exactly as runtime-go stores it.
    #[serde(default, skip_serializing_if = "Headers::is_empty")]
    pub headers: Headers,
}

impl Stream {
    /// A stream named `name`, to finish with the other builders and hand to
    /// [`Streams::create`](crate::Streams::create):
    ///
    /// ```
    /// let stream = streams::Stream::named("orders")
    ///     .subjects(["order.placed", "order.shipped"])
    ///     .header("lang", "rust")
    ///     .headers([("site", "plant-3"), ("team", "billing")]);
    /// assert_eq!(stream.headers.get("lang"), Some("rust"));
    /// ```
    pub fn named(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Self::default()
        }
    }

    /// Uses `id` instead of letting the provider generate one.
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }

    /// Accepts `subject` as well.
    pub fn subject(mut self, subject: impl Into<String>) -> Self {
        self.subjects.push(subject.into());
        self
    }

    /// Accepts each of `subjects` as well.
    pub fn subjects<S: Into<String>>(mut self, subjects: impl IntoIterator<Item = S>) -> Self {
        self.subjects.extend(subjects.into_iter().map(Into::into));
        self
    }

    /// Describes the stream.
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    /// Records who owns the stream.
    pub fn user(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = user_id.into();
        self
    }

    /// Marks the stream as accepting traffic, or not.
    pub fn active(mut self, active: bool) -> Self {
        self.active = active;
        self
    }

    /// Stamps `key: value` on every message published through the stream.
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key, value);
        self
    }

    /// Stamps each of `headers` on every message published through the stream.
    pub fn headers<K: Into<String>, V: Into<String>>(
        mut self,
        headers: impl IntoIterator<Item = (K, V)>,
    ) -> Self {
        self.headers.extend(headers);
        self
    }
}

/// One delivered payload.
///
/// The data is left encoded so a subscriber decodes into its own model with
/// [`Message::decode`] rather than receiving a shape this crate chose.
#[derive(Clone, PartialEq, Eq)]
pub struct Message {
    id: String,
    // Shared rather than owned: every message on a subscription carries the
    // same subject and codec name, and copying them was two allocations per
    // message on the receive path.
    subject: Arc<str>,
    codec: Arc<str>,
    data: Bytes,
    headers: Headers,
}

impl Message {
    /// Assembles a delivered message. Providers build one through
    /// [`provider::unpack`](crate::provider::unpack) rather than calling this
    /// directly.
    pub fn new(
        id: impl Into<String>,
        subject: impl Into<Arc<str>>,
        codec: impl Into<Arc<str>>,
        data: impl Into<Bytes>,
    ) -> Self {
        Self {
            id: id.into(),
            subject: subject.into(),
            codec: codec.into(),
            data: data.into(),
            headers: Headers::new(),
        }
    }

    /// The same message carrying `headers`. Providers attach what arrived
    /// with the message.
    pub fn with_headers(mut self, headers: Headers) -> Self {
        self.headers = headers;
        self
    }

    /// The headers the message was published with: the stream's defaults and
    /// the publish's own, as they arrived.
    pub fn headers(&self) -> &Headers {
        &self.headers
    }

    /// The publisher-assigned identifier.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The subject it arrived on.
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// The name of the codec the publisher encoded with.
    pub fn codec(&self) -> &str {
        &self.codec
    }

    /// The encoded payload. Prefer [`Message::decode`] over reading it.
    pub fn data(&self) -> &Bytes {
        &self.data
    }

    /// Decodes the payload with `codec`, refusing one the publisher did not use.
    ///
    /// A message decodes the way it was written: handing a protobuf frame to a
    /// JSON decoder fails with [`StreamError::Unsupported`] naming both, rather
    /// than with a parse error that hides why.
    pub fn decode<T, C: Codec<T> + ?Sized>(&self, codec: &C) -> Result<T> {
        if codec.name() != &*self.codec {
            return Err(StreamError::Unsupported(format!(
                "message {} was encoded with the {:?} codec and cannot be decoded as {:?}",
                self.id,
                self.codec,
                codec.name()
            )));
        }
        codec.decode(&self.data)
    }

    /// Decodes a JSON payload.
    pub fn json<T: Serialize + DeserializeOwned>(&self) -> Result<T> {
        self.decode(&Json)
    }
}

impl fmt::Debug for Message {
    // Payload bytes stay out of debug output, the same rule the logging
    // wrappers follow.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Message")
            .field("id", &self.id)
            .field("subject", &self.subject)
            .field("codec", &self.codec)
            .field("headers", &self.headers)
            .field("len", &self.data.len())
            .finish()
    }
}

/// Settles a lent message. Providers implement it; callers reach it through
/// [`Delivery::ack`] and [`Delivery::nak`].
///
/// Both methods consume the acker, so a delivery is settled at most once by
/// construction rather than by convention.
#[async_trait::async_trait]
pub trait Acker: Send + Sync {
    /// Reports the message as handled, so it is not delivered again.
    async fn ack(self: Box<Self>) -> Result<()>;

    /// Returns the message for redelivery.
    async fn nak(self: Box<Self>) -> Result<()>;

    /// Returns the message for redelivery no sooner than `delay` from now.
    ///
    /// Only a provider whose broker can hold a redelivery back implements
    /// it. The default naks at once and then reports that it could not wait:
    /// leaving the message unsettled instead would strand it, holding a
    /// prefetch slot until the broker gives up on the consumer.
    async fn nak_after(self: Box<Self>, delay: Duration) -> Result<()> {
        let _ = delay;
        self.nak().await?;
        Err(StreamError::Unsupported(
            "this provider cannot hold a redelivery back, so the message was returned for \
             redelivery at once"
                .into(),
        ))
    }

    /// Whether [`Acker::nak_after`] is implemented.
    fn delays_nak(&self) -> bool {
        false
    }

    /// The longest delay [`Acker::nak_after`] honours, where the broker caps
    /// it; `None` when there is no cap (or no delay at all, see
    /// [`Acker::delays_nak`]).
    fn max_nak_delay(&self) -> Option<Duration> {
        None
    }
}

/// A message that has to be settled.
///
/// It is a distinct type from [`Message`] because the difference is the whole
/// point: a Message has been handed over, and a Delivery has been lent. A
/// consumer that treats one as the other either loses work or replays it
/// forever. A Delivery dropped without settling is left to the backend's
/// redelivery, which is at-least-once doing what it promised.
pub struct Delivery {
    message: Message,
    attempt: u32,
    acker: Box<dyn Acker>,
}

impl Delivery {
    /// Assembles a lent message.
    pub fn new(message: Message, attempt: u32, acker: Box<dyn Acker>) -> Self {
        Self {
            message,
            attempt,
            acker,
        }
    }

    /// The message itself.
    pub fn message(&self) -> &Message {
        &self.message
    }

    /// How many times this message has been delivered, starting at 1.
    ///
    /// It is the one signal a consumer needs to break a redelivery loop it
    /// cannot otherwise detect. Zero where a provider cannot count.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Reports the message as handled, so it is not delivered again.
    ///
    /// Call it after the work is done rather than on receipt: acknowledging
    /// first turns at-least-once into at-most-once.
    pub async fn ack(self) -> Result<()> {
        self.acker.ack().await
    }

    /// Returns the message for redelivery, for work that failed in a way a
    /// retry could fix.
    ///
    /// A message naked forever is a poison message, and no backend here decides
    /// on its own when to stop trying. A consumer that cannot make progress
    /// should ack it and record the failure somewhere it will be seen.
    pub async fn nak(self) -> Result<()> {
        self.acker.nak().await
    }

    /// Whether [`Delivery::nak_after`] really waits on this delivery's
    /// provider. Where it does not, `nak_after` returns the message for
    /// redelivery at once and reports [`StreamError::Unsupported`].
    pub fn can_delay_redelivery(&self) -> bool {
        self.acker.delays_nak()
    }

    /// The longest delay [`Delivery::nak_after`] will wait on this
    /// delivery's provider, where the broker caps it (Redis Streams waits at
    /// most its reclaim interval); `None` when there is no cap. Read it before
    /// calling `nak_after`, which consumes the delivery.
    pub fn max_redelivery_delay(&self) -> Option<Duration> {
        if self.acker.delays_nak() {
            self.acker.max_nak_delay()
        } else {
            None
        }
    }

    /// Returns the message for redelivery no sooner than `delay` from now,
    /// for work that should wait before it is tried again (a device that is
    /// busy, a dependency that is restarting).
    ///
    /// Where the broker cannot hold a redelivery back, the message is
    /// returned for redelivery at once, as [`Delivery::nak`] would, and the
    /// call reports [`StreamError::Unsupported`] so the caller knows it did
    /// not wait; see [`Delivery::can_delay_redelivery`].
    pub async fn nak_after(self, delay: Duration) -> Result<()> {
        self.acker.nak_after(delay).await
    }
}

impl Deref for Delivery {
    type Target = Message;

    fn deref(&self) -> &Message {
        &self.message
    }
}

impl fmt::Debug for Delivery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Delivery")
            .field("message", &self.message)
            .field("attempt", &self.attempt)
            .finish_non_exhaustive()
    }
}

/// Where a positioned consumer starts reading.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Position {
    /// Messages published after the consumer attaches. The default, because it
    /// is the only position every backend can offer and the only one that
    /// cannot surprise a caller with a flood of history.
    #[default]
    New,
    /// The oldest message still retained. How far back that reaches is the
    /// server's retention, not this contract's.
    Earliest,
}
