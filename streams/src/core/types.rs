//! Values that cross the stream contract: stream metadata and delivered messages.

use std::{fmt, ops::Deref};

use bytes::Bytes;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{Codec, Json, Result, StreamError};

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
}

/// One delivered payload.
///
/// The data is left encoded so a subscriber decodes into its own model with
/// [`Message::decode`] rather than receiving a shape this crate chose.
#[derive(Clone, PartialEq, Eq)]
pub struct Message {
    id: String,
    subject: String,
    codec: String,
    data: Bytes,
}

impl Message {
    /// Assembles a delivered message. Providers build one through
    /// [`provider::unpack`](crate::provider::unpack) rather than calling this
    /// directly.
    pub fn new(
        id: impl Into<String>,
        subject: impl Into<String>,
        codec: impl Into<String>,
        data: impl Into<Bytes>,
    ) -> Self {
        Self {
            id: id.into(),
            subject: subject.into(),
            codec: codec.into(),
            data: data.into(),
        }
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
        if codec.name() != self.codec {
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
