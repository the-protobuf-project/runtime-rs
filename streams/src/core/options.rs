//! Per-operation settings.

use std::time::Duration;

/// The per-operation settings a provider understands.
///
/// A provider ignores a setting that only tunes delivery and it cannot honor,
/// so a call written against one backend still runs against another. Settings
/// that change what the call means, a delayed delivery or a shared group, are
/// refused with [`StreamError::Unsupported`](crate::StreamError::Unsupported)
/// instead of being silently dropped.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Options {
    /// Delays delivery until it elapses, for providers that can schedule.
    /// `None` publishes immediately.
    pub ttl: Option<Duration>,

    /// Publishes under a chosen identifier instead of a generated one.
    pub id: Option<String>,

    /// Makes several subscribers share a subject rather than each receiving
    /// every message: a Kafka consumer group, a NATS queue group.
    ///
    /// It changes the delivery semantics rather than tuning them. Without a
    /// group a subject fans out and a second consumer doubles the work; with
    /// one it is shared and a second consumer halves the time.
    pub group: Option<String>,

    /// How many messages a provider may hold ahead of the reader. `None` lets
    /// the provider choose; see [`provider::prefetch`](crate::provider::prefetch).
    ///
    /// A larger number keeps a fast consumer fed; it also means more messages
    /// held unacknowledged, every one of which is redelivered if the process
    /// dies.
    pub prefetch: Option<usize>,

    /// Decides which partition a message lands on, and so which messages are
    /// ordered relative to each other. Providers with no partitions ignore it.
    pub partition_key: Option<String>,
}

impl Options {
    /// Delays delivery until `ttl` elapses.
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Publishes under a chosen identifier.
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Shares the subject among subscribers in `group`.
    pub fn with_group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }

    /// Lets the provider hold `n` messages ahead of the reader.
    pub fn with_prefetch(mut self, n: usize) -> Self {
        self.prefetch = Some(n);
        self
    }

    /// Orders this message with every other carrying the same key.
    pub fn with_partition_key(mut self, key: impl Into<String>) -> Self {
        self.partition_key = Some(key.into());
        self
    }

    /// A non-zero TTL, treating `Some(ZERO)` the way Go treats zero.
    pub fn delay(&self) -> Option<Duration> {
        self.ttl.filter(|ttl| !ttl.is_zero())
    }
}
