//! The Redis key layout, byte-for-byte runtime-go's `streams/redis/keys.go`.
//!
//! A Go service and a Rust service pointed at one Redis under one prefix see
//! the same streams, publish on the same channels, and join the same consumer
//! groups, because every key below is spelled exactly as Go spells it.

/// Separates the delivery modes so an immediate stream, a scheduled one, and a
/// durable one never collide on a key, even in one database under one prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Kind {
    /// Immediate delivery over pub/sub.
    Stream,
    /// Delivery on TTL expiry, over keyspace notifications.
    Notify,
    /// Durable delivery over Redis Streams.
    Durable,
}

impl Kind {
    /// The namespace segment Go writes for this kind.
    fn as_str(self) -> &'static str {
        match self {
            Self::Stream => "stream",
            Self::Notify => "notify",
            Self::Durable => "durable",
        }
    }
}

/// The field a stream's metadata is stored under, inside the Redis stream key
/// that represents it.
pub(super) const METADATA_FIELD: &str = "meta";

/// The field a durable message's frame is stored under.
///
/// One field rather than a flat map of the caller's values: the body is a frame
/// this crate packed, and spreading it across fields would make the entry's
/// shape depend on the payload's.
pub(super) const PAYLOAD_FIELD: &str = "msg";

/// Builds every key one provider uses. The prefix and kind are baked in at
/// construction, so an operation asks for "the key for this id" and cannot
/// reach another provider's keyspace.
#[derive(Clone, Debug)]
pub(super) struct Keys {
    base: String,
}

impl Keys {
    /// Keys under `prefix` (may be empty) for one delivery mode.
    pub(super) fn new(prefix: &str, kind: Kind) -> Self {
        let mut base = String::new();
        if !prefix.is_empty() {
            base.push_str(prefix);
            base.push(':');
        }
        base.push_str(kind.as_str());
        base.push(':');
        Self { base }
    }

    /// The key holding a stream's metadata.
    pub(super) fn stream(&self, id: &str) -> String {
        format!("{}meta:{id}", self.base)
    }

    /// Matches every stream metadata key in this namespace.
    pub(super) fn stream_pattern(&self) -> String {
        format!("{}meta:*", self.base)
    }

    /// Recovers a stream id from a key built by [`Keys::stream`].
    pub(super) fn id_from_stream<'a>(&self, key: &'a str) -> &'a str {
        let prefix = format!("{}meta:", self.base);
        key.strip_prefix(prefix.as_str()).unwrap_or(key)
    }

    /// The pub/sub channel a subject's messages travel on.
    pub(super) fn channel(&self, stream: &str, subject: &str) -> String {
        format!("{}ch:{stream}:{subject}", self.base)
    }

    /// The Redis stream key a durable subject's messages are appended to.
    ///
    /// One key per subject rather than per stream: a consumer group's position
    /// is a property of the key, so sharing one across subjects would make a
    /// group that wants one subject read, and acknowledge, all of them.
    pub(super) fn subject(&self, stream: &str, subject: &str) -> String {
        format!("{}s:{stream}:{subject}", self.base)
    }

    /// The key whose expiry is a scheduled message's delivery.
    ///
    /// An expiry event carries only a key name, so everything needed to route
    /// it is in the name; the message id keeps two scheduled messages on one
    /// subject from overwriting each other and resetting each other's timer.
    pub(super) fn pending(&self, stream: &str, subject: &str, id: &str) -> String {
        format!("{}{id}", self.pending_prefix(stream, subject))
    }

    /// What a scheduled subscriber matches an expiry event against.
    pub(super) fn pending_prefix(&self, stream: &str, subject: &str) -> String {
        format!("{}pending:{stream}:{subject}:", self.base)
    }

    /// The key holding a scheduled message's frame, which outlives the pending
    /// key announcing it: by the time the expiry fires, that key's value is gone.
    pub(super) fn payload(&self, id: &str) -> String {
        format!("{}payload:{id}", self.base)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_go_layout() {
        let keys = Keys::new("app", Kind::Stream);
        assert_eq!(keys.stream("s1"), "app:stream:meta:s1");
        assert_eq!(keys.stream_pattern(), "app:stream:meta:*");
        assert_eq!(
            keys.channel("s1", "user.created"),
            "app:stream:ch:s1:user.created"
        );

        let keys = Keys::new("app", Kind::Durable);
        assert_eq!(
            keys.subject("s1", "order.placed"),
            "app:durable:s:s1:order.placed"
        );

        let keys = Keys::new("app", Kind::Notify);
        assert_eq!(
            keys.pending("s1", "reminder", "m1"),
            "app:notify:pending:s1:reminder:m1"
        );
        assert_eq!(
            keys.pending_prefix("s1", "reminder"),
            "app:notify:pending:s1:reminder:"
        );
        assert_eq!(keys.payload("m1"), "app:notify:payload:m1");
    }

    #[test]
    fn an_empty_prefix_adds_no_separator() {
        let keys = Keys::new("", Kind::Stream);
        assert_eq!(keys.stream("s1"), "stream:meta:s1");
    }

    #[test]
    fn kinds_never_share_a_key() {
        let a = Keys::new("p", Kind::Stream).stream("x");
        let b = Keys::new("p", Kind::Notify).stream("x");
        let c = Keys::new("p", Kind::Durable).stream("x");
        assert_ne!(a, b);
        assert_ne!(b, c);
        assert_ne!(a, c);
    }

    #[test]
    fn recovers_a_stream_id() {
        let keys = Keys::new("p", Kind::Stream);
        let id = "01J0000000000000000000000:users";
        assert_eq!(keys.id_from_stream(&keys.stream(id)), id);
    }
}
