//! What the gRPC listener holds itself to: how many clients at once, what happens to the next
//! one, how long a silent connection lives, how long a stop waits, and the HTTP/2 settings
//! that bound what each connection may buffer.
//!
//! Every setting defaults to "as before": no connection limit, no idle timeout, no drain bound
//! beyond [`crate::HybridServer::with_shutdown_timeout`], and tonic's own HTTP/2 defaults. A
//! server that never calls [`crate::HybridServer::with_grpc_limits`] behaves exactly as it did
//! before these existed.
//!
//! Message size is not here, because tonic sets it per service rather than per server:
//! configure it on the generated server you register, with
//! `FooServer::new(svc).max_decoding_message_size(n)`, and hand that to
//! [`crate::HybridServer::with_service`].

use std::time::Duration;

/// What the listener does with a client that arrives while `max_connections` are open.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum Overflow {
    /// Stop accepting until a connection closes. The client waits in the operating system's
    /// backlog — the listen queue for a socket, busy retries for a named pipe — and is served
    /// in turn. The default: a burst of clients is delayed rather than failed.
    #[default]
    Queue,
    /// Accept and close at once. The client sees the connection drop and can retry or report
    /// it; nothing waits on the server's behalf.
    Refuse,
}

/// Limits and HTTP/2 settings for the gRPC listener, TCP or local alike.
///
/// Built from [`GrpcLimits::default`] with the setters below, so adding a setting later
/// breaks no caller. Plain data: `Send`, `Sync`, `Copy`. The limits apply to clients of the
/// listener; calls through the in-process [`crate::Loopback`] open no connection and are not
/// counted.
///
/// ```
/// use std::time::Duration;
/// use grpc::options::{GrpcLimits, Overflow};
///
/// let limits = GrpcLimits::default()
///     .max_connections(64)
///     .overflow(Overflow::Refuse)
///     .idle_timeout(Duration::from_secs(600))
///     .drain_timeout(Duration::from_secs(10))
///     .max_concurrent_streams(16)
///     .initial_connection_window_size(1 << 20)
///     .initial_stream_window_size(1 << 16);
/// assert_eq!(limits.connection_limit(), Some(64));
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GrpcLimits {
    pub(crate) max_connections: Option<usize>,
    pub(crate) overflow: Overflow,
    pub(crate) idle_timeout: Option<Duration>,
    pub(crate) drain_timeout: Option<Duration>,
    pub(crate) max_concurrent_streams: Option<u32>,
    pub(crate) initial_stream_window_size: Option<u32>,
    pub(crate) initial_connection_window_size: Option<u32>,
}

impl GrpcLimits {
    /// At most `n` connections open at once; what happens to the next is [`Overflow`]'s.
    /// Zero is treated as one, since a listener that admits nobody is a misconfiguration
    /// nobody would notice until the first client hung.
    #[must_use]
    pub fn max_connections(mut self, n: usize) -> Self {
        self.max_connections = Some(n.max(1));
        self
    }

    /// What a client arriving at the connection limit gets. Without a limit it never applies.
    #[must_use]
    pub fn overflow(mut self, overflow: Overflow) -> Self {
        self.overflow = overflow;
        self
    }

    /// Closes a connection that has neither read nor written a byte for `timeout`.
    ///
    /// Traffic is bytes on the wire, so an HTTP/2 PING counts and a call waiting silently on
    /// a slow handler does not: a long, quiet streaming call needs keep-alives or a longer
    /// timeout.
    #[must_use]
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = Some(timeout);
        self
    }

    /// How long a stop lets in-flight calls finish before their connections are cut.
    ///
    /// A stop first sends every connection GOAWAY and waits; past this bound the connections
    /// still open fail on their next read or write and the stop goes on. Without it the wait
    /// is bounded only by [`crate::HybridServer::with_shutdown_timeout`], which cuts off the
    /// whole server rather than just the gRPC connections.
    #[must_use]
    pub fn drain_timeout(mut self, timeout: Duration) -> Self {
        self.drain_timeout = Some(timeout);
        self
    }

    /// The HTTP/2 `SETTINGS_MAX_CONCURRENT_STREAMS` each connection advertises: how many
    /// calls one client may have in flight at once.
    #[must_use]
    pub fn max_concurrent_streams(mut self, n: u32) -> Self {
        self.max_concurrent_streams = Some(n);
        self
    }

    /// The HTTP/2 flow-control window each call starts with, in bytes: how much one call's
    /// client may send before the server has read it.
    #[must_use]
    pub fn initial_stream_window_size(mut self, bytes: u32) -> Self {
        self.initial_stream_window_size = Some(bytes);
        self
    }

    /// The HTTP/2 flow-control window each connection starts with, in bytes: how much all of
    /// one client's calls together may send before the server has read it.
    #[must_use]
    pub fn initial_connection_window_size(mut self, bytes: u32) -> Self {
        self.initial_connection_window_size = Some(bytes);
        self
    }

    /// The connection limit, if one is set.
    pub fn connection_limit(&self) -> Option<usize> {
        self.max_connections
    }

    /// The overflow policy.
    pub fn overflow_policy(&self) -> Overflow {
        self.overflow
    }

    /// The idle timeout, if one is set.
    pub fn idle(&self) -> Option<Duration> {
        self.idle_timeout
    }

    /// The drain bound, if one is set.
    pub fn drain(&self) -> Option<Duration> {
        self.drain_timeout
    }
}
