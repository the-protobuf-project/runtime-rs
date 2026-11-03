//! The gRPC listener: a TCP socket or a local endpoint, with [`GrpcLimits`] applied to every
//! connection it accepts.
//!
//! tonic serves whatever stream of connections it is handed, so this module's whole job is
//! to produce that stream. Binding a local endpoint is [`agents::listen::LocalListener`]'s —
//! the same code the agent protocols listen with — so a Unix socket or a named pipe behaves
//! here exactly as it does for MCP: owner-only, stale socket replaced, live one refused,
//! removed when serving stops.

mod conn;

use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use agents::listen::{LocalAddr, LocalListener};
use futures_util::Stream;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::options::{Endpoint, GrpcLimits, Overflow};

pub(crate) use conn::Conn;

/// Where the gRPC listener is bound, as [`crate::HybridServer::grpc_listen_addr`] reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ListenAddr {
    /// A TCP socket, with the port the operating system chose when the options named 0.
    Tcp(SocketAddr),
    /// A Unix domain socket or a Windows named pipe.
    Local(LocalAddr),
}

impl ListenAddr {
    /// The TCP address, if this is one.
    pub fn tcp(&self) -> Option<SocketAddr> {
        match self {
            Self::Tcp(addr) => Some(*addr),
            Self::Local(_) => None,
        }
    }

    /// The local endpoint, if this is one.
    pub fn local(&self) -> Option<&LocalAddr> {
        match self {
            Self::Tcp(_) => None,
            Self::Local(addr) => Some(addr),
        }
    }
}

impl fmt::Display for ListenAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tcp(addr) => addr.fmt(f),
            Self::Local(addr) => addr.fmt(f),
        }
    }
}

/// A bound gRPC listener, not yet accepting.
pub(crate) enum Listener {
    Tcp(TcpListener),
    Local(LocalListener),
}

/// How long to wait before accepting again after `accept` fails: usually the process is out
/// of descriptors, and tonic retries a failed accept at once, which would spin a core against
/// a limit only closes clear.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

impl Listener {
    /// Binds `endpoint`: its local endpoint when it names one, else its `host:port`.
    pub(crate) async fn bind(endpoint: &Endpoint) -> io::Result<Self> {
        match endpoint.local() {
            Some(local) => LocalListener::bind(&local).map(Self::Local),
            None => TcpListener::bind(endpoint.addr()).await.map(Self::Tcp),
        }
    }

    /// Where this listener is bound.
    pub(crate) fn addr(&self) -> io::Result<ListenAddr> {
        match self {
            Self::Tcp(listener) => listener.local_addr().map(ListenAddr::Tcp),
            Self::Local(listener) => Ok(ListenAddr::Local(listener.local_addr().clone())),
        }
    }

    /// The connections this listener accepts, as tonic consumes them.
    ///
    /// The stream never yields an error: a failed accept is logged and retried after a
    /// pause. Every connection carries `abort`, which cuts it when a drain runs out of time.
    pub(crate) fn incoming(
        self,
        limits: GrpcLimits,
        abort: CancellationToken,
    ) -> impl Stream<Item = io::Result<Conn>> + Send + 'static {
        let accepting = Accepting {
            listener: self,
            slots: limits.max_connections.map(|n| Arc::new(Semaphore::new(n))),
            limits,
            abort,
        };
        futures_util::stream::unfold(accepting, |mut accepting| async move {
            let conn = accepting.next().await;
            Some((Ok(conn), accepting))
        })
    }
}

/// The state behind [`Listener::incoming`].
struct Accepting {
    listener: Listener,
    /// One permit per connection allowed open, when there is a limit.
    slots: Option<Arc<Semaphore>>,
    limits: GrpcLimits,
    abort: CancellationToken,
}

impl Accepting {
    /// The next connection admitted under the limits.
    async fn next(&mut self) -> Conn {
        loop {
            // Queueing waits for a slot before accepting, so the client waits in the
            // operating system's backlog rather than on a socket nobody reads.
            let queued = match (&self.slots, self.limits.overflow) {
                (Some(slots), Overflow::Queue) => Arc::clone(slots).acquire_owned().await.ok(),
                _ => None,
            };
            let io = match self.accept().await {
                Ok(io) => io,
                Err(err) => {
                    tracing::warn!(error = %err, "gRPC: accept failed; backing off");
                    tokio::time::sleep(ACCEPT_BACKOFF).await;
                    continue;
                }
            };
            let slot = match (&self.slots, queued) {
                (_, Some(slot)) => Some(slot),
                (None, None) => None,
                (Some(slots), None) => match Arc::clone(slots).try_acquire_owned() {
                    Ok(slot) => Some(slot),
                    Err(_) => {
                        tracing::debug!("gRPC: at the connection limit; refusing a client");
                        drop(io);
                        continue;
                    }
                },
            };
            return Conn::new(io, slot, self.limits.idle_timeout, self.abort.clone());
        }
    }

    async fn accept(&mut self) -> io::Result<conn::Io> {
        match &mut self.listener {
            Listener::Tcp(listener) => listener
                .accept()
                .await
                .map(|(stream, _)| conn::Io::Tcp(stream)),
            Listener::Local(listener) => listener.accept().await.map(conn::Io::Local),
        }
    }
}
