//! Where a service listens: a TCP address (`127.0.0.1:8080`, Go's `:8080`), a Unix domain
//! socket named `unix:/run/agents/mcp.sock`, or a Windows named pipe named
//! `pipe:\\.\pipe\agents-mcp`.
//!
//! A local socket is a listener, not a transport: Streamable HTTP, SSE, A2A and gRPC all speak
//! HTTP, and HTTP runs the same over any byte stream. Choosing one moves who may call from who
//! can reach a port to who may open a file or a pipe, which is why both are created owner-only.
//!
//! [`LocalListener`] is the one place that knows how. The `grpc` crate serves gRPC on it and
//! this crate serves MCP and A2A on it, so the two cannot disagree on what "owner-only" means
//! or on what happens to a socket a crashed process left behind.
//!
//! # Platform seam
//!
//! Everything platform-specific is in one module chosen here, once: Unix sockets on Unix,
//! named pipes on Windows, and neither elsewhere. Each answers the other's address with
//! [`io::ErrorKind::Unsupported`] rather than failing to compile, so an address read from
//! configuration is an error a caller can report rather than a build break.

use std::future::Future;
use std::io;

use axum::Router;
use tokio::net::TcpListener;

mod local;

#[cfg_attr(unix, path = "sys/unix.rs")]
#[cfg_attr(windows, path = "sys/windows.rs")]
#[cfg_attr(not(any(unix, windows)), path = "sys/other.rs")]
mod sys;

pub use local::{LocalAddr, LocalListener, LocalStream};

/// The prefix that names a Unix domain socket rather than a TCP address.
pub const UNIX_PREFIX: &str = "unix:";

/// The prefix that names a Windows named pipe rather than a TCP address.
pub const PIPE_PREFIX: &str = "pipe:";

/// The socket path `addr` names, if it names one.
pub fn unix_path(addr: &str) -> Option<&str> {
    addr.strip_prefix(UNIX_PREFIX).filter(|p| !p.is_empty())
}

/// Encodes a socket path or pipe name as the authority of an `http+unix` URL.
#[cfg(any(feature = "mcp", feature = "a2a"))]
fn unix_authority(path: &str) -> String {
    use std::fmt::Write as _;
    let mut encoded = String::new();
    for byte in path.bytes() {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
            encoded.push(char::from(byte));
        } else {
            write!(encoded, "%{byte:02X}").expect("writing to a String cannot fail");
        }
    }
    encoded
}

/// The URL scheme and base a client reaches a local `addr` at, if it names one: a Unix socket
/// as `http+unix://`, a named pipe as `http+npipe://`, the schemes HTTP clients that speak
/// either accept, with the path or pipe name percent-encoded where a host would be.
#[cfg(any(feature = "mcp", feature = "a2a"))]
pub(crate) fn local_url_base(addr: &str) -> Option<(&'static str, String)> {
    let (scheme, name) = match LocalAddr::parse(addr)? {
        LocalAddr::Unix(_) => ("http+unix", unix_path(addr)?.to_owned()),
        LocalAddr::Pipe(name) => ("http+npipe", name),
    };
    Some((scheme, format!("{scheme}://{}", unix_authority(&name))))
}

/// A bound listener, not yet serving. Dropping a Unix one removes its socket file.
pub(crate) enum Bound {
    /// A TCP listener.
    Tcp(TcpListener),
    /// A Unix domain socket or a named pipe.
    Local(LocalListener),
}

impl Bound {
    /// Binds `addr`: a TCP address, Go's `:port` meaning every interface, `unix:PATH`, or
    /// `pipe:NAME`.
    ///
    /// # Errors
    ///
    /// The address could not be bound; for a local one also when a server already answers
    /// there, the path is not a socket, or this platform lacks that kind of endpoint.
    pub(crate) async fn bind(addr: &str) -> io::Result<Self> {
        if let Some(local) = LocalAddr::parse(addr) {
            return LocalListener::bind(&local).map(Self::Local);
        }
        let addr = if addr.starts_with(':') {
            format!("0.0.0.0{addr}")
        } else {
            addr.to_owned()
        };
        Ok(Self::Tcp(TcpListener::bind(addr).await?))
    }

    /// The TCP port this listener bound — the one the operating system chose when the
    /// address named port 0. `None` for a local endpoint, which has no port.
    pub(crate) fn port(&self) -> Option<u16> {
        match self {
            Self::Tcp(listener) => listener.local_addr().ok().map(|addr| addr.port()),
            Self::Local(_) => None,
        }
    }

    /// Serves `router` until `shutdown` resolves, then lets in-flight requests finish. A Unix
    /// socket's file is removed once serving stops.
    ///
    /// # Errors
    ///
    /// The listener failed while serving.
    pub(crate) async fn serve(
        self,
        router: Router,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> io::Result<()> {
        match self {
            Self::Tcp(listener) => {
                axum::serve(listener, router)
                    .with_graceful_shutdown(shutdown)
                    .await
            }
            // axum drops the listener when it stops accepting, which removes the socket.
            Self::Local(listener) => {
                axum::serve(listener, router)
                    .with_graceful_shutdown(shutdown)
                    .await
            }
        }
    }
}
