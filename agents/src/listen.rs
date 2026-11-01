//! Where a service listens: a TCP address (`127.0.0.1:8080`, Go's `:8080`), or a Unix domain
//! socket named `unix:/run/agents/mcp.sock`.
//!
//! A Unix socket is a listener, not a transport: Streamable HTTP, SSE and A2A all speak HTTP,
//! and HTTP runs the same over either. Choosing it moves who may call from who can reach a
//! port to who may open a file, which is why the socket is created owner-only.

use std::future::Future;
use std::io;

use axum::Router;
use tokio::net::TcpListener;

/// The prefix that names a Unix domain socket rather than a TCP address.
pub const UNIX_PREFIX: &str = "unix:";

/// The socket path `addr` names, if it names one.
pub fn unix_path(addr: &str) -> Option<&str> {
    addr.strip_prefix(UNIX_PREFIX).filter(|p| !p.is_empty())
}

/// Encodes a socket path as the authority of an `http+unix` URL.
#[cfg(any(feature = "mcp", feature = "a2a"))]
pub(crate) fn unix_authority(path: &str) -> String {
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

/// A bound listener, not yet serving. Dropping a Unix one removes its socket file.
pub(crate) enum Bound {
    /// A TCP listener.
    Tcp(TcpListener),
    /// A Unix domain socket.
    #[cfg(unix)]
    Unix(crate::unix::Socket),
}

impl Bound {
    /// Binds `addr`: a TCP address, Go's `:port` meaning every interface, or `unix:PATH`.
    ///
    /// # Errors
    ///
    /// The address could not be bound; for a Unix socket also when a server already answers
    /// on the path, the path is not a socket, or this platform has no Unix sockets.
    pub(crate) async fn bind(addr: &str) -> io::Result<Self> {
        if let Some(path) = unix_path(addr) {
            return bind_unix(path);
        }
        let addr = if addr.starts_with(':') {
            format!("0.0.0.0{addr}")
        } else {
            addr.to_owned()
        };
        Ok(Self::Tcp(TcpListener::bind(addr).await?))
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
            #[cfg(unix)]
            Self::Unix(socket) => socket.serve(router, shutdown).await,
        }
    }
}

#[cfg(unix)]
fn bind_unix(path: &str) -> io::Result<Bound> {
    crate::unix::Socket::bind(std::path::Path::new(path)).map(Bound::Unix)
}

#[cfg(not(unix))]
fn bind_unix(path: &str) -> io::Result<Bound> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        format!("unix:{path} needs Unix domain sockets, which this platform lacks"),
    ))
}
