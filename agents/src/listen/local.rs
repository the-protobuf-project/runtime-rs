//! A local endpoint — a Unix domain socket or a Windows named pipe — as one listener type.
//!
//! The rules are the same on both platforms, stated once here and kept by `sys`:
//!
//! - **Owner-only.** A Unix socket is `0600`, and its directory is created `0700` when it does
//!   not exist yet; a named pipe carries a protected DACL granting only the user running the
//!   process. Who may call is who may open the endpoint.
//! - **A live endpoint is refused.** Binding where a server already answers fails with
//!   [`io::ErrorKind::AddrInUse`] and leaves the other server alone.
//! - **A stale socket is replaced.** A socket file nothing answers on is what a process that
//!   did not exit cleanly leaves; nothing is lost by removing it. A path that is not a socket
//!   is refused, so a mistyped path never deletes a file. Named pipes vanish with their last
//!   handle, so they are never stale.
//! - **Removed on drop.** Dropping the listener removes its socket file, but only if the path
//!   still names the socket this listener made — a successor's socket is not touched.

use std::fmt;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::{PIPE_PREFIX, UNIX_PREFIX, sys};

/// The prefix every named pipe's full name carries.
const PIPE_NAMESPACE: &str = r"\\.\pipe\";

/// A local endpoint's address: what follows `unix:` or `pipe:`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum LocalAddr {
    /// A Unix domain socket at this path.
    Unix(PathBuf),
    /// A Windows named pipe with this full name, `\\.\pipe\…`.
    Pipe(String),
}

impl LocalAddr {
    /// Parses `unix:PATH` or `pipe:NAME`; `None` for anything else, including an empty path.
    ///
    /// A pipe name without the `\\.\pipe\` namespace is put in it, so `pipe:norn` and
    /// `pipe:\\.\pipe\norn` name the same pipe. Remote pipe names (`\\host\pipe\…`) are kept
    /// as written and refused when bound, since a server can only create a local pipe.
    pub fn parse(addr: &str) -> Option<Self> {
        if let Some(path) = addr.strip_prefix(UNIX_PREFIX).filter(|p| !p.is_empty()) {
            return Some(Self::Unix(PathBuf::from(path)));
        }
        let name = addr.strip_prefix(PIPE_PREFIX).filter(|n| !n.is_empty())?;
        if name.starts_with(r"\\") {
            Some(Self::Pipe(name.to_owned()))
        } else {
            Some(Self::Pipe(format!("{PIPE_NAMESPACE}{name}")))
        }
    }

    /// Whether `addr` names a local endpoint rather than a TCP address.
    pub fn is_local(addr: &str) -> bool {
        Self::parse(addr).is_some()
    }
}

impl fmt::Display for LocalAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unix(path) => write!(f, "{UNIX_PREFIX}{}", path.display()),
            Self::Pipe(name) => write!(f, "{PIPE_PREFIX}{name}"),
        }
    }
}

/// A bound local endpoint, accepting connections.
///
/// `Send` and `Sync`; accepting takes `&mut self`, so one task accepts at a time, which is
/// what every server loop does anyway. Binding is synchronous: neither a socket nor a pipe
/// instance takes a round trip to create.
pub struct LocalListener {
    inner: sys::Listener,
    addr: LocalAddr,
}

impl LocalListener {
    /// Binds `addr` owner-only, replacing a stale Unix socket there.
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::AddrInUse`] when a server answers there;
    /// [`io::ErrorKind::AlreadyExists`] when a Unix path exists and is not a socket;
    /// [`io::ErrorKind::Unsupported`] for an endpoint this platform lacks; otherwise the
    /// operating system's error. Every message names the address.
    pub fn bind(addr: &LocalAddr) -> io::Result<Self> {
        let inner = sys::bind(addr)
            .map_err(|e| io::Error::new(e.kind(), format!("cannot listen on {addr}: {e}")))?;
        Ok(Self {
            inner,
            addr: addr.clone(),
        })
    }

    /// The address this listener was bound to.
    pub fn local_addr(&self) -> &LocalAddr {
        &self.addr
    }

    /// Waits for the next client.
    ///
    /// Cancel-safe: dropping the future before it resolves loses no client, which is what a
    /// `select!` against a shutdown signal needs.
    ///
    /// # Errors
    ///
    /// The operating system refused the connection or ran out of descriptors. The listener
    /// stays usable; a server loop backs off and accepts again.
    pub async fn accept(&mut self) -> io::Result<LocalStream> {
        self.inner.accept().await.map(LocalStream)
    }
}

impl fmt::Debug for LocalListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalListener")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

/// How long to wait before accepting again after `accept` fails: usually the process is out
/// of descriptors, and retrying at once would spin a core against a limit only closes clear.
const ACCEPT_BACKOFF: std::time::Duration = std::time::Duration::from_millis(100);

impl axum::serve::Listener for LocalListener {
    type Io = LocalStream;
    type Addr = LocalAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            match LocalListener::accept(self).await {
                Ok(stream) => return (stream, self.addr.clone()),
                Err(err) => {
                    tracing::warn!(addr = %self.addr, error = %err, "accept failed; backing off");
                    tokio::time::sleep(ACCEPT_BACKOFF).await;
                }
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        Ok(self.addr.clone())
    }
}

/// One accepted connection on a [`LocalListener`]: a byte stream, nothing more.
///
/// `Send` and `Unpin`; like any socket it is read by one task and written by one task.
pub struct LocalStream(sys::Stream);

impl fmt::Debug for LocalStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalStream").finish_non_exhaustive()
    }
}

impl AsyncRead for LocalStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for LocalStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.0.is_write_vectored()
    }
}

#[cfg(test)]
mod tests {
    use super::LocalAddr;

    #[test]
    fn an_address_names_a_socket_a_pipe_or_neither() {
        assert_eq!(
            LocalAddr::parse("unix:/run/a.sock"),
            Some(LocalAddr::Unix("/run/a.sock".into()))
        );
        assert_eq!(
            LocalAddr::parse(r"pipe:\\.\pipe\a"),
            Some(LocalAddr::Pipe(r"\\.\pipe\a".into()))
        );
        assert_eq!(
            LocalAddr::parse("pipe:a"),
            Some(LocalAddr::Pipe(r"\\.\pipe\a".into()))
        );
        for tcp in ["unix:", "pipe:", "127.0.0.1:80", ":80", "localhost:1"] {
            assert_eq!(LocalAddr::parse(tcp), None, "{tcp}");
        }
    }

    #[test]
    fn an_address_prints_as_it_parses() {
        for addr in ["unix:/run/a.sock", r"pipe:\\.\pipe\a"] {
            assert_eq!(LocalAddr::parse(addr).unwrap().to_string(), addr);
        }
    }
}
