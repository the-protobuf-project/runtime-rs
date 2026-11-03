//! One accepted gRPC connection: its byte stream, its slot under the connection limit, its
//! idle timer, and the signal that cuts it when a drain runs out of time.
//!
//! The limits live in the stream rather than in tonic because tonic spawns each connection
//! as a task of its own and keeps no handle to it: dropping the server's future leaves every
//! connection running. A stream that fails its next read once told to is the one lever that
//! reaches them all.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use agents::listen::LocalStream;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::OwnedSemaphorePermit;
use tokio::time::{Instant, Sleep};
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use tonic::transport::server::{Connected, TcpConnectInfo};

/// The transport underneath.
pub(crate) enum Io {
    Tcp(TcpStream),
    Local(LocalStream),
}

/// An accepted connection under the listener's limits.
pub(crate) struct Conn {
    io: Io,
    /// What tonic puts on every request: the TCP addresses, or none for a local endpoint, so
    /// `Request::remote_addr` keeps working for TCP clients as it did before.
    info: TcpConnectInfo,
    /// Released when the connection closes, admitting the next client.
    _slot: Option<OwnedSemaphorePermit>,
    idle: Option<(Duration, Pin<Box<Sleep>>)>,
    abort: CancellationToken,
    aborted: Pin<Box<WaitForCancellationFutureOwned>>,
}

impl Conn {
    pub(crate) fn new(
        io: Io,
        slot: Option<OwnedSemaphorePermit>,
        idle: Option<Duration>,
        abort: CancellationToken,
    ) -> Self {
        let info = match &io {
            Io::Tcp(stream) => stream.connect_info(),
            Io::Local(_) => TcpConnectInfo {
                local_addr: None,
                remote_addr: None,
            },
        };
        Self {
            io,
            info,
            _slot: slot,
            idle: idle.map(|d| (d, Box::pin(tokio::time::sleep(d)))),
            aborted: Box::pin(abort.clone().cancelled_owned()),
            abort,
        }
    }

    /// Fails once the drain has run out, registering for the signal otherwise.
    fn check_abort(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self.abort.is_cancelled() || self.aborted.as_mut().poll(cx).is_ready() {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "gRPC drain timed out; connection cut",
            ));
        }
        Ok(())
    }

    /// Pushes the idle deadline out after traffic.
    fn touch(&mut self) {
        if let Some((timeout, sleep)) = &mut self.idle {
            sleep.as_mut().reset(Instant::now() + *timeout);
        }
    }

    /// Whether the connection has been silent past its idle timeout.
    fn idle_expired(&mut self, cx: &mut Context<'_>) -> bool {
        match &mut self.idle {
            Some((_, sleep)) => sleep.as_mut().poll(cx).is_ready(),
            None => false,
        }
    }

    /// Settles a transport poll: traffic resets the idle timer, and silence past it is an
    /// error, which hyper treats as the connection closing.
    fn settle<T>(
        &mut self,
        cx: &mut Context<'_>,
        polled: Poll<io::Result<T>>,
    ) -> Poll<io::Result<T>> {
        match polled {
            Poll::Ready(result) => {
                self.touch();
                Poll::Ready(result)
            }
            Poll::Pending if self.idle_expired(cx) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "gRPC connection idle timeout",
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Connected for Conn {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.info.clone()
    }
}

impl AsyncRead for Conn {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.check_abort(cx)?;
        let polled = match &mut this.io {
            Io::Tcp(io) => Pin::new(io).poll_read(cx, buf),
            Io::Local(io) => Pin::new(io).poll_read(cx, buf),
        };
        this.settle(cx, polled)
    }
}

impl AsyncWrite for Conn {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.check_abort(cx)?;
        let polled = match &mut this.io {
            Io::Tcp(io) => Pin::new(io).poll_write(cx, buf),
            Io::Local(io) => Pin::new(io).poll_write(cx, buf),
        };
        this.settle(cx, polled)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.check_abort(cx)?;
        let polled = match &mut this.io {
            Io::Tcp(io) => Pin::new(io).poll_write_vectored(cx, bufs),
            Io::Local(io) => Pin::new(io).poll_write_vectored(cx, bufs),
        };
        this.settle(cx, polled)
    }

    fn is_write_vectored(&self) -> bool {
        match &self.io {
            Io::Tcp(io) => io.is_write_vectored(),
            Io::Local(io) => io.is_write_vectored(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.check_abort(cx)?;
        match &mut this.io {
            Io::Tcp(io) => Pin::new(io).poll_flush(cx),
            Io::Local(io) => Pin::new(io).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().io {
            Io::Tcp(io) => Pin::new(io).poll_shutdown(cx),
            Io::Local(io) => Pin::new(io).poll_shutdown(cx),
        }
    }
}
