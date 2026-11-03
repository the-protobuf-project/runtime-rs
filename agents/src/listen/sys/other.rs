//! A platform with neither Unix sockets nor named pipes: every local address is refused.

use std::io;

use super::LocalAddr;

/// A connected client. Never constructed here.
pub(crate) type Stream = tokio::io::DuplexStream;

/// A bound endpoint. Never constructed here.
pub(crate) struct Listener(std::convert::Infallible);

/// Refuses `addr`: this platform has no local endpoint to bind.
pub(crate) fn bind(addr: &LocalAddr) -> io::Result<Listener> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        format!("{addr} needs Unix domain sockets or named pipes, which this platform lacks"),
    ))
}

impl Listener {
    /// Unreachable: no listener exists to accept on.
    pub(crate) async fn accept(&mut self) -> io::Result<Stream> {
        match self.0 {}
    }
}
