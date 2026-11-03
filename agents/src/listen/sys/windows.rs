//! Windows named pipes, created owner-only. Unix sockets are Unix's.
//!
//! A pipe server is a sequence of instances: one waits for a client, and once a client takes
//! it the next is created before the connected one is handed over, so a client arriving in
//! between finds an instance waiting rather than `ERROR_FILE_NOT_FOUND`. Clients that arrive
//! while none waits get `ERROR_PIPE_BUSY` and retry, which is the pipe's listen backlog.
//!
//! The first instance is created with `FILE_FLAG_FIRST_PIPE_INSTANCE`, so a name another
//! server already holds is refused rather than joined: a second daemon must not take half
//! the first one's clients. Remote clients are rejected; a named pipe here is local IPC.

use std::io;

use tokio::net::windows::named_pipe::NamedPipeServer;

use super::LocalAddr;

#[path = "windows_security.rs"]
mod security;

/// A connected client.
pub(crate) type Stream = NamedPipeServer;

/// A named pipe accepting clients.
pub(crate) struct Listener {
    name: String,
    /// The instance waiting for the next client.
    waiting: NamedPipeServer,
    owner: security::OwnerOnly,
}

/// Binds `addr`, which must be a named pipe here.
pub(crate) fn bind(addr: &LocalAddr) -> io::Result<Listener> {
    let LocalAddr::Pipe(name) = addr else {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Unix domain sockets are not served on Windows; use pipe:NAME here",
        ));
    };
    if !name.starts_with(r"\\.\pipe\") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            r"a server can only create a local pipe, \\.\pipe\NAME",
        ));
    }
    let owner = security::OwnerOnly::for_current_user()?;
    let waiting = owner.create(name, true).map_err(|e| {
        // The first-instance flag fails with access denied when the name is already held.
        if e.raw_os_error() == Some(windows_sys::Win32::Foundation::ERROR_ACCESS_DENIED as i32) {
            io::Error::new(io::ErrorKind::AddrInUse, "a server is listening there")
        } else {
            e
        }
    })?;
    Ok(Listener {
        name: name.clone(),
        waiting,
        owner,
    })
}

impl Listener {
    /// The next client.
    ///
    /// Cancel-safe: the waiting instance is only replaced after a client has connected, so a
    /// dropped future leaves it waiting for the next call.
    pub(crate) async fn accept(&mut self) -> io::Result<Stream> {
        if let Err(err) = self.waiting.connect().await {
            // A client that came and went before the connect leaves the instance unusable;
            // a fresh one keeps the next accept from failing the same way forever.
            self.waiting = self.owner.create(&self.name, false)?;
            return Err(err);
        }
        let next = self.owner.create(&self.name, false)?;
        Ok(std::mem::replace(&mut self.waiting, next))
    }
}
