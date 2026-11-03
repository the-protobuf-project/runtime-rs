//! Unix domain sockets, bound owner-only (`0600`). Named pipes are Windows's.
//!
//! The mode is set just after `bind`, and in that instant the process's umask decides; a
//! deployment that needs no window at all puts the socket in a directory only its user can
//! enter, which is the stronger control anyway. A missing directory is created that way
//! (`0700`); an existing one is used as it is, because its mode is its owner's decision.
//!
//! A socket left by a process that did not exit cleanly is replaced: nothing answers on it, so
//! nothing is lost. A socket something still answers on, or a path that is not a socket, is
//! refused rather than removed, so a mistyped path never deletes a file. When the listener is
//! dropped the socket is removed, but only if the path still names the socket it made.

use std::io;
use std::os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use super::LocalAddr;

/// A connected client.
pub(crate) type Stream = tokio::net::UnixStream;

/// A bound socket. Removes its file when dropped, served or not, so a listener bound and then
/// discarded leaves nothing behind.
pub(crate) struct Listener {
    listener: tokio::net::UnixListener,
    _file: File,
}

/// Binds `addr`, which must be a Unix socket here.
pub(crate) fn bind(addr: &LocalAddr) -> io::Result<Listener> {
    match addr {
        LocalAddr::Unix(path) => bind_unix(path),
        LocalAddr::Pipe(_) => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "named pipes are a Windows endpoint; use unix:PATH here",
        )),
    }
}

impl Listener {
    /// The next client. Cancel-safe, as tokio's `accept` is.
    pub(crate) async fn accept(&mut self) -> io::Result<Stream> {
        self.listener.accept().await.map(|(stream, _)| stream)
    }
}

/// The socket's file, removed on drop if the path still names it.
struct File {
    path: PathBuf,
    /// The file's device and inode, so a successor's socket at the same path is not removed.
    id: (u64, u64),
}

impl Drop for File {
    fn drop(&mut self) {
        let ours =
            std::fs::symlink_metadata(&self.path).is_ok_and(|m| (m.dev(), m.ino()) == self.id);
        if ours {
            // Nobody to tell: the listener is stopping, and a socket left behind is replaced
            // by the next process to bind here.
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

fn bind_unix(path: &Path) -> io::Result<Listener> {
    prepare_directory(path)?;
    clear_stale(path)?;
    let listener = tokio::net::UnixListener::bind(path)?;
    // From here the socket is ours: a failure below must not leave it behind.
    let meta = std::fs::symlink_metadata(path)?;
    let file = File {
        path: path.to_owned(),
        id: (meta.dev(), meta.ino()),
    };
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(Listener {
        listener,
        _file: file,
    })
}

/// Creates the socket's directory owner-only when it does not exist yet.
fn prepare_directory(path: &Path) -> io::Result<()> {
    match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() && !dir.exists() => std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir),
        _ => Ok(()),
    }
}

/// Removes a socket nothing answers on; refuses a live one and anything that is not a socket.
fn clear_stale(path: &Path) -> io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_socket() => {
            match std::os::unix::net::UnixStream::connect(path) {
                Ok(_) => Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    "a server is listening there",
                )),
                Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => {
                    std::fs::remove_file(path)
                }
                Err(e) => Err(e),
            }
        }
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "it exists and is not a socket",
        )),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}
