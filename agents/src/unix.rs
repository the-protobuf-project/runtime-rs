//! A Unix domain socket a service listens on, bound owner-only (`0600`).
//!
//! The mode is set just after `bind`, and in that instant the process's umask decides; a
//! deployment that needs no window at all puts the socket in a directory only its user can
//! enter, which is the stronger control anyway.
//!
//! A socket left by a process that did not exit cleanly is replaced: nothing answers on it, so
//! nothing is lost. A socket something still answers on, or a path that is not a socket, is
//! refused rather than removed, so a mistyped path never deletes a file. When serving stops the
//! socket is removed, but only if the path still names the socket this process made.

use std::future::Future;
use std::io;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use axum::Router;

/// A bound socket, not yet serving. Removes its file when dropped, served or not, so a
/// listener bound and then discarded leaves nothing behind.
pub(crate) struct Socket {
    listener: tokio::net::UnixListener,
    file: File,
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

impl Socket {
    /// Binds `path` owner-only, replacing a stale socket there.
    ///
    /// # Errors
    ///
    /// A server answers on `path`, `path` exists and is not a socket, or the bind or the mode
    /// change failed.
    pub(crate) fn bind(path: &Path) -> io::Result<Self> {
        let failed = |kind, why: &dyn std::fmt::Display| {
            io::Error::new(
                kind,
                format!("cannot listen on unix:{}: {why}", path.display()),
            )
        };
        match std::fs::symlink_metadata(path) {
            Ok(meta) if meta.file_type().is_socket() => {
                match std::os::unix::net::UnixStream::connect(path) {
                    Ok(_) => {
                        return Err(failed(
                            io::ErrorKind::AddrInUse,
                            &"a server is listening there",
                        ));
                    }
                    Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => {}
                    Err(e) => return Err(failed(e.kind(), &e)),
                }
                std::fs::remove_file(path).map_err(|e| failed(e.kind(), &e))?;
            }
            Ok(_) => {
                return Err(failed(
                    io::ErrorKind::AlreadyExists,
                    &"it exists and is not a socket",
                ));
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(failed(e.kind(), &e)),
        }
        let listener = tokio::net::UnixListener::bind(path).map_err(|e| failed(e.kind(), &e))?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .map_err(|e| failed(e.kind(), &e))?;
        let meta = std::fs::symlink_metadata(path).map_err(|e| failed(e.kind(), &e))?;
        Ok(Self {
            listener,
            file: File {
                path: path.to_owned(),
                id: (meta.dev(), meta.ino()),
            },
        })
    }

    /// Serves `router` until `shutdown`, then removes the socket.
    pub(crate) async fn serve(
        self,
        router: Router,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> io::Result<()> {
        let Self { listener, file } = self;
        let served = axum::serve(listener, router)
            .with_graceful_shutdown(shutdown)
            .await;
        drop(file);
        served
    }
}
