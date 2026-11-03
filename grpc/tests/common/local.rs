//! Helpers for gRPC over a Unix socket: isolated socket directories short enough for
//! `sun_path`, a tonic channel dialled over a socket, and a count of a process's TCP
//! listeners that fails rather than guesses when it cannot look.

#![allow(dead_code)]

use std::path::{Path, PathBuf};

/// A fresh directory under `/tmp`, removed on drop. `/tmp` rather than the system temp
/// directory because macOS's is too long for a socket path.
pub struct SocketDir(pub PathBuf);

impl Drop for SocketDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A directory of our own and the path of `name.sock` inside it.
pub fn socket(name: &str) -> (SocketDir, PathBuf) {
    // Tests run in parallel threads, so the clock alone can repeat; the counter cannot.
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let unique = format!(
        "tpp-grpc-{}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("after the epoch")
            .as_nanos()
    );
    let directory = PathBuf::from("/tmp").join(unique);
    std::fs::create_dir(&directory).expect("an isolated socket directory");
    let path = directory.join(format!("{name}.sock"));
    (SocketDir(directory), path)
}

/// A tonic channel to the gRPC server listening on the Unix socket at `path`.
pub async fn channel(path: &Path) -> Result<tonic::transport::Channel, tonic::transport::Error> {
    let path = path.to_owned();
    tonic::transport::Endpoint::from_static("http://localhost")
        .connect_with_connector(tower::service_fn(move |_: http::Uri| {
            let path = path.clone();
            async move {
                let stream = tokio::net::UnixStream::connect(path).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        }))
        .await
}

/// How many TCP sockets process `pid` has listening.
///
/// Read from `/proc` on Linux and from `lsof` elsewhere. Panics when neither can be read, so
/// a test asserting "none" cannot pass because it saw nothing.
pub fn tcp_listeners(pid: u32) -> usize {
    if cfg!(target_os = "linux") {
        linux_listeners(pid)
    } else {
        lsof_listeners(pid)
    }
}

/// Proves [`tcp_listeners`] sees a listener on this platform: one appears while bound and
/// is gone after.
pub fn assert_tcp_listeners_are_visible() {
    let before = tcp_listeners(std::process::id());
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("a probe listener");
    assert_eq!(
        tcp_listeners(std::process::id()),
        before + 1,
        "the listener count must see a TCP listener, or a count of zero proves nothing"
    );
    drop(probe);
    assert_eq!(tcp_listeners(std::process::id()), before);
}

fn lsof_listeners(pid: u32) -> usize {
    let output = std::process::Command::new("lsof")
        .args([
            "-nP",
            "-a",
            "-p",
            &pid.to_string(),
            "-iTCP",
            "-sTCP:LISTEN",
            "-Fn",
        ])
        .output()
        .expect("lsof is needed to count TCP listeners on this platform");
    // lsof exits 1 when it matches nothing; anything else with no output is a failure.
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() || (output.status.code() == Some(1) && stdout.is_empty()),
        "lsof failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    stdout.lines().filter(|line| line.starts_with('n')).count()
}

fn linux_listeners(pid: u32) -> usize {
    let fds = std::fs::read_dir(format!("/proc/{pid}/fd")).expect("/proc/PID/fd is readable");
    let sockets: std::collections::HashSet<String> = fds
        .filter_map(|entry| std::fs::read_link(entry.ok()?.path()).ok())
        .filter_map(|target| {
            let target = target.to_string_lossy().into_owned();
            Some(
                target
                    .strip_prefix("socket:[")?
                    .strip_suffix(']')?
                    .to_owned(),
            )
        })
        .collect();
    let tables: Vec<String> = ["tcp", "tcp6"]
        .iter()
        .filter_map(|table| std::fs::read_to_string(format!("/proc/{pid}/net/{table}")).ok())
        .collect();
    assert!(!tables.is_empty(), "no /proc/PID/net/tcp table is readable");
    tables
        .iter()
        .flat_map(|text| {
            text.lines()
                .skip(1)
                .filter_map(|line| {
                    let fields: Vec<&str> = line.split_whitespace().collect();
                    // `st` 0A is LISTEN; the inode is the tenth field.
                    (fields.get(3) == Some(&"0A")).then(|| fields.get(9).map(|s| s.to_string()))?
                })
                .collect::<Vec<_>>()
        })
        .filter(|inode| sockets.contains(inode))
        .count()
}
