//! Isolated, automatically removed Unix socket directories, short enough for `sun_path`.

use std::path::PathBuf;

pub struct SocketDir(PathBuf);

impl Drop for SocketDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn socket(name: &str) -> (SocketDir, PathBuf) {
    let directory = PathBuf::from("/tmp").join(format!("agents-{}", uuid::Uuid::new_v4().simple()));
    std::fs::create_dir(&directory).expect("isolated socket directory");
    let path = directory.join(format!("{name}.sock"));
    (SocketDir(directory), path)
}
