//! Bakes the VCS revision into the binary, which is what the startup banner reports.
//!
//! Go reads this from `debug.ReadBuildInfo()`; Rust has no equivalent, so it is captured here
//! and passed through as an environment variable.

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    watch_git_head();

    let revision = std::process::Command::new("git")
        .args(["rev-parse", "--short=7", "HEAD"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .map(|rev| rev.trim().to_string())
        .filter(|rev| !rev.is_empty())
        // A build outside a checkout — a vendored crate, a release tarball — has no revision
        // to report, and saying "dev" is honest where a blank would look like a bug.
        .unwrap_or_else(|| "dev".to_string());

    println!("cargo:rustc-env=TPP_GRPC_BUILD_REV={revision}");
}

/// Reruns this script when a checkout or commit moves `HEAD`, so the banner's revision is the
/// one actually built rather than whichever was current when this script last ran.
///
/// It watches `HEAD` itself, the branch file `HEAD` points at, and `packed-refs`, where git
/// moves that branch file on `gc`. `--git-path` resolves each one inside a linked worktree
/// too. A path is only watched when it exists: Cargo treats a missing one as always changed,
/// which would rebuild every time.
fn watch_git_head() {
    let git = |args: &[&str]| {
        std::process::Command::new("git")
            .args(args)
            .output()
            .ok()
            .filter(|out| out.status.success())
            .and_then(|out| String::from_utf8(out.stdout).ok())
            .map(|out| out.trim().to_string())
            .filter(|out| !out.is_empty())
    };

    let mut watched = vec!["HEAD".to_string(), "packed-refs".to_string()];
    if let Some(branch) = git(&["symbolic-ref", "-q", "HEAD"]) {
        watched.push(branch);
    }
    for name in watched {
        if let Some(path) = git(&["rev-parse", "--git-path", &name])
            && std::path::Path::new(&path).exists()
        {
            println!("cargo:rerun-if-changed={path}");
        }
    }
}
