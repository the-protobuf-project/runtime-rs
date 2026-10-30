//! Bakes the VCS revision into the binary, which is what the startup banner reports.
//!
//! Go reads this from `debug.ReadBuildInfo()`; Rust has no equivalent, so it is captured here
//! and passed through as an environment variable.

fn main() {
    println!("cargo:rerun-if-changed=build.rs");

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
