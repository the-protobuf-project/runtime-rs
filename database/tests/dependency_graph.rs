//! What a build of this crate links, read from Cargo's own resolution.
use std::process::Command;

/// The packages a build links, one per line, with `args` added to `cargo tree`.
fn packages(args: &[&str]) -> String {
    let manifest = concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml");
    let output = Command::new(std::env::var("CARGO").expect("cargo test sets CARGO"))
        .args(["tree", "--manifest-path", manifest, "--edges", "normal"])
        .args(["--prefix", "none", "--format", "{p}"])
        .args(args)
        .output()
        .expect("cargo tree runs");
    assert!(output.status.success(), "{output:?}");
    String::from_utf8(output.stdout).expect("utf-8")
}

#[test]
fn nothing_selects_aws_lc_so_a_bazel_build_has_nothing_to_wire() {
    let tree = packages(&[]);
    assert!(tree.contains("rustls v"), "the tree was read: {tree}");
    assert!(tree.contains("ring v"), "ring is the provider: {tree}");
    assert!(!tree.contains("aws-lc"), "aws-lc is in the graph:\n{tree}");
}

#[test]
fn the_advisories_under_parquet_are_not_reachable() {
    let tree = packages(&[]);
    assert!(
        tree.contains("arrow-array v60"),
        "the tree was read: {tree}"
    );
    for absent in ["parquet v", "zstd v"] {
        assert!(!tree.contains(absent), "{absent} is in the graph:\n{tree}");
    }
}

#[test]
fn a_capture_only_build_has_one_arrow_and_no_http_client() {
    let tree = packages(&["--no-default-features"]);
    assert!(
        tree.contains("arrow-array v60"),
        "the tree was read: {tree}"
    );
    for absent in [
        "arrow-array v59",
        "influxdb3-client",
        "reqwest v",
        "rustls v",
    ] {
        assert!(!tree.contains(absent), "{absent} is in the graph:\n{tree}");
    }
}

#[test]
fn the_sql_backend_speaks_rustls_on_ring_and_links_no_native_tls() {
    let tree = packages(&["--features", "orm"]);
    assert!(
        tree.contains("sqlx-postgres v"),
        "the tree was read: {tree}"
    );
    assert!(tree.contains("ring v"), "ring is the provider: {tree}");
    for absent in ["aws-lc", "native-tls", "openssl"] {
        assert!(!tree.contains(absent), "{absent} is in the graph:\n{tree}");
    }
}

#[test]
fn one_dialect_links_one_sql_client() {
    let tree = packages(&["--no-default-features", "--features", "orm-postgres"]);
    assert!(
        tree.contains("sqlx-postgres v"),
        "the tree was read: {tree}"
    );
    for absent in ["sqlx-mysql", "sqlx-sqlite", "libsqlite3-sys"] {
        assert!(!tree.contains(absent), "{absent} is in the graph:\n{tree}");
    }
}

#[test]
fn the_mongodb_driver_selects_ring_as_well() {
    let tree = packages(&["--features", "mongodb"]);
    assert!(tree.contains("mongodb v3"), "the tree was read: {tree}");
    assert!(tree.contains("ring v"), "ring is the provider: {tree}");
    assert!(!tree.contains("aws-lc"), "aws-lc is in the graph:\n{tree}");
}
