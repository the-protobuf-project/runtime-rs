//! What a build of this crate links, read from Cargo's own resolution.
//!
//! The graph is read for every target at once (`--target all`), so a C library pulled in only
//! on Linux or only on Windows is caught on whichever host runs the test, and with normal
//! edges only, since a dev-dependency never reaches a caller's binary.

use std::process::Command;

/// The crates that compile C into a build: the two TLS crypto providers rustls can use.
const C_CRYPTO: [&str; 3] = ["ring v", "aws-lc-rs v", "aws-lc-sys v"];

/// The packages a build of `package` with only `features` links, one per line.
///
/// Fails rather than returning an empty graph: a tree too small to hold axum was not read,
/// and every absence asserted against it would pass for no reason.
fn packages(package: &str, features: &[&str]) -> String {
    let manifest = concat!(env!("CARGO_MANIFEST_DIR"), "/../Cargo.toml");
    let cargo = std::env::var("CARGO").expect("cargo test sets CARGO");
    let mut command = Command::new(cargo);
    command.args(["tree", "--manifest-path", manifest, "-p", package]);
    command.args(["--edges", "normal", "--target", "all"]);
    command.args(["--prefix", "none", "--format", "{p}"]);
    command.arg("--no-default-features");
    if !features.is_empty() {
        command.args(["--features", &features.join(",")]);
    }
    let output = command.output().expect("cargo tree runs");
    assert!(output.status.success(), "{output:?}");
    let tree = String::from_utf8(output.stdout).expect("utf-8");
    assert!(
        tree.lines().count() > 20 && tree.contains("axum v"),
        "the graph of {package} was not read:\n{tree}"
    );
    tree
}

/// The C crypto crates `tree` names.
fn c_crypto(tree: &str) -> Vec<&'static str> {
    C_CRYPTO.into_iter().filter(|c| tree.contains(c)).collect()
}

#[test]
fn without_a2a_there_is_no_a2a_sdk_reqwest_or_aws_lc() {
    let tree = packages("tpp-grpc", &["tls"]);
    assert!(tree.contains("tonic v"), "the tree was read: {tree}");
    assert!(tree.contains("rmcp v"), "MCP stays: {tree}");
    for absent in ["a2a-", "reqwest v", "aws-lc"] {
        assert!(!tree.contains(absent), "{absent} is in the graph:\n{tree}");
    }
}

#[test]
fn the_a2a_feature_brings_the_sdk_back() {
    assert!(packages("tpp-grpc", &["a2a"]).contains("a2a-lf v"));
}

#[test]
fn without_tls_nothing_links_ring_or_aws_lc() {
    let tree = packages("tpp-grpc", &[]);
    assert!(tree.contains("tonic v"), "the tree was read: {tree}");
    assert!(tree.contains("rmcp v"), "MCP stays: {tree}");
    for absent in ["rustls v", "tokio-rustls v", "quinn v"] {
        assert!(!tree.contains(absent), "{absent} is in the graph:\n{tree}");
    }
    assert!(c_crypto(&tree).is_empty(), "C crypto is linked:\n{tree}");
}

#[test]
fn the_mcp_path_through_the_agents_crate_links_no_c_crypto() {
    let tree = packages("tpp-agents", &["mcp"]);
    assert!(tree.contains("rmcp v"), "the tree was read: {tree}");
    assert!(c_crypto(&tree).is_empty(), "C crypto is linked:\n{tree}");
}

/// The checks above can fail: the `tls` build does link ring, and `c_crypto` finds it.
#[test]
fn the_tls_feature_brings_ring_and_the_check_sees_it() {
    let tree = packages("tpp-grpc", &["tls"]);
    assert_eq!(c_crypto(&tree), ["ring v"], "\n{tree}");
}
