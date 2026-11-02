//! What a build of this crate links, read from Cargo's own resolution.

use std::process::Command;

/// The packages a build with `features` links, one per line.
fn packages(features: &[&str]) -> String {
    let manifest = concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml");
    let cargo = std::env::var("CARGO").expect("cargo test sets CARGO");
    let mut command = Command::new(cargo);
    command.args(["tree", "--manifest-path", manifest, "--edges", "normal"]);
    command.args([
        "--prefix",
        "none",
        "--format",
        "{p}",
        "--no-default-features",
    ]);
    if !features.is_empty() {
        command.args(["--features", &features.join(",")]);
    }
    let output = command.output().expect("cargo tree runs");
    assert!(output.status.success(), "{output:?}");
    String::from_utf8(output.stdout).expect("utf-8")
}

#[test]
fn without_a2a_there_is_no_a2a_sdk_reqwest_or_aws_lc() {
    let tree = packages(&[]);
    assert!(tree.contains("tonic v"), "the tree was read: {tree}");
    assert!(tree.contains("rmcp v"), "MCP stays: {tree}");
    for absent in ["a2a-", "reqwest v", "aws-lc"] {
        assert!(!tree.contains(absent), "{absent} is in the graph:\n{tree}");
    }
}

#[test]
fn the_a2a_feature_brings_the_sdk_back() {
    assert!(packages(&["a2a"]).contains("a2a-lf v"));
}
