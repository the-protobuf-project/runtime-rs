//! Where the startup banner goes. MCP over stdio owns stdout, so these run a server in a
//! child process and read what really reached its two streams.

use std::process::{Command, Output, Stdio};

use grpc::banner::BannerOutput;
use grpc::options::{Endpoint, Options};
use grpc::{HybridServer, banner};

const CHOICE: &str = "BANNER_OUTPUT_TEST_CHOICE";
/// Only the banner prints this: it is the build line's label.
const MARK: &str = "Environment:";

/// Not a test of its own: the body the child process runs, doing nothing in the parent.
#[tokio::test]
async fn banner_child() {
    let Ok(choice) = std::env::var(CHOICE) else {
        return;
    };
    let server = HybridServer::new(Options {
        service_name: "banner-test".into(),
        grpc: Endpoint::new("127.0.0.1", 0),
        ..Default::default()
    });
    let mut server = match choice.as_str() {
        "off" => server.with_banner(BannerOutput::Off),
        "stderr" => server.with_banner(BannerOutput::Stderr),
        "stdout" => server.with_banner(BannerOutput::Stdout),
        _ => server,
    };
    server.start().await.expect("the server starts");
    server.stop().await.expect("the server stops");
}

fn run(choice: &str) -> Output {
    let output = Command::new(std::env::current_exe().expect("this test binary"))
        .args(["--exact", "banner_child", "--nocapture"])
        .env(CHOICE, choice)
        .stdin(Stdio::null())
        .output()
        .expect("the child runs");
    assert!(output.status.success(), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("1 passed"),
        "the child must have run its body: {output:?}"
    );
    output
}

fn printed(stream: &[u8]) -> bool {
    String::from_utf8_lossy(stream).contains(MARK)
}

#[test]
fn the_default_leaves_stdout_clean_for_a_protocol_and_prints_to_stderr() {
    let output = run("default");
    assert!(!printed(&output.stdout), "stdout carries MCP over stdio");
    assert!(printed(&output.stderr), "the banner is still shown");
}

#[test]
fn stderr_is_stderr_and_stdout_is_stdout() {
    let output = run("stderr");
    assert!(!printed(&output.stdout) && printed(&output.stderr));
    let output = run("stdout");
    assert!(printed(&output.stdout) && !printed(&output.stderr));
}

#[test]
fn off_prints_nothing_anywhere() {
    let output = run("off");
    assert!(!printed(&output.stdout) && !printed(&output.stderr));
}

#[test]
fn the_summary_says_nothing_of_grpc_without_a_listener() {
    let opts = Options {
        service_name: "x".into(),
        ..Default::default()
    };
    let state = banner::Summary {
        no_grpc_listener: true,
        ..Default::default()
    };
    assert!(banner::rows(&opts, &state).is_empty());
    assert!(!banner::rows(&opts, &banner::Summary::default()).is_empty());
}
