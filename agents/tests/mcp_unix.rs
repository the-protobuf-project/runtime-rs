//! MCP on a Unix domain socket, end to end: a real MCP client lists and calls a tool over
//! Streamable HTTP on `unix:PATH`, the legacy SSE stream opens on one, the socket is
//! owner-only and gone after shutdown, and a live socket or a file that is not one is refused.

#![cfg(all(unix, feature = "mcp"))]

mod common;
mod mcp_probe;
#[path = "common/socket.rs"]
mod socket;

use socket::socket;
use std::os::unix::fs::PermissionsExt;

use agents::Runtime;
use agents::mcp::{self, Transport};
use common::{config, free_port};
use mcp_probe::Probe;
use rmcp::ServiceExt;
use rmcp::model::CallToolRequestParams;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::transport::{StreamableHttpClientTransport, UnixSocketHttpClient};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

#[test]
fn socket_directories_are_isolated_and_removed_on_drop() {
    let (first, a) = socket("same");
    let (_second, b) = socket("same");
    assert_ne!(a.parent(), b.parent());
    let directory = a.parent().unwrap().to_owned();
    drop(first);
    assert!(!directory.exists());
    assert!(b.parent().unwrap().exists());
}

async fn serve(path: &std::path::Path, transport: Transport) -> (Runtime, CancellationToken) {
    let service = mcp::Service::from_handler(Probe::new("probe"))
        .transports([transport])
        .addr(format!("unix:{}", path.display()));
    let runtime = Runtime::new(config(free_port().await)).register(service);
    let cancel = CancellationToken::new();
    runtime.start(cancel.clone()).await.expect("start");
    (runtime, cancel)
}

#[tokio::test]
async fn a_client_lists_and_calls_tools_over_streamable_http_on_a_unix_socket() {
    let (_directory, path) = socket("streamable");
    let (runtime, cancel) = serve(&path, Transport::StreamableHttp).await;
    let endpoint = &runtime.endpoints()[0];
    assert_eq!(endpoint.transport, "streamable-http");
    assert!(endpoint.url.starts_with("http+unix://"), "{}", endpoint.url);

    let http = UnixSocketHttpClient::new(path.to_str().expect("utf-8"), "http://localhost/mcp");
    let transport = StreamableHttpClientTransport::with_client(
        http,
        StreamableHttpClientTransportConfig::with_uri("http://localhost/mcp"),
    );
    let client = ().serve(transport).await.expect("initialize over the socket");
    let tools = client.list_all_tools().await.expect("tools/list");
    assert!(tools.iter().any(|t| t.name == "probe"), "{tools:?}");
    let mut call = CallToolRequestParams::new("probe");
    call.arguments = serde_json::json!({"word": "hi"}).as_object().cloned();
    let result = client.call_tool(call).await.expect("tools/call");
    assert!(format!("{result:?}").contains("hi"), "{result:?}");
    client.cancel().await.expect("close");

    let mode = std::fs::metadata(&path)
        .expect("bound")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "owner-only");
    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");
    assert!(!path.exists(), "the socket is removed after shutdown");
}

#[tokio::test]
async fn the_sse_stream_opens_on_a_unix_socket() {
    let (_directory, path) = socket("sse");
    let (runtime, cancel) = serve(&path, Transport::Sse).await;
    let mut stream = tokio::net::UnixStream::connect(&path)
        .await
        .expect("connect");
    stream
        .write_all(b"GET /mcp/ HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n")
        .await
        .expect("request");
    let mut seen = String::new();
    let mut buf = [0u8; 1024];
    while !seen.contains("event: endpoint") {
        let n = tokio::time::timeout(std::time::Duration::from_secs(5), stream.read(&mut buf))
            .await
            .expect("an event within 5 s")
            .expect("read");
        assert!(n > 0, "closed before the endpoint event: {seen}");
        seen.push_str(&String::from_utf8_lossy(&buf[..n]));
    }
    assert!(seen.starts_with("HTTP/1.1 200"), "{seen}");
    drop(stream);
    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_socket_something_answers_on_is_refused_and_left_alone() {
    let (_directory, path) = socket("live");
    let _live = std::os::unix::net::UnixListener::bind(&path).expect("someone else's socket");
    let service =
        mcp::Service::from_handler(Probe::new("probe")).addr(format!("unix:{}", path.display()));
    let runtime = Runtime::new(config(free_port().await)).register(service);
    let error = runtime
        .start(CancellationToken::new())
        .await
        .expect_err("refused");
    assert!(error.to_string().contains("unix:"), "{error}");
    assert!(path.exists(), "the other server's socket is not removed");
}

#[tokio::test]
async fn a_stale_socket_is_replaced_and_a_file_that_is_not_one_is_kept() {
    let (_stale_directory, stale) = socket("stale");
    drop(std::os::unix::net::UnixListener::bind(&stale).expect("a socket nobody serves"));
    let (runtime, cancel) = serve(&stale, Transport::StreamableHttp).await;
    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");

    let (_file_directory, file) = socket("not-a-socket");
    std::fs::write(&file, "keep me").expect("a file");
    let service =
        mcp::Service::from_handler(Probe::new("probe")).addr(format!("unix:{}", file.display()));
    let runtime = Runtime::new(config(free_port().await)).register(service);
    runtime
        .start(CancellationToken::new())
        .await
        .expect_err("refused");
    assert_eq!(std::fs::read_to_string(&file).expect("kept"), "keep me");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_socket_that_cannot_be_connected_to_is_not_unlinked() {
    let (_directory, path) = socket("denied");
    let _live = std::os::unix::net::UnixListener::bind(&path).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();
    let error = std::os::unix::net::UnixStream::connect(&path).expect_err("permission denied");
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    let service =
        mcp::Service::from_handler(Probe::new("probe")).addr(format!("unix:{}", path.display()));
    let runtime = Runtime::new(config(free_port().await)).register(service);
    runtime
        .start(CancellationToken::new())
        .await
        .expect_err("permission denied");
    assert!(
        path.exists(),
        "a connection error must not remove the socket"
    );
}
