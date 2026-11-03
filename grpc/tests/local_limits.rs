//! The gRPC listener's limits: a connection past the limit is refused or queued, a silent
//! connection is closed after its idle timeout, a connection runs no more calls at once than
//! its stream limit, and a TCP client is still told its own address.

#![cfg(unix)]

mod common;
#[path = "common/local.rs"]
mod local;

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::pb::GetShelfRequest;
use common::{SHOP_DESCRIPTOR_SET, Shop, ShopServiceClient, create, shop_server};
use grpc::HybridServer;
use grpc::banner::BannerOutput;
use grpc::options::{Endpoint, GrpcLimits, Options, Overflow};
use local::{channel, socket};
use tokio::io::AsyncReadExt;

async fn started(endpoint: Endpoint, shop: Shop, limits: GrpcLimits) -> HybridServer {
    let mut server = HybridServer::new(Options {
        service_name: "limits-test".into(),
        grpc: endpoint,
        ..Default::default()
    })
    .with_banner(BannerOutput::Off)
    .with_reflection(SHOP_DESCRIPTOR_SET)
    .with_service(shop_server(shop))
    .with_grpc_limits(limits);
    server.start().await.expect("the server starts");
    server
}

fn shelf() -> GetShelfRequest {
    GetShelfRequest {
        name: "shelves/1".into(),
    }
}

/// A client whose connection is open and has answered once.
async fn connected(path: &Path) -> ShopServiceClient<tonic::transport::Channel> {
    let mut client = ShopServiceClient::new(channel(path).await.expect("connects"));
    client.get_shelf(shelf()).await.expect("answers");
    client
}

#[tokio::test]
async fn a_connection_past_the_limit_is_refused() {
    let (_dir, path) = socket("refuse");
    let limits = GrpcLimits::default()
        .max_connections(1)
        .overflow(Overflow::Refuse);
    let server = started(Endpoint::unix(&path), Shop::default(), limits).await;
    let first = connected(&path).await;

    let second = tokio::time::timeout(Duration::from_secs(5), async {
        let mut client = ShopServiceClient::new(channel(&path).await?);
        client.get_shelf(shelf()).await.map_err(Into::into)
    })
    .await
    .expect("the refusal is prompt");
    let refused: Result<_, Box<dyn std::error::Error>> = second;
    assert!(
        refused.is_err(),
        "a second connection is closed at the limit"
    );

    drop(first);
    // The slot is released when the first connection closes.
    let mut admitted = false;
    for _ in 0..50 {
        if let Ok(channel) = channel(&path).await
            && ShopServiceClient::new(channel)
                .get_shelf(shelf())
                .await
                .is_ok()
        {
            admitted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(admitted, "a client is admitted once the first leaves");
    server.stop().await.expect("stops");
}

#[tokio::test]
async fn a_connection_past_the_limit_waits_for_a_slot() {
    let (_dir, path) = socket("queue");
    let limits = GrpcLimits::default().max_connections(1);
    let server = started(Endpoint::unix(&path), Shop::default(), limits).await;
    let first = connected(&path).await;

    let waiting_path = path.clone();
    let mut second = tokio::spawn(async move {
        let mut client = ShopServiceClient::new(channel(&waiting_path).await?);
        client
            .get_shelf(shelf())
            .await
            .map_err(Box::<dyn std::error::Error + Send + Sync>::from)
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(300), &mut second)
            .await
            .is_err(),
        "the second client waits while the first holds the only slot"
    );
    drop(first);
    tokio::time::timeout(Duration::from_secs(5), second)
        .await
        .expect("served once the slot frees")
        .expect("the task")
        .expect("the queued call answers");
    server.stop().await.expect("stops");
}

#[tokio::test]
async fn a_silent_connection_is_closed_after_its_idle_timeout() {
    let (_dir, path) = socket("idle");
    let limits = GrpcLimits::default().idle_timeout(Duration::from_millis(200));
    let server = started(Endpoint::unix(&path), Shop::default(), limits).await;

    let mut silent = tokio::net::UnixStream::connect(&path)
        .await
        .expect("connects");
    let mut buf = [0u8; 64];
    let closed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match silent.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                // The server's HTTP/2 preface and settings; then it goes quiet.
                Ok(_) => {}
            }
        }
    })
    .await;
    assert!(closed.is_ok(), "the idle connection was closed");
    server.stop().await.expect("stops");
}

#[tokio::test]
async fn a_connection_runs_no_more_calls_at_once_than_its_stream_limit() {
    let (_dir, path) = socket("streams");
    let shop = Shop::default();
    let limits = GrpcLimits::default()
        .max_concurrent_streams(1)
        .initial_stream_window_size(1 << 16)
        .initial_connection_window_size(1 << 20);
    let server = started(Endpoint::unix(&path), shop.clone(), limits).await;
    let client = connected(&path).await;

    let mut waiting = client.clone();
    let first =
        tokio::spawn(async move { waiting.create_book(create("shelves/1", "wait", "")).await });
    shop.entered().await;
    let mut queued = client.clone();
    let mut second = tokio::spawn(async move { queued.get_shelf(shelf()).await });
    assert!(
        tokio::time::timeout(Duration::from_millis(300), &mut second)
            .await
            .is_err(),
        "the second call waits for the connection's only stream"
    );
    shop.release();
    first.await.expect("task").expect("the first call answers");
    tokio::time::timeout(Duration::from_secs(5), second)
        .await
        .expect("the second runs once the stream frees")
        .expect("task")
        .expect("the second call answers");
    server.stop().await.expect("stops");
}

#[tokio::test]
async fn a_tcp_client_is_still_told_its_address_under_limits() {
    let seen = Arc::new(Mutex::new(None));
    let record = Arc::clone(&seen);
    let mut server = HybridServer::new(Options {
        service_name: "limits-test".into(),
        grpc: Endpoint::new("127.0.0.1", 0),
        ..Default::default()
    })
    .with_banner(BannerOutput::Off)
    .with_service(shop_server(Shop::default()))
    .with_grpc_limits(GrpcLimits::default().max_connections(4))
    .with_unary_interceptor(Arc::new(move |request: tonic::Request<()>| {
        *record.lock().expect("unpoisoned") = request.remote_addr();
        Ok(request)
    }));
    server.start().await.expect("starts");
    let port = server.grpc_addr().expect("a TCP address").port();
    common::client(port)
        .await
        .get_shelf(shelf())
        .await
        .expect("answers");
    let remote = seen.lock().expect("unpoisoned").expect("a remote address");
    assert!(remote.ip().is_loopback(), "{remote}");
    server.stop().await.expect("stops");
}
