//! gRPC on a Windows named pipe: a unary call and a server stream answer over it, a name
//! another server holds is refused, and a stop releases the name for the next server.

#![cfg(windows)]

mod common;

use std::time::Duration;

use common::pb::GetShelfRequest;
use common::{Shop, ShopServiceClient, shop_server};
use grpc::banner::BannerOutput;
use grpc::options::{Endpoint, LocalAddr, Options};
use grpc::{Error, HybridServer, ListenAddr};
use tokio::net::windows::named_pipe::ClientOptions;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_client::HealthClient;

/// `ERROR_PIPE_BUSY`: every instance is taken; the client waits and retries.
const PIPE_BUSY: i32 = 231;

fn name(test: &str) -> String {
    format!(r"\\.\pipe\tpp-grpc-{test}-{}", std::process::id())
}

fn server(name: &str) -> HybridServer {
    HybridServer::new(Options {
        service_name: "pipe-test".into(),
        grpc: Endpoint::pipe(name),
        enable_health: true,
        ..Default::default()
    })
    .with_banner(BannerOutput::Off)
    .with_service(shop_server(Shop::default()))
}

async fn channel(name: &str) -> tonic::transport::Channel {
    let name = name.to_owned();
    tonic::transport::Endpoint::from_static("http://localhost")
        .connect_with_connector(tower::service_fn(move |_: http::Uri| {
            let name = name.clone();
            async move {
                loop {
                    match ClientOptions::new().open(&name) {
                        Ok(client) => return Ok(hyper_util::rt::TokioIo::new(client)),
                        Err(e) if e.raw_os_error() == Some(PIPE_BUSY) => {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                        Err(e) => return Err::<_, std::io::Error>(e),
                    }
                }
            }
        }))
        .await
        .expect("connects over the pipe")
}

#[tokio::test]
async fn unary_and_streaming_calls_answer_over_a_named_pipe() {
    let name = name("calls");
    let mut server = server(&name);
    server.start().await.expect("the server starts on the pipe");
    assert_eq!(
        server.grpc_listen_addr(),
        Some(ListenAddr::Local(LocalAddr::Pipe(name.clone())))
    );
    let channel = channel(&name).await;
    let shelf = ShopServiceClient::new(channel.clone())
        .get_shelf(GetShelfRequest {
            name: "shelves/1".into(),
        })
        .await
        .expect("a unary call answers");
    assert_eq!(shelf.get_ref().name, "shelves/1");
    let mut watch = HealthClient::new(channel)
        .watch(HealthCheckRequest::default())
        .await
        .expect("a server stream opens")
        .into_inner();
    watch
        .message()
        .await
        .expect("a message")
        .expect("not ended");
    drop(watch);
    server.stop().await.expect("stops");
}

#[tokio::test]
async fn a_pipe_another_server_holds_is_refused_and_released_after_a_stop() {
    let name = name("held");
    let mut first = server(&name);
    first.start().await.expect("the first server starts");
    let mut second = server(&name);
    match second.start().await {
        Err(Error::Listen { source, .. }) => {
            assert_eq!(source.kind(), std::io::ErrorKind::AddrInUse, "{source}");
        }
        other => panic!("expected a listen error, got {other:?}"),
    }
    first.stop().await.expect("the first stops");
    second
        .start()
        .await
        .expect("the name is free once the first stops");
    second.stop().await.expect("stops");
}
