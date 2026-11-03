//! gRPC on a Unix domain socket: unary and streaming calls, reflection, health, the
//! interceptor chain and caller layers all answer over it; a stale socket is replaced, a live
//! one or a regular file refused; the socket is `0600` in a `0700` directory and gone after a
//! stop; a stop drains in-flight calls, and cuts them once its bound runs out.

#![cfg(unix)]

mod common;
#[path = "common/local.rs"]
mod local;

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use common::pb::GetShelfRequest;
use common::{SHOP_DESCRIPTOR_SET, Shop, ShopServiceClient, create, shop_server};
use grpc::banner::{BannerOutput, Summary};
use grpc::options::{Endpoint, GrpcLimits, LocalAddr, Options};
use grpc::{Error, GrpcService, HybridServer, ListenAddr};
use local::{channel, socket};
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::{HealthCheckRequest, health_check_response::ServingStatus};

fn server(path: &Path, shop: Shop) -> HybridServer {
    HybridServer::new(Options {
        service_name: "local-test".into(),
        grpc: Endpoint::unix(path),
        enable_health: true,
        ..Default::default()
    })
    .with_banner(BannerOutput::Off)
    .with_reflection(SHOP_DESCRIPTOR_SET)
    .with_service(shop_server(shop))
}

async fn started(path: &Path, shop: Shop) -> HybridServer {
    let mut server = server(path, shop);
    server
        .start()
        .await
        .expect("the server starts on the socket");
    server
}

#[tokio::test]
async fn unary_and_streaming_calls_reach_the_services_over_a_unix_socket() {
    let (_dir, path) = socket("calls");
    let server = started(&path, Shop::default()).await;
    assert_eq!(
        server.grpc_listen_addr(),
        Some(ListenAddr::Local(LocalAddr::Unix(path.clone())))
    );
    assert_eq!(server.grpc_addr(), None, "a socket has no TCP address");

    let channel = channel(&path).await.expect("connects over the socket");
    let shelf = ShopServiceClient::new(channel.clone())
        .get_shelf(GetShelfRequest {
            name: "shelves/1".into(),
        })
        .await
        .expect("a unary call answers");
    assert_eq!(shelf.get_ref().name, "shelves/1");

    let mut watch = HealthClient::new(channel)
        .watch(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("a server stream opens")
        .into_inner();
    let first = watch
        .message()
        .await
        .expect("a message")
        .expect("not ended");
    assert_eq!(first.status, ServingStatus::Serving as i32);

    // A watch never ends on its own, and a graceful stop waits for in-flight calls.
    drop(watch);
    server.stop().await.expect("the server stops");
    assert!(!path.exists(), "the socket is removed after a stop");
}

#[tokio::test]
async fn reflection_and_health_answer_over_a_unix_socket() {
    use tonic_reflection::pb::v1::ServerReflectionRequest;
    use tonic_reflection::pb::v1::server_reflection_client::ServerReflectionClient;
    use tonic_reflection::pb::v1::server_reflection_request::MessageRequest;
    use tonic_reflection::pb::v1::server_reflection_response::MessageResponse;

    let (_dir, path) = socket("reflect");
    let server = started(&path, Shop::default()).await;
    let channel = channel(&path).await.expect("connects");

    let health = HealthClient::new(channel.clone())
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("health answers");
    assert_eq!(health.get_ref().status, ServingStatus::Serving as i32);

    let request = ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::ListServices(String::new())),
    };
    let mut replies = ServerReflectionClient::new(channel)
        .server_reflection_info(tokio_stream::iter([request]))
        .await
        .expect("reflection answers")
        .into_inner();
    let reply = replies
        .message()
        .await
        .expect("a reply")
        .expect("not ended");
    let Some(MessageResponse::ListServicesResponse(list)) = reply.message_response else {
        panic!("not a service list: {reply:?}");
    };
    let names: Vec<_> = list.service.iter().map(|s| s.name.as_str()).collect();
    assert!(names.contains(&"shop.v1.ShopService"), "{names:?}");
    server.stop().await.expect("stops");
}

#[tokio::test]
async fn the_interceptor_chain_and_caller_layers_run_over_a_unix_socket() {
    let (_dir, path) = socket("chain");
    let layered = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&layered);
    let mut server = server(&path, Shop::default())
        .with_unary_interceptor(Arc::new(|request: tonic::Request<()>| {
            match request.metadata().get("authorization") {
                Some(value) if value == "Bearer good" => Ok(request),
                _ => Err(tonic::Status::unauthenticated("no credentials")),
            }
        }))
        .with_grpc_layer(tower::layer::layer_fn(move |inner: GrpcService| {
            let seen = Arc::clone(&seen);
            tower::util::MapRequest::new(inner, move |request| {
                seen.fetch_add(1, Ordering::SeqCst);
                request
            })
        }));
    server.start().await.expect("starts");
    let mut client = ShopServiceClient::new(channel(&path).await.expect("connects"));

    let refused = client
        .get_shelf(GetShelfRequest {
            name: "shelves/1".into(),
        })
        .await
        .expect_err("refused without credentials");
    assert_eq!(refused.code(), tonic::Code::Unauthenticated);
    assert_eq!(
        layered.load(Ordering::SeqCst),
        0,
        "refused before the layer"
    );

    let mut request = tonic::Request::new(GetShelfRequest {
        name: "shelves/1".into(),
    });
    request
        .metadata_mut()
        .insert("authorization", "Bearer good".parse().expect("ascii"));
    client.get_shelf(request).await.expect("admitted");
    assert_eq!(
        layered.load(Ordering::SeqCst),
        1,
        "the caller's layer saw the call"
    );
    server.stop().await.expect("stops");
}

#[tokio::test]
async fn a_stale_socket_is_replaced() {
    let (_dir, path) = socket("stale");
    drop(std::os::unix::net::UnixListener::bind(&path).expect("a socket left behind"));
    assert!(path.exists(), "the stale socket file remains");
    let server = started(&path, Shop::default()).await;
    HealthClient::new(channel(&path).await.expect("connects"))
        .check(HealthCheckRequest::default())
        .await
        .expect("the new server answers");
    server.stop().await.expect("stops");
}

#[tokio::test]
async fn a_live_socket_is_refused_and_left_alone() {
    let (_dir, path) = socket("live");
    let _live = std::os::unix::net::UnixListener::bind(&path).expect("someone else's socket");
    let mut server = server(&path, Shop::default());
    match server.start().await {
        Err(Error::Listen { addr, source }) => {
            assert_eq!(source.kind(), std::io::ErrorKind::AddrInUse, "{source}");
            assert!(addr.starts_with("unix:"), "{addr}");
        }
        other => panic!("expected a listen error, got {other:?}"),
    }
    assert!(path.exists(), "the other server's socket is not removed");
}

#[tokio::test]
async fn a_file_that_is_not_a_socket_is_refused_and_kept() {
    let (_dir, path) = socket("file");
    std::fs::write(&path, b"precious").expect("a regular file");
    let mut server = server(&path, Shop::default());
    assert!(matches!(server.start().await, Err(Error::Listen { .. })));
    assert_eq!(std::fs::read(&path).expect("still there"), b"precious");
}

#[tokio::test]
async fn the_socket_is_owner_only_in_an_owner_only_directory() {
    let (dir, _) = socket("unused");
    let path = dir.0.join("run").join("grpc.sock");
    let server = started(&path, Shop::default()).await;
    let mode = |p: &Path| std::fs::metadata(p).expect("exists").permissions().mode() & 0o777;
    assert_eq!(mode(path.parent().expect("a parent")), 0o700, "directory");
    assert_eq!(mode(&path), 0o600, "socket");
    server.stop().await.expect("stops");
    assert!(!path.exists());
}

#[tokio::test]
async fn the_banner_names_the_socket() {
    let (_dir, path) = socket("banner");
    let server = started(&path, Shop::default()).await;
    let summary = Summary {
        services: vec!["shop.v1.ShopService".into()],
        health: true,
        ..Default::default()
    };
    let rows = grpc::banner::rows(server.options(), &summary);
    let expected = format!("unix:{}", path.display());
    assert!(
        rows.iter()
            .all(|row| row.section != "gRPC" || row.address == expected),
        "{rows:?}"
    );
    assert!(rows.iter().any(|row| row.address == expected), "{rows:?}");
    server.stop().await.expect("stops");
}

#[tokio::test]
async fn a_stop_lets_an_in_flight_call_finish() {
    let (_dir, path) = socket("drain");
    let shop = Shop::default();
    let server = started(&path, shop.clone()).await;
    let mut client = ShopServiceClient::new(channel(&path).await.expect("connects"));
    let call =
        tokio::spawn(async move { client.create_book(create("shelves/1", "wait", "")).await });
    shop.entered().await;

    let stopping = async { server.stop().await };
    let release = async {
        tokio::time::sleep(Duration::from_millis(200)).await;
        shop.release();
    };
    let (stopped, ()) = tokio::join!(stopping, release);
    stopped.expect("the stop drains");
    call.await
        .expect("the call task")
        .expect("the in-flight call completes during the drain");
}

#[tokio::test]
async fn a_drain_bound_cuts_a_call_that_never_finishes() {
    let (_dir, path) = socket("bound");
    let shop = Shop::default();
    let mut server = server(&path, shop.clone())
        .with_shutdown_timeout(Duration::from_secs(30))
        .with_grpc_limits(GrpcLimits::default().drain_timeout(Duration::from_millis(200)));
    server.start().await.expect("starts");
    let mut client = ShopServiceClient::new(channel(&path).await.expect("connects"));
    let call =
        tokio::spawn(async move { client.create_book(create("shelves/1", "wait", "")).await });
    shop.entered().await;

    let began = Instant::now();
    server
        .stop()
        .await
        .expect("the stop ends within its own bound");
    assert!(
        began.elapsed() < Duration::from_secs(5),
        "the drain bound, not the 30 s shutdown timeout, ended it: {:?}",
        began.elapsed()
    );
    assert!(
        call.await.expect("the call task").is_err(),
        "the call was cut"
    );
}
