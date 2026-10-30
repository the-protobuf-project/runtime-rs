//! Stream interceptors guard streaming calls on the TCP listener and the loopback alike, in
//! registration order, and see how each stream ended.

use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use grpc::options::{Endpoint, Options};
use grpc::{HybridServer, Loopback, RestGateway, StreamCall, StreamInterceptor, StreamNext};
use tokio_stream::StreamExt as _;
use tonic::Status;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_client::HealthClient;
use tonic_reflection::pb::v1::server_reflection_client::ServerReflectionClient;
use tonic_reflection::pb::v1::server_reflection_request::MessageRequest;
use tonic_reflection::pb::v1::{FILE_DESCRIPTOR_SET as REFLECTION_SET, ServerReflectionRequest};

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
        .expect("a free port")
}

type Log = Arc<Mutex<Vec<String>>>;

/// Records when it is entered and how the stream ended, refuses a call carrying `x-deny`,
/// and turns a clean end into `ABORTED` when asked to with `x-rewrite`.
struct Recorder {
    name: &'static str,
    log: Log,
}

#[async_trait::async_trait]
impl StreamInterceptor for Recorder {
    async fn intercept(&self, call: StreamCall, next: StreamNext<'_>) -> Result<(), Status> {
        let method = call.full_method().to_string();
        self.log
            .lock()
            .unwrap()
            .push(format!("{} in {method}", self.name));
        if call.metadata.contains_key("x-deny") {
            return Err(Status::permission_denied("streams are closed"));
        }
        let rewrite = call.metadata.contains_key("x-rewrite");
        let ended = next.run(call).await;
        let code = ended.as_ref().err().map_or(tonic::Code::Ok, Status::code);
        self.log
            .lock()
            .unwrap()
            .push(format!("{} out {code:?}", self.name));
        match ended {
            Ok(()) if rewrite => Err(Status::aborted("rewritten")),
            other => other,
        }
    }
}

/// Claims nothing; it exists to hand the test the server's loopback.
struct Capture;

impl RestGateway for Capture {
    fn claims(&self, _method: &http::Method, _path: &str) -> bool {
        false
    }

    fn serve(
        &self,
        _request: axum::extract::Request,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = axum::response::Response> + Send>> {
        unreachable!("claims nothing")
    }
}

async fn started(log: &Log, loopback: &Arc<Mutex<Option<Loopback>>>) -> (HybridServer, u16) {
    let grpc_port = free_port();
    let stash = Arc::clone(loopback);
    let mut server = HybridServer::new(Options {
        service_name: "stream-test".into(),
        grpc: Endpoint::new("127.0.0.1", grpc_port),
        enable_health: true,
        ..Default::default()
    })
    .with_reflection(tonic_health::pb::FILE_DESCRIPTOR_SET)
    .with_reflection(REFLECTION_SET)
    .with_stream_interceptor(Recorder {
        name: "a",
        log: Arc::clone(log),
    })
    .with_stream_interceptor(Recorder {
        name: "b",
        log: Arc::clone(log),
    })
    .with_gateway(move |loopback| {
        *stash.lock().unwrap() = Some(loopback);
        Capture
    });
    server.start().await.expect("the server starts");
    (server, grpc_port)
}

async fn channel(port: u16) -> tonic::transport::Channel {
    tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
        .expect("a valid endpoint")
        .connect()
        .await
        .expect("connects")
}

fn list_services() -> ServerReflectionRequest {
    ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::ListServices(String::new())),
    }
}

/// Waits for the log to hold `want` entries, so a stream's end has been recorded.
async fn settled(log: &Log, want: usize) -> Vec<String> {
    for _ in 0..200 {
        if log.lock().unwrap().len() >= want {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    std::mem::take(&mut *log.lock().unwrap())
}

#[tokio::test]
async fn stream_interceptors_chain_in_order_and_see_the_end() {
    let log = Log::default();
    let loopback = Arc::default();
    let (server, port) = started(&log, &loopback).await;
    let mut client = ServerReflectionClient::new(channel(port).await);
    let method = "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo";

    let mut replies = client
        .server_reflection_info(tokio_stream::iter([list_services()]))
        .await
        .expect("the stream opens")
        .into_inner();
    let mut count = 0;
    while let Some(reply) = replies.next().await {
        reply.expect("a reply");
        count += 1;
    }
    assert_eq!(count, 1);
    assert_eq!(
        settled(&log, 4).await,
        [
            format!("a in {method}"),
            format!("b in {method}"),
            "b out Ok".to_string(),
            "a out Ok".to_string(),
        ]
    );

    // A unary method with a registered descriptor is not theirs to see.
    HealthClient::new(channel(port).await)
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("the check is served");
    assert!(settled(&log, 0).await.is_empty());

    server.stop().await.expect("the server stops");
}

#[tokio::test]
async fn a_stream_interceptor_can_refuse_and_rewrite_over_tcp() {
    let log = Log::default();
    let loopback = Arc::default();
    let (server, port) = started(&log, &loopback).await;
    let mut client = ServerReflectionClient::new(channel(port).await);

    let mut denied = tonic::Request::new(tokio_stream::iter([list_services()]));
    denied
        .metadata_mut()
        .insert("x-deny", "1".parse().expect("ascii"));
    let refused = client
        .server_reflection_info(denied)
        .await
        .expect_err("the first interceptor refuses");
    assert_eq!(refused.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        settled(&log, 1).await.len(),
        1,
        "the second never ran, and neither did the service"
    );

    let mut rewritten = tonic::Request::new(tokio_stream::iter([list_services()]));
    rewritten
        .metadata_mut()
        .insert("x-rewrite", "1".parse().expect("ascii"));
    let mut replies = client
        .server_reflection_info(rewritten)
        .await
        .expect("the stream opens")
        .into_inner();
    replies
        .next()
        .await
        .expect("a message")
        .expect("the reply arrives untouched");
    let end = replies
        .next()
        .await
        .expect("the end of the stream")
        .expect_err("the interceptor rewrote how it ended");
    assert_eq!(end.code(), tonic::Code::Aborted);

    server.stop().await.expect("the server stops");
}

#[tokio::test]
async fn a_caller_hanging_up_reads_as_cancelled() {
    let log = Log::default();
    let loopback = Arc::default();
    let (server, port) = started(&log, &loopback).await;

    let mut watch = HealthClient::new(channel(port).await)
        .watch(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("the watch opens")
        .into_inner();
    let first = watch.next().await.expect("a message").expect("serving");
    assert_eq!(first.status, 1);
    drop(watch);

    let entries = settled(&log, 4).await;
    assert_eq!(entries[2], "b out Cancelled", "{entries:?}");
    server.stop().await.expect("the server stops");
}

#[tokio::test]
async fn the_loopback_runs_the_same_chain() {
    let log = Log::default();
    let loopback = Arc::new(Mutex::new(None));
    let (server, _) = started(&log, &loopback).await;
    let loopback = loopback.lock().unwrap().clone().expect("captured");
    let mut client = HealthClient::new(loopback);

    let mut denied = tonic::Request::new(HealthCheckRequest {
        service: String::new(),
    });
    denied
        .metadata_mut()
        .insert("x-deny", "1".parse().expect("ascii"));
    let refused = client.watch(denied).await.expect_err("refused in-process");
    assert_eq!(refused.code(), tonic::Code::PermissionDenied);

    let mut watch = client
        .watch(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("the watch opens")
        .into_inner();
    assert_eq!(
        watch.next().await.expect("a message").expect("ok").status,
        1
    );
    drop(watch);
    let entries = settled(&log, 5).await;
    assert_eq!(entries[1], "a in /grpc.health.v1.Health/Watch");
    server.stop().await.expect("the server stops");
}
