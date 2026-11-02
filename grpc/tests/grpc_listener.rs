//! A server with its gRPC listener off still serves REST and MCP through the loopback, and
//! opens no port for gRPC to do it.

use std::future::Future;
use std::net::TcpListener;
use std::pin::Pin;
use std::sync::Arc;

use grpc::options::{A2aTransport, Endpoint, Options};
use grpc::{Error, HybridServer, Loopback, RestGateway};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_client::HealthClient;

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
        .expect("a free port")
}

/// Answers `/check` by calling the health service through the loopback.
struct Check(Loopback);

impl RestGateway for Check {
    fn claims(&self, _: &http::Method, path: &str) -> bool {
        path == "/check"
    }

    fn serve(
        &self,
        _: axum::extract::Request,
    ) -> Pin<Box<dyn Future<Output = axum::response::Response> + Send>> {
        let loopback = self.0.clone();
        Box::pin(async move {
            let reply = HealthClient::new(loopback)
                .check(HealthCheckRequest {
                    service: String::new(),
                })
                .await
                .expect("the health service answers through the loopback");
            axum::response::Response::new(reply.get_ref().status.to_string().into())
        })
    }
}

fn options(grpc_port: u16) -> Options {
    Options {
        service_name: "listener-test".into(),
        grpc: Endpoint::new("127.0.0.1", grpc_port),
        http: Endpoint::new("127.0.0.1", 0),
        enable_http: true,
        enable_health: true,
        ..Default::default()
    }
}

#[tokio::test]
async fn rest_reaches_the_services_with_no_grpc_port_open() {
    let grpc_port = free_port();
    let mut server = HybridServer::new(options(grpc_port))
        .without_grpc_listener()
        .with_gateway(Check);
    server.start().await.expect("the server starts");

    assert_eq!(server.grpc_addr(), None, "no gRPC address to report");
    assert!(
        tokio::net::TcpStream::connect(("127.0.0.1", grpc_port))
            .await
            .is_err(),
        "nothing listens on the configured gRPC port"
    );

    let http = server.http_addr().expect("HTTP bound");
    let body = reqwest::get(format!("http://{http}/check"))
        .await
        .expect("REST answers")
        .text()
        .await
        .expect("a body");
    assert_eq!(body, "1", "SERVING, through the loopback");
    server.stop().await.expect("the server stops");
}

#[tokio::test]
async fn the_listener_is_on_unless_turned_off() {
    let mut server = HybridServer::new(options(0)).with_gateway(Check);
    server.start().await.expect("the server starts");
    let bound = server.grpc_addr().expect("gRPC bound");
    assert!(tokio::net::TcpStream::connect(bound).await.is_ok());
    server.stop().await.expect("the server stops");
    assert_eq!(server.grpc_addr(), None, "gone after a stop");
}

#[tokio::test]
async fn a_dialling_gateway_is_refused_without_a_listener() {
    let mut server = HybridServer::new(options(0))
        .without_grpc_listener()
        .with_http_gateways(Arc::new(|router, _| Ok(router)));
    assert!(matches!(
        server.start().await,
        Err(Error::GrpcListenerRequired("with_http_gateways"))
    ));
}

#[tokio::test]
async fn a2a_over_grpc_is_refused_without_a_listener() {
    let mut opts = options(0);
    opts.enable_a2a = true;
    opts.a2a.transports = vec![A2aTransport::Grpc];
    let mut server = HybridServer::new(opts).without_grpc_listener();
    assert!(matches!(
        server.start().await,
        Err(Error::GrpcListenerRequired(_))
    ));
}

#[tokio::test]
async fn no_host_is_needed_for_a_listener_that_is_not_opened() {
    let mut opts = options(0);
    opts.grpc.host.clear();
    let mut server = HybridServer::new(opts).without_grpc_listener();
    server.start().await.expect("the server starts");
    server.stop().await.expect("the server stops");
}
