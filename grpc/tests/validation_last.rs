//! `with_validation_last` moves validation behind the caller's message-level interceptors,
//! so an unauthenticated caller learns it is unauthenticated before it learns anything else.
//!
//! Validation is a message-level interceptor, so the order it changes is against
//! [`HybridServer::with_interceptor`]. Metadata-level interceptors registered with
//! `with_unary_interceptor` run before the whole message chain either way.

use std::net::TcpListener;

use grpc::options::{Endpoint, Options};
use grpc::{HybridServer, Interceptor, Next, UnaryCall, UnaryReply};
use tonic::Status;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_client::HealthClient;

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
        .expect("a free port")
}

/// Refuses any call without an `authorization` header.
struct Authenticate;

#[async_trait::async_trait]
impl Interceptor for Authenticate {
    async fn intercept(&self, call: UnaryCall, next: Next<'_>) -> Result<UnaryReply, Status> {
        if !call.metadata.contains_key("authorization") {
            return Err(Status::unauthenticated("who are you?"));
        }
        next.run(call).await
    }
}

fn refuse_everything(_call: &UnaryCall) -> Result<(), Status> {
    Err(Status::invalid_argument("every field is wrong"))
}

/// Starts a server with an authenticating interceptor and a validator that refuses every
/// request, and returns the code an anonymous check is answered with.
async fn anonymous_check(last: bool) -> tonic::Code {
    let grpc_port = free_port();
    let mut server = HybridServer::new(Options {
        service_name: "validation-order-test".into(),
        grpc: Endpoint::new("127.0.0.1", grpc_port),
        enable_health: true,
        ..Default::default()
    })
    .with_interceptor(Authenticate)
    .with_validator(refuse_everything);
    if last {
        server = server.with_validation_last();
    }
    server.start().await.expect("the server starts");

    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{grpc_port}"))
        .expect("a valid endpoint")
        .connect()
        .await
        .expect("connects");
    let code = HealthClient::new(channel)
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect_err("refused one way or the other")
        .code();
    server.stop().await.expect("the server stops");
    code
}

#[tokio::test]
async fn validation_runs_first_by_default() {
    assert_eq!(anonymous_check(false).await, tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn validation_last_lets_authentication_answer_first() {
    assert_eq!(anonymous_check(true).await, tonic::Code::Unauthenticated);
}
