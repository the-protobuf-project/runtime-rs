//! Message-level interceptors see the decoded request and reply, on both the gRPC listener
//! and the REST gateway's loopback.

use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use grpc::options::{Endpoint, Options};
use grpc::{HybridServer, Interceptor, Next, UnaryCall, UnaryReply};
use prost_reflect::Value;
use tonic::Status;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_client::HealthClient;

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
        .expect("a free port")
}

/// Rejects a check for the service named `forbidden`, rewrites `alias` to the whole-server
/// check, and records the status it saw in every reply.
#[derive(Default)]
struct Policy {
    seen_serving: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Interceptor for Policy {
    async fn intercept(&self, mut call: UnaryCall, next: Next<'_>) -> Result<UnaryReply, Status> {
        assert_eq!(call.full_method(), "/grpc.health.v1.Health/Check");
        let request = call.message()?;
        let service = request
            .get_field_by_name("service")
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or_default();

        if service == "forbidden" {
            return Err(Status::permission_denied(
                "That service is not yours to check.",
            ));
        }
        if service == "alias" {
            call.set_message(&HealthCheckRequest {
                service: String::new(),
            });
        }

        let reply = next.run(call).await?;
        let status = reply
            .message()?
            .get_field_by_name("status")
            .map(|v| v.into_owned());
        if status == Some(Value::EnumNumber(1)) {
            self.seen_serving.fetch_add(1, Ordering::SeqCst);
        }
        Ok(reply)
    }
}

#[tokio::test]
async fn a_message_interceptor_reads_and_rewrites_requests_and_replies() {
    let grpc_port = free_port();
    let seen = Arc::new(AtomicUsize::new(0));
    let mut server = HybridServer::new(Options {
        service_name: "intercept-test".into(),
        grpc: Endpoint::new("127.0.0.1", grpc_port),
        enable_health: true,
        ..Default::default()
    })
    .with_reflection(tonic_health::pb::FILE_DESCRIPTOR_SET)
    .with_interceptor(Policy {
        seen_serving: Arc::clone(&seen),
    });
    server.start().await.expect("the server starts");

    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{grpc_port}"))
        .expect("a valid endpoint")
        .connect()
        .await
        .expect("connects");
    let mut client = HealthClient::new(channel);

    let serving = client
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("the whole-server check is served");
    assert_eq!(serving.into_inner().status, 1);

    // Without the rewrite the health service would answer NOT_FOUND for "alias".
    let aliased = client
        .check(HealthCheckRequest {
            service: "alias".into(),
        })
        .await
        .expect("the interceptor rewrote the request");
    assert_eq!(aliased.into_inner().status, 1);

    let denied = client
        .check(HealthCheckRequest {
            service: "forbidden".into(),
        })
        .await
        .expect_err("the interceptor rejects");
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);

    // An error the service itself returns passes back through unchanged.
    let unknown = client
        .check(HealthCheckRequest {
            service: "nope".into(),
        })
        .await
        .expect_err("the service rejects an unknown name");
    assert_eq!(unknown.code(), tonic::Code::NotFound);

    assert_eq!(
        seen.load(Ordering::SeqCst),
        2,
        "the interceptor read both replies"
    );
    server.stop().await.expect("the server stops");
}

#[tokio::test]
async fn a_validator_over_the_tcp_listener_is_told_the_method() {
    let grpc_port = free_port();
    let seen = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let recorder = Arc::clone(&seen);
    let mut server = HybridServer::new(Options {
        service_name: "validator-test".into(),
        grpc: Endpoint::new("127.0.0.1", grpc_port),
        enable_health: true,
        ..Default::default()
    })
    .with_validator(move |call: &UnaryCall| -> Result<(), Status> {
        recorder
            .lock()
            .unwrap()
            .push(call.full_method().to_string());
        Ok(())
    });
    server.start().await.expect("the server starts");

    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{grpc_port}"))
        .expect("a valid endpoint")
        .connect()
        .await
        .expect("connects");
    HealthClient::new(channel)
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("the check is served");

    assert_eq!(
        *seen.lock().unwrap(),
        ["/grpc.health.v1.Health/Check"],
        "the validator saw the method a TCP caller reached"
    );
    server.stop().await.expect("the server stops");
}
