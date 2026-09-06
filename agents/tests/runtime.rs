//! What a runtime refuses, and what it fills in: the checks that happen before
//! anything binds.

mod common;

use std::time::Duration;

use agents::{Config, Error, Protocol, Runtime};
use tokio_util::sync::CancellationToken;

use common::*;

#[tokio::test]
async fn start_with_nothing_registered_is_refused() {
    let runtime = Runtime::new(config(0));
    let err = runtime.start(CancellationToken::new()).await.unwrap_err();
    assert!(matches!(err, Error::NoServices), "got {err:?}");
}

#[tokio::test]
async fn a_second_start_is_refused() {
    let port = free_port().await;
    let runtime =
        Runtime::new(config(port)).register(Fake::mounting(Protocol::Mcp, "/mcp", "tools"));
    let cancel = CancellationToken::new();

    runtime.start(cancel.clone()).await.expect("first start");
    let err = runtime.start(cancel.clone()).await.unwrap_err();
    assert!(matches!(err, Error::AlreadyStarted), "got {err:?}");

    cancel.cancel();
    runtime.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn a_grpc_binding_without_a_registry_is_refused() {
    let runtime = Runtime::new(config(0))
        .register(Fake::mounting(Protocol::A2a, "/a2a", "agent").needing_grpc());

    let err = runtime.start(CancellationToken::new()).await.unwrap_err();
    assert!(
        matches!(err, Error::NeedsGrpcRoutes(Protocol::A2a)),
        "got {err:?}"
    );
}

#[tokio::test]
async fn a_service_that_never_mounts_times_out() {
    let runtime = Runtime::new(Config {
        ready_timeout: Some(Duration::from_millis(150)),
        ..config(free_port().await)
    })
    .register(Fake::mounting(Protocol::Mcp, "/mcp", "tools").behaving(Behaviour::NeverMount));

    let err = runtime.start(CancellationToken::new()).await.unwrap_err();
    assert!(
        matches!(
            err,
            Error::MountTimeout {
                protocol: Protocol::Mcp,
                ..
            }
        ),
        "got {err:?}"
    );
}

#[tokio::test]
async fn a_service_that_fails_on_the_way_up_reports_its_own_error() {
    let runtime = Runtime::new(config(free_port().await)).register(
        Fake::mounting(Protocol::Mcp, "/mcp", "tools").behaving(Behaviour::FailBeforeReady),
    );

    let err = runtime.start(CancellationToken::new()).await.unwrap_err();
    match err {
        Error::FailedToStart { protocol, source } => {
            assert_eq!(protocol, Protocol::Mcp);
            assert!(source.to_string().contains("refused to mount"), "{source}");
        }
        other => panic!("got {other:?}"),
    }
}

#[tokio::test]
async fn a_service_that_returns_before_mounting_is_not_a_stall() {
    let runtime = Runtime::new(config(free_port().await)).register(
        Fake::mounting(Protocol::A2a, "/a2a", "agent").behaving(Behaviour::StopBeforeReady),
    );

    let err = runtime.start(CancellationToken::new()).await.unwrap_err();
    assert!(
        matches!(err, Error::StoppedBeforeMount(Protocol::A2a)),
        "got {err:?}"
    );
}

#[tokio::test]
async fn identity_and_defaults_reach_the_placement() {
    let runtime = Runtime::new(Config {
        host: String::new(),
        ..config(free_port().await)
    });
    assert_eq!(runtime.config().host, agents::DEFAULT_HOST);
    assert_eq!(runtime.config().identity.version, "1.2.3");
    assert_eq!(
        runtime.config().ready_timeout,
        Some(agents::DEFAULT_READY_TIMEOUT)
    );
}
