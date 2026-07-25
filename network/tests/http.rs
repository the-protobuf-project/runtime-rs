use std::collections::HashMap;
use std::time::Duration;

use network::{ClientType, ConnectionOptions, Error, HttpMethod, Network, UrlOptions, UrlScheme};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn opts_for(server: &MockServer, path: &str) -> ConnectionOptions {
    let url = url::Url::parse(&server.uri()).unwrap();
    ConnectionOptions {
        url: UrlOptions {
            scheme: UrlScheme::Http,
            host: format!("{}:{}", url.host_str().unwrap(), url.port().unwrap()),
            paths: vec![path.to_string()],
            params: HashMap::new(),
        },
        timeout: Duration::from_secs(2),
        ..Default::default()
    }
}

#[tokio::test]
async fn connect_head_success_skips_get() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .and(path("/api"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let mut network = Network::new_connection(ClientType::Http).unwrap();
    network.with_opts(opts_for(&server, "/api")).await.unwrap();

    server.verify().await;
}

#[tokio::test]
async fn connect_head_405_falls_back_to_get() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .and(path("/api"))
        .respond_with(ResponseTemplate::new(405))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api"))
        .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
        .expect(1)
        .mount(&server)
        .await;

    let mut network = Network::new_connection(ClientType::Http).unwrap();
    network.with_opts(opts_for(&server, "/api")).await.unwrap();

    server.verify().await;
}

#[tokio::test]
async fn connect_skip_connectivity_check_sends_no_request() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;

    let mut opts = opts_for(&server, "/api");
    opts.skip_connectivity_check = true;
    let mut network = Network::new_connection(ClientType::Http).unwrap();
    network.with_opts(opts).await.unwrap();

    server.verify().await;
}

#[tokio::test]
async fn connect_fails_when_unreachable() {
    let mut opts = ConnectionOptions {
        url: UrlOptions {
            scheme: UrlScheme::Http,
            host: "127.0.0.1:1".to_string(),
            paths: vec!["/api".to_string()],
            params: HashMap::new(),
        },
        timeout: Duration::from_millis(300),
        ..Default::default()
    };
    opts.retries = 0;
    let mut network = Network::new_connection(ClientType::Http).unwrap();
    let err = network.with_opts(opts).await.unwrap_err();
    match err {
        Error::HttpConnect { host, .. } => assert_eq!(host, "127.0.0.1:1"),
        other => panic!("expected HttpConnect, got {other:?}"),
    }
}

#[tokio::test]
async fn request_retries_and_wraps_last_error() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/always-500"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let opts = opts_for(&server, "/always-500");
    let mut network = Network::new_connection(ClientType::Http).unwrap();
    network.with_opts(opts.clone()).await.unwrap();
    let http = network.as_http().unwrap();

    let err = http
        .request(
            HttpMethod::Get,
            &opts.url,
            Vec::new(),
            &HashMap::new(),
            0,
            2,
            None,
        )
        .await
        .unwrap_err();

    match err {
        Error::RetryExhausted { retries, source } => {
            assert_eq!(retries, 2);
            assert!(matches!(*source, Error::ServerStatus(500)));
        }
        other => panic!("expected RetryExhausted, got {other:?}"),
    }
}

#[tokio::test]
async fn request_cancellation_stops_immediately() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/slow"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
        .mount(&server)
        .await;

    let mut opts = opts_for(&server, "/slow");
    opts.timeout = Duration::from_secs(10);
    let mut network = Network::new_connection(ClientType::Http).unwrap();
    network.with_opts(opts.clone()).await.unwrap();
    let http = network.as_http().unwrap();

    let cancel = tokio_util::sync::CancellationToken::new();
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel_clone.cancel();
    });

    let start = std::time::Instant::now();
    let err = http
        .request(
            HttpMethod::Get,
            &opts.url,
            Vec::new(),
            &HashMap::new(),
            0,
            0,
            Some(&cancel),
        )
        .await
        .unwrap_err();
    assert!(start.elapsed() < Duration::from_secs(2));
    assert!(matches!(err, Error::Cancelled));
}

#[tokio::test]
async fn request_success_returns_body() {
    let server = MockServer::start().await;
    Mock::given(method("HEAD"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/data"))
        .respond_with(ResponseTemplate::new(200).set_body_string("hello"))
        .mount(&server)
        .await;

    let opts = opts_for(&server, "/data");
    let mut network = Network::new_connection(ClientType::Http).unwrap();
    network.with_opts(opts.clone()).await.unwrap();
    let http = network.as_http().unwrap();

    let data = http
        .request(
            HttpMethod::Get,
            &opts.url,
            Vec::new(),
            &HashMap::new(),
            0,
            0,
            None,
        )
        .await
        .unwrap();
    assert_eq!(data, b"hello");
}
