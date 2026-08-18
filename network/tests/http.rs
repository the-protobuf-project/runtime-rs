use std::collections::HashMap;
use std::time::Duration;

use network::{ConnectionOptions, Error, HttpClient, HttpMethod, UrlOptions, UrlScheme};
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

    let mut http = HttpClient::default();
    http.connect(opts_for(&server, "/api")).await.unwrap();

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

    let mut http = HttpClient::default();
    http.connect(opts_for(&server, "/api")).await.unwrap();

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
    let mut http = HttpClient::default();
    http.connect(opts).await.unwrap();

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
    let mut http = HttpClient::default();
    let err = http.connect(opts).await.unwrap_err();
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
    let mut http = HttpClient::default();
    http.connect(opts.clone()).await.unwrap();

    let err = http
        .request_sync(
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
    let mut http = HttpClient::default();
    http.connect(opts.clone()).await.unwrap();

    let cancel = tokio_util::sync::CancellationToken::new();
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel_clone.cancel();
    });

    let start = std::time::Instant::now();
    let err = http
        .request_sync(
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
    let mut http = HttpClient::default();
    http.connect(opts.clone()).await.unwrap();

    let data = http
        .request_sync(
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

#[tokio::test]
async fn request_returns_http_response_with_data_on_success() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/ok"))
        .respond_with(ResponseTemplate::new(200).set_body_string("payload"))
        .mount(&server)
        .await;

    let mut http = HttpClient::default();
    let mut opts = opts_for(&server, "/ok");
    opts.skip_connectivity_check = true;
    http.connect(opts.clone()).await.unwrap();

    let resp = http
        .request(
            HttpMethod::Get,
            &opts.url,
            Vec::new(),
            &HashMap::new(),
            0,
            0,
            None,
        )
        .await;
    assert_eq!(resp.data, b"payload");
    assert!(resp.error.is_none());
    assert_eq!(resp.into_result().unwrap(), b"payload");
}

#[tokio::test]
async fn request_returns_http_response_with_error_on_failure() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/gone"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let mut http = HttpClient::default();
    let mut opts = opts_for(&server, "/gone");
    opts.skip_connectivity_check = true;
    http.connect(opts.clone()).await.unwrap();

    let resp = http
        .request(
            HttpMethod::Get,
            &opts.url,
            Vec::new(),
            &HashMap::new(),
            0,
            0,
            None,
        )
        .await;
    assert!(resp.data.is_empty());
    // Go sets only one of Data/Error; the retry driver wraps the last attempt's failure.
    assert!(matches!(resp.error, Some(Error::RetryExhausted { .. })));
    assert!(resp.into_result().is_err());
}

#[tokio::test]
async fn request_sync_matches_request_unpacked() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/same"))
        .respond_with(ResponseTemplate::new(200).set_body_string("same"))
        .mount(&server)
        .await;

    let mut http = HttpClient::default();
    let mut opts = opts_for(&server, "/same");
    opts.skip_connectivity_check = true;
    http.connect(opts.clone()).await.unwrap();

    let via_request = http
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
        .into_result()
        .unwrap();
    let via_sync = http
        .request_sync(
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
    assert_eq!(via_request, via_sync);
}

#[test]
fn graphql_scalar_aliases_name_the_expected_rust_types() {
    use network::scalars;
    let _: scalars::Boolean = true;
    let _: scalars::Float = 1.5f64;
    let _: scalars::Int = 32i32;
    let _: scalars::String = "text".to_string();
    let _: scalars::ID = network::Id::from("abc");
    // GraphQLResult<T> is Result<T>: handing one to the other only compiles if they are the
    // same type.
    let aliased: network::GraphQLResult<u8> = Ok(1);
    let as_result: network::Result<u8> = aliased;
    assert!(as_result.is_ok());
}
