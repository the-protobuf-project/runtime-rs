//! Counterpart to runtime-go/network/transport_test.go.
//!
//! The Go test reaches into `http.Transport` and asserts `MaxIdleConns`/`MaxIdleConnsPerHost`
//! are >= 100 and that the transport is not the shared `http.DefaultTransport`. `reqwest`
//! exposes no equivalent getters — pool settings are consumed by the builder and never
//! readable — so the configuration itself cannot be asserted here. What these tests pin instead
//! is the behaviour that configuration exists to protect: many concurrent requests to a single
//! host all complete through one client.

use std::collections::HashMap;
use std::time::Duration;

use network::{ConnectionOptions, HttpClient, HttpMethod, UrlOptions, UrlScheme};
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
        timeout: Duration::from_secs(5),
        skip_connectivity_check: true,
        ..Default::default()
    }
}

#[tokio::test]
async fn pooled_client_serves_concurrent_single_host_traffic() {
    const CONCURRENCY: usize = 50;

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/pool"))
        .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
        .expect(CONCURRENCY as u64)
        .mount(&server)
        .await;

    let mut http = HttpClient::default();
    http.connect(opts_for(&server, "/pool")).await.unwrap();

    let opts = opts_for(&server, "/pool");
    let headers = HashMap::new();
    let results =
        futures_util::future::join_all((0..CONCURRENCY).map(|_| {
            http.request_sync(HttpMethod::Get, &opts.url, Vec::new(), &headers, 0, 0, None)
        }))
        .await;

    for result in results {
        assert_eq!(result.unwrap(), b"ok");
    }
    server.verify().await;
}

#[tokio::test]
async fn clients_are_independent() {
    // Go asserts two pooled clients do not share a transport. The observable analogue: two
    // clients are separately connectable and usable, and closing one does not disturb the other.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/independent"))
        .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
        .mount(&server)
        .await;

    let mut a = HttpClient::default();
    let mut b = HttpClient::default();
    a.connect(opts_for(&server, "/independent")).await.unwrap();
    b.connect(opts_for(&server, "/independent")).await.unwrap();

    a.close().await.unwrap();

    let opts = opts_for(&server, "/independent");
    let headers = HashMap::new();
    let from_b = b
        .request_sync(HttpMethod::Get, &opts.url, Vec::new(), &headers, 0, 0, None)
        .await
        .expect("closing one client must not affect the other");
    assert_eq!(from_b, b"ok");
}
