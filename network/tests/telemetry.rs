//! Counterpart to runtime-go/network/observe_test.go: every request is a client
//! span, and the server receives its trace in the request's headers.

use std::collections::HashMap;
use std::time::Duration;

use network::{ConnectionOptions, HttpClient, HttpMethod, UrlOptions, UrlScheme};
use telemetry::{SpanKind, testing::Recorder};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn opts_for(server: &MockServer, path: &str, rec: Option<&Recorder>) -> ConnectionOptions {
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
        telemetry: rec.map(Recorder::signals).unwrap_or_default(),
        ..Default::default()
    }
}

#[tokio::test]
async fn a_request_is_traced_to_the_server() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/books/1"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let rec = Recorder::new();
    let mut http = HttpClient::default();
    let opts = opts_for(&server, "/books/1", Some(&rec));
    http.connect(opts.clone()).await.unwrap();
    let _ = http
        .request_sync(
            HttpMethod::Get,
            &opts.url,
            Vec::new(),
            &HashMap::new(),
            0,
            0,
            None,
        )
        .await;

    let span = rec.span("network.request").expect("request span");
    assert_eq!(span.kind, SpanKind::Client);
    assert_eq!(span.attributes["http.response.status_code"], "404");
    assert_eq!(span.attributes["url.path"], "/books/1");
    assert!(!span.failed, "a 404 is a missing record, not a failure");
    assert_eq!(rec.outcomes()["request"], "not_found");

    let received = server.received_requests().await.unwrap();
    let traceparent = received[0]
        .headers
        .get("traceparent")
        .expect("traceparent sent");
    assert!(traceparent.to_str().unwrap().contains(&span.span_id));
}

#[tokio::test]
async fn without_telemetry_nothing_is_injected() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let mut http = HttpClient::default();
    let opts = opts_for(&server, "/plain", None);
    http.connect(opts.clone()).await.unwrap();
    http.request_sync(
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
    let received = server.received_requests().await.unwrap();
    assert!(received[0].headers.get("traceparent").is_none());
}
