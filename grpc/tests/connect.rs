//! Connect unary calls on the HTTP surface: binary and JSON, errors in Connect's shape, and
//! the interceptor chain in between — answering as Go's `WithConnect` does.

mod common;

use base64::Engine;
use base64::engine::general_purpose::STANDARD_NO_PAD;
use common::pb::{GetShelfRequest, Shelf};
use common::{SHOP_DESCRIPTOR_SET, Shop, free_port, shop_server};
use grpc::options::{Endpoint, Options};
use prost::Message;
use serde_json::Value;

fn deny(request: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
    if request
        .metadata()
        .get("authorization")
        .is_some_and(|v| v == "Bearer bad")
    {
        return Err(tonic::Status::unauthenticated("bad token"));
    }
    Ok(request)
}

/// A shop server with HTTP on, Connect on when `connect`, and its HTTP base URL.
async fn shop(connect: bool) -> (grpc::HybridServer, String) {
    let http_port = free_port();
    let mut server = grpc::HybridServer::new(Options {
        service_name: "connect-test".into(),
        grpc: Endpoint::new("127.0.0.1", free_port()),
        http: Endpoint::new("127.0.0.1", http_port),
        enable_http: true,
        ..Default::default()
    })
    .with_reflection(SHOP_DESCRIPTOR_SET)
    .with_service(shop_server(Shop::default()))
    .with_unary_interceptor(std::sync::Arc::new(deny));
    if connect {
        server = server.with_connect();
    }
    server.start().await.expect("the server starts");
    (server, format!("http://127.0.0.1:{http_port}"))
}

async fn post(
    url: &str,
    content_type: &str,
    body: Vec<u8>,
    auth: Option<&str>,
) -> reqwest::Response {
    let mut request = reqwest::Client::new()
        .post(url)
        .header("content-type", content_type)
        .header("connect-protocol-version", "1")
        .body(body);
    if let Some(auth) = auth {
        request = request.header("authorization", auth);
    }
    request.send().await.expect("request")
}

/// One error case: path, content type, body, authorization, HTTP status, Connect code.
type Case = (
    &'static str,
    &'static str,
    &'static [u8],
    Option<&'static str>,
    u16,
    &'static str,
);

const GET_SHELF: &str = "/shop.v1.ShopService/GetShelf";
const CREATE_BOOK: &str = "/shop.v1.ShopService/CreateBook";

#[tokio::test]
async fn binary_and_json_unary_calls_reach_the_service() {
    let (server, base) = shop(true).await;

    let request = GetShelfRequest {
        name: "shelves/1".into(),
    }
    .encode_to_vec();
    let binary = post(
        &format!("{base}{GET_SHELF}"),
        "application/proto",
        request,
        None,
    )
    .await;
    assert_eq!(binary.status(), 200);
    assert_eq!(binary.headers()["content-type"], "application/proto");
    let shelf = Shelf::decode(binary.bytes().await.unwrap()).unwrap();
    assert_eq!(shelf.name, "shelves/1");

    let json = post(
        &format!("{base}{GET_SHELF}"),
        "application/json; charset=utf-8",
        br#"{"name":"shelves/2"}"#.to_vec(),
        None,
    )
    .await;
    assert_eq!(json.status(), 200);
    let body: Value = json.json().await.unwrap();
    assert_eq!(body["name"], "shelves/2");

    server.stop().await.unwrap();
}

#[tokio::test]
async fn errors_use_connect_codes_statuses_and_details() {
    let (server, base) = shop(true).await;
    let cases: [Case; 6] = [
        (
            GET_SHELF,
            "application/json",
            br#"{"name":"shelves/missing"}"#,
            None,
            404,
            "not_found",
        ),
        (
            GET_SHELF,
            "application/json",
            br#"{"name":"shelves/1"}"#,
            Some("Bearer bad"),
            401,
            "unauthenticated",
        ),
        (
            GET_SHELF,
            "application/json",
            br#"{"name":"#,
            None,
            400,
            "invalid_argument",
        ),
        (
            "/shop.v1.ShopService/Nope",
            "application/proto",
            b"",
            None,
            501,
            "unimplemented",
        ),
        (
            "/no.such.v1.Service/Method",
            "application/json",
            b"{}",
            None,
            501,
            "unimplemented",
        ),
        (GET_SHELF, "text/plain", b"", None, 415, "unknown"),
    ];
    for (path, content_type, body, auth, status, code) in cases {
        let response = post(&format!("{base}{path}"), content_type, body.to_vec(), auth).await;
        assert_eq!(response.status(), status, "{path} {content_type}");
        let body: Value = response.json().await.unwrap();
        assert_eq!(body["code"], code, "{path}: {body}");
    }

    // A problem's ErrorInfo arrives as a detail: its type name and its bytes in base64.
    let sold_out = br#"{"parent":"shelves/1","bookId":"sold-out"}"#.to_vec();
    let response = post(
        &format!("{base}{CREATE_BOOK}"),
        "application/json",
        sold_out,
        None,
    )
    .await;
    let body: Value = response.json().await.unwrap();
    let info = body["details"]
        .as_array()
        .and_then(|d| d.iter().find(|d| d["type"] == "google.rpc.ErrorInfo"))
        .unwrap_or_else(|| panic!("no ErrorInfo in {body}"));
    let bytes = STANDARD_NO_PAD
        .decode(info["value"].as_str().unwrap())
        .unwrap();
    let info = tonic_types::pb::ErrorInfo::decode(bytes.as_slice()).unwrap();
    // The server's own reason and domain, decoded from Connect's base64 intact.
    assert!(
        !info.reason.is_empty() && !info.domain.is_empty(),
        "{info:?}"
    );

    server.stop().await.unwrap();
}

#[tokio::test]
async fn connect_is_off_by_default_and_health_stays_beside_it() {
    let (server, base) = shop(false).await;
    let off = post(
        &format!("{base}{GET_SHELF}"),
        "application/json",
        b"{}".to_vec(),
        None,
    )
    .await;
    assert_ne!(off.status(), 200);
    let health = reqwest::get(format!("{base}/health")).await.unwrap();
    assert_eq!(health.status(), 200);
    server.stop().await.unwrap();
}

/// The HTTP port speaks cleartext HTTP/2 (h2c) as well as HTTP/1.1, so a Connect client
/// gets multiplexing and full-duplex streams without TLS.
#[tokio::test]
async fn the_http_port_serves_cleartext_http2() {
    let (server, base) = shop(true).await;
    let client = reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .unwrap();
    let response = client
        .post(format!("{base}{GET_SHELF}"))
        .header("content-type", "application/json")
        .body(r#"{"name":"shelves/1"}"#)
        .send()
        .await
        .expect("h2c request");
    assert_eq!(response.version(), reqwest::Version::HTTP_2);
    assert_eq!(response.status(), 200);
    server.stop().await.unwrap();
}

/// `Connect-Timeout-Ms` bounds the call: a handler still running at the deadline ends with
/// `deadline_exceeded`, as Go's `WithConnect` answers.
#[tokio::test]
async fn the_connect_deadline_ends_a_call_that_runs_over() {
    let (server, base) = shop(true).await;
    let started = std::time::Instant::now();
    let response = reqwest::Client::new()
        .post(format!("{base}{CREATE_BOOK}"))
        .header("content-type", "application/json")
        .header("connect-timeout-ms", "300")
        .timeout(std::time::Duration::from_secs(5))
        .body(r#"{"parent":"shelves/1","bookId":"wait"}"#)
        .send()
        .await
        .expect("an answer before the client gives up");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(response.status(), 504);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["code"], "deadline_exceeded", "{body}");
    server.stop().await.unwrap();
}
