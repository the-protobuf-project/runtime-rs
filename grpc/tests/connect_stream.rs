//! Connect streaming on the HTTP surface: server streaming, client streaming and full-duplex
//! bidi over HTTP/2, each ending with the end-stream envelope — as Go's `WithConnect` does.

mod common;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use common::pb::{CreateBookRequest, GetShelfRequest, Shelf};
use common::{SHOP_DESCRIPTOR_SET, Shop, free_port, shop_server};
use futures_util::StreamExt;
use grpc::options::{Endpoint, Options};
use prost::Message;
use serde_json::Value;

const BASE: &str = "/shop.v1.ShopService/";

async fn shop() -> (grpc::HybridServer, String, reqwest::Client) {
    let http_port = free_port();
    let mut server = grpc::HybridServer::new(Options {
        service_name: "connect-stream-test".into(),
        grpc: Endpoint::new("127.0.0.1", free_port()),
        http: Endpoint::new("127.0.0.1", http_port),
        enable_http: true,
        ..Default::default()
    })
    .with_reflection(SHOP_DESCRIPTOR_SET)
    .with_service(shop_server(Shop::default()))
    .with_connect();
    server.start().await.expect("the server starts");
    let client = reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .unwrap();
    (
        server,
        format!("http://127.0.0.1:{http_port}{BASE}"),
        client,
    )
}

fn envelope(flags: u8, data: &[u8]) -> Bytes {
    let mut out = BytesMut::new();
    out.put_u8(flags);
    out.put_u32(data.len() as u32);
    out.put_slice(data);
    out.freeze()
}

/// Splits a whole response body into its envelopes: messages, then the end-stream JSON.
fn envelopes(mut body: Bytes) -> (Vec<Bytes>, Value) {
    let mut messages = Vec::new();
    loop {
        let flags = body.get_u8();
        let len = body.get_u32() as usize;
        let data = body.split_to(len);
        if flags & 0x02 != 0 {
            assert!(body.is_empty(), "bytes after the end-stream envelope");
            return (messages, serde_json::from_slice(&data).unwrap());
        }
        messages.push(data);
    }
}

async fn call(
    client: &reqwest::Client,
    url: String,
    content_type: &str,
    body: Vec<u8>,
) -> (Vec<Bytes>, Value) {
    let response = client
        .post(url)
        .header("content-type", content_type)
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["content-type"], content_type);
    envelopes(response.bytes().await.unwrap())
}

#[tokio::test]
async fn server_streaming_relays_each_reply_then_the_trailers() {
    let (server, base, client) = shop().await;
    let request = envelope(
        0,
        &GetShelfRequest {
            name: "shelves/1".into(),
        }
        .encode_to_vec(),
    );
    let (replies, end) = call(
        &client,
        format!("{base}WatchShelf"),
        "application/connect+proto",
        request.to_vec(),
    )
    .await;
    let themes: Vec<_> = replies
        .iter()
        .map(|r| Shelf::decode(r.clone()).unwrap().theme)
        .collect();
    assert_eq!(themes, ["v1", "v2", "v3"]);
    assert!(end.get("error").is_none(), "{end}");
    server.stop().await.unwrap();
}

#[tokio::test]
async fn an_error_mid_stream_arrives_in_the_end_stream_envelope() {
    let (server, base, client) = shop().await;
    let request = envelope(0, br#"{"name":"shelves/fail"}"#);
    let (replies, end) = call(
        &client,
        format!("{base}WatchShelf"),
        "application/connect+json",
        request.to_vec(),
    )
    .await;
    assert_eq!(replies.len(), 1);
    let first: Value = serde_json::from_slice(&replies[0]).unwrap();
    assert_eq!(first["theme"], "v1");
    assert_eq!(end["error"]["code"], "failed_precondition", "{end}");
    server.stop().await.unwrap();
}

#[tokio::test]
async fn client_streaming_counts_every_request() {
    let (server, base, client) = shop().await;
    let mut body = Vec::new();
    for id in ["a", "b", "c"] {
        let book = CreateBookRequest {
            parent: "shelves/1".into(),
            book_id: id.into(),
            ..Default::default()
        };
        body.extend_from_slice(&envelope(0, &book.encode_to_vec()));
    }
    let (replies, end) = call(
        &client,
        format!("{base}ImportBooks"),
        "application/connect+proto",
        body,
    )
    .await;
    assert_eq!(replies.len(), 1);
    assert_eq!(
        common::pb::ImportBooksResponse::decode(replies[0].clone())
            .unwrap()
            .imported,
        3
    );
    assert!(end.get("error").is_none(), "{end}");
    server.stop().await.unwrap();
}

/// Bidi is full duplex: each reply is read before the next request is sent.
#[tokio::test]
async fn bidi_answers_each_request_before_the_next_is_sent() {
    let (server, base, client) = shop().await;
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(4);
    let send = |name: &str| envelope(0, &GetShelfRequest { name: name.into() }.encode_to_vec());
    tx.send(Ok(send("shelves/a"))).await.unwrap();
    let response = client
        .post(format!("{base}Chat"))
        .header("content-type", "application/connect+proto")
        .body(reqwest::Body::wrap_stream(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let mut chunks = response.bytes_stream();
    let mut buf = BytesMut::new();
    let mut next_message = async |buf: &mut BytesMut| loop {
        if buf.len() >= 5 {
            let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
            if buf.len() >= 5 + len {
                let flags = buf[0];
                buf.advance(5);
                return (flags, buf.split_to(len).freeze());
            }
        }
        buf.extend_from_slice(&chunks.next().await.expect("more").unwrap());
    };
    let (_, first) = next_message(&mut buf).await;
    assert_eq!(Shelf::decode(first).unwrap().name, "shelves/a");
    tx.send(Ok(send("shelves/b"))).await.unwrap();
    let (_, second) = next_message(&mut buf).await;
    assert_eq!(Shelf::decode(second).unwrap().name, "shelves/b");
    drop(tx);
    let (flags, end) = next_message(&mut buf).await;
    assert_eq!(flags & 0x02, 0x02);
    let end: Value = serde_json::from_slice(&end).unwrap();
    assert!(end.get("error").is_none(), "{end}");
    server.stop().await.unwrap();
}

/// A deadline ends a stream whose client is still sending.
#[tokio::test]
async fn the_connect_deadline_ends_an_open_stream() {
    let (server, base, client) = shop().await;
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(1);
    tx.send(Ok(envelope(
        0,
        &GetShelfRequest {
            name: "shelves/a".into(),
        }
        .encode_to_vec(),
    )))
    .await
    .unwrap();
    let response = client
        .post(format!("{base}Chat"))
        .header("content-type", "application/connect+proto")
        .header("connect-timeout-ms", "300")
        .timeout(std::time::Duration::from_secs(5))
        .body(reqwest::Body::wrap_stream(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ))
        .send()
        .await
        .unwrap();
    let (replies, end) = envelopes(
        response
            .bytes()
            .await
            .expect("the stream ends before the client gives up"),
    );
    assert_eq!(replies.len(), 1);
    assert_eq!(end["error"]["code"], "deadline_exceeded", "{end}");
    drop(tx);
    server.stop().await.unwrap();
}
