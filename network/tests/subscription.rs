use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use network::{ClientType, ConnectionOptions, FieldArg, Network, UrlOptions, UrlScheme};
use serde::Deserialize;
use serde_json::json;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

/// A minimal graphql-transport-ws test server: acks connection_init, waits for a subscribe
/// message, streams three `next` payloads, then completes.
async fn start_subscription_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else { return };
            tokio::spawn(async move {
                #[allow(clippy::result_large_err)]
                let echo_subprotocol = |req: &tokio_tungstenite::tungstenite::handshake::server::Request,
                                         mut response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                    if let Some(proto) = req.headers().get("Sec-WebSocket-Protocol") {
                        response.headers_mut().insert("Sec-WebSocket-Protocol", proto.clone());
                    }
                    Ok(response)
                };
                let Ok(ws) = tokio_tungstenite::accept_hdr_async(stream, echo_subprotocol).await else { return };
                let (mut sink, mut stream) = ws.split();

                loop {
                    let Some(Ok(Message::Text(txt))) = stream.next().await else { return };
                    let v: serde_json::Value = serde_json::from_str(&txt).unwrap();
                    if v["type"] == "connection_init" {
                        break;
                    }
                }
                let ack = json!({"type": "connection_ack"}).to_string();
                if sink.send(Message::Text(ack.into())).await.is_err() {
                    return;
                }

                let sub_id;
                loop {
                    let Some(Ok(Message::Text(txt))) = stream.next().await else { return };
                    let v: serde_json::Value = serde_json::from_str(&txt).unwrap();
                    if v["type"] == "subscribe" {
                        sub_id = v["id"].as_str().unwrap().to_string();
                        break;
                    }
                }

                for i in 0..3 {
                    let next = json!({
                        "type": "next",
                        "id": sub_id,
                        "payload": {"data": {"counter": i}},
                    })
                    .to_string();
                    if sink.send(Message::Text(next.into())).await.is_err() {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                let complete = json!({"type": "complete", "id": sub_id}).to_string();
                let _ = sink.send(Message::Text(complete.into())).await;
            });
        }
    });
    addr
}

fn opts_for(addr: SocketAddr) -> ConnectionOptions {
    ConnectionOptions {
        url: UrlOptions {
            scheme: UrlScheme::Http,
            host: addr.to_string(),
            paths: vec!["/graphql".to_string()],
            params: HashMap::new(),
        },
        timeout: Duration::from_secs(2),
        skip_connectivity_check: true,
        ..Default::default()
    }
}

#[derive(Deserialize, Debug)]
struct Counter {
    counter: i32,
}

#[tokio::test]
async fn subscribe_fields_streams_decoded_updates_then_completes() {
    let addr = start_subscription_server().await;
    let mut network = Network::new_connection(ClientType::GraphQL).unwrap();
    network.with_opts(opts_for(addr)).await.unwrap();
    let gql = network.as_graphql().unwrap();

    let mut sub = gql
        .subscribe_fields::<Counter>("counterUpdated", &[FieldArg::new("from", 0, "Int!")], "{ counter }", None)
        .await
        .unwrap();

    let mut seen = Vec::new();
    for _ in 0..3 {
        let update = tokio::time::timeout(Duration::from_secs(2), sub.updates().recv())
            .await
            .expect("timed out waiting for update")
            .expect("channel closed early")
            .expect("expected Ok update");
        seen.push(update.counter);
    }
    assert_eq!(seen, vec![0, 1, 2]);

    sub.stop().await.unwrap();
}

#[tokio::test]
async fn subscribe_fields_cancellation_stops_subscription() {
    let addr = start_subscription_server().await;
    let mut network = Network::new_connection(ClientType::GraphQL).unwrap();
    network.with_opts(opts_for(addr)).await.unwrap();
    let gql = network.as_graphql().unwrap();

    let cancel = tokio_util::sync::CancellationToken::new();
    let mut sub = gql
        .subscribe_fields::<Counter>("counterUpdated", &[], "{ counter }", Some(cancel.clone()))
        .await
        .unwrap();

    cancel.cancel();
    let result = tokio::time::timeout(Duration::from_secs(2), sub.updates().recv()).await.unwrap();
    assert!(result.is_none(), "expected the updates channel to close after cancellation");
}
