use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use network::{ConnectionOptions, Message, UrlOptions, UrlScheme, WebSocketClient};
use tokio::net::TcpListener;

/// Starts a WebSocket echo server on an ephemeral port. If `drop_first` is set, the first
/// accepted connection is closed immediately after the handshake (no echo) to simulate an
/// abrupt disconnect for auto-reconnect tests; every later connection echoes text messages.
async fn start_echo_server(drop_first: bool) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut first = true;
        loop {
            let Ok((stream, _)) = listener.accept().await else { return };
            let should_drop = drop_first && first;
            first = false;
            tokio::spawn(async move {
                let Ok(ws) = tokio_tungstenite::accept_async(stream).await else { return };
                if should_drop {
                    drop(ws);
                    return;
                }
                let (mut sink, mut stream) = ws.split();
                while let Some(Ok(msg)) = stream.next().await {
                    if msg.is_close() {
                        break;
                    }
                    if sink.send(msg).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    addr
}

fn ws_opts(addr: SocketAddr) -> ConnectionOptions {
    ConnectionOptions {
        url: UrlOptions {
            scheme: UrlScheme::Ws,
            host: addr.to_string(),
            paths: vec!["/".to_string()],
            params: HashMap::new(),
        },
        timeout: Duration::from_secs(2),
        ..Default::default()
    }
}

#[tokio::test]
async fn connect_send_receive_round_trip() {
    let addr = start_echo_server(false).await;
    let client = WebSocketClient::default();
    client.connect(ws_opts(addr)).await.unwrap();

    client.send(Message::Text("hello".into())).await.unwrap();
    let msg = client.receive().await.unwrap();
    assert_eq!(msg.into_text().unwrap(), "hello");

    client.close().await.unwrap();
}

#[tokio::test]
async fn close_frees_the_connection() {
    let addr = start_echo_server(false).await;
    let client = WebSocketClient::default();
    client.connect(ws_opts(addr)).await.unwrap();
    client.close().await.unwrap();

    let err = client.send(Message::Text("hi".into())).await.unwrap_err();
    assert!(matches!(err, network::Error::WsNotConnected));
}

#[tokio::test]
async fn auto_reconnect_after_read_error() {
    let addr = start_echo_server(true).await;
    let client = WebSocketClient::default();
    client.connect(ws_opts(addr)).await.unwrap();
    client.set_auto_reconnect(true, Some(Duration::from_millis(50))).await;

    let mut rx = client.listen(None);

    let sender = client.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(400)).await;
        let _ = sender.send(Message::Text("ping".into())).await;
    });

    let msg = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("listen did not yield in time")
        .expect("channel closed unexpectedly")
        .expect("expected a message, got an error");
    assert_eq!(msg.into_text().unwrap(), "ping");
}

#[tokio::test]
async fn listen_terminates_without_auto_reconnect() {
    let addr = start_echo_server(true).await;
    let client = WebSocketClient::default();
    client.connect(ws_opts(addr)).await.unwrap();

    let mut rx = client.listen(None);
    let result = tokio::time::timeout(Duration::from_secs(2), rx.recv()).await.unwrap();
    assert!(matches!(result, Some(Err(_))));
    assert!(tokio::time::timeout(Duration::from_secs(1), rx.recv()).await.unwrap().is_none());
}

#[tokio::test]
async fn cancellation_stops_listen() {
    let addr = start_echo_server(false).await;
    let client = WebSocketClient::default();
    client.connect(ws_opts(addr)).await.unwrap();

    let cancel = tokio_util::sync::CancellationToken::new();
    let mut rx = client.listen(Some(cancel.clone()));
    cancel.cancel();

    let result = tokio::time::timeout(Duration::from_secs(1), rx.recv()).await.unwrap();
    assert!(matches!(result, Some(Err(network::Error::Cancelled))));
}
