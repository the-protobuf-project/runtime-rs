//! The background task a live [`super::Subscription`] runs on, plus the small message-framing
//! helpers it shares with the handshake in `subscription.rs`. Split out purely to keep
//! `subscription.rs` focused on the public API surface.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::de::DeserializeOwned;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use super::ws_protocol::{ClientMessage, ServerMessage};
use crate::error::{Error, Result};

pub(super) type WsSink = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    Message,
>;
pub(super) type WsStream = futures_util::stream::SplitStream<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
>;

/// Serializes `msg` as a graphql-transport-ws text frame and sends it.
pub(super) async fn send_json(sink: &mut WsSink, msg: &ClientMessage<'_>) -> Result<()> {
    let text = serde_json::to_string(msg)?;
    sink.send(Message::Text(text.into()))
        .await
        .map_err(|e| Error::WsSend(Box::new(e)))
}

/// Reads frames until a `connection_ack` arrives (or `timeout` elapses), discarding anything
/// else received in between.
pub(super) async fn await_connection_ack(stream: &mut WsStream, timeout: Duration) -> Result<()> {
    let wait = async {
        loop {
            match stream.next().await {
                Some(Ok(Message::Text(txt))) => match serde_json::from_str::<ServerMessage>(&txt) {
                    Ok(ServerMessage::ConnectionAck { .. }) => return Ok(()),
                    Ok(_) => continue,
                    Err(e) => return Err(Error::from(e)),
                },
                Some(Ok(_)) => continue,
                Some(Err(e)) => return Err(Error::WsReceive(Box::new(e))),
                None => return Err(Error::WsClosed),
            }
        }
    };
    tokio::time::timeout(timeout, wait)
        .await
        .map_err(|_| Error::Timeout)?
}

/// Drives one subscription after the handshake completes: forwards each `next` payload (decoded
/// into `T`) to `tx`, answers protocol `ping`s with `pong`s, and stops on `complete`, a fatal
/// error, or `cancel` firing (sending `complete` and closing the socket in that case).
pub(super) async fn run_subscription<T: DeserializeOwned + Send + 'static>(
    mut sink: WsSink,
    mut stream: WsStream,
    sub_id: String,
    tx: mpsc::Sender<Result<T>>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                let _ = send_json(&mut sink, &ClientMessage::Complete { id: sub_id.clone() }).await;
                let _ = sink.close().await;
                return;
            }
            msg = stream.next() => {
                match msg {
                    Some(Ok(Message::Text(txt))) => {
                        match serde_json::from_str::<ServerMessage>(&txt) {
                            Ok(ServerMessage::Next { payload, .. }) => {
                                let data = payload.get("data").cloned().unwrap_or(serde_json::Value::Null);
                                let decoded = serde_json::from_value::<T>(data).map_err(Error::SubscriptionDecode);
                                if tx.send(decoded).await.is_err() {
                                    return;
                                }
                            }
                            Ok(ServerMessage::Error { payload, .. }) => {
                                if tx.send(Err(Error::GraphQLErrors(payload))).await.is_err() {
                                    return;
                                }
                            }
                            Ok(ServerMessage::Complete { .. }) => return,
                            Ok(ServerMessage::Ping { payload }) => {
                                let _ = send_json(&mut sink, &ClientMessage::Pong { payload }).await;
                            }
                            Ok(ServerMessage::Pong { .. }) | Ok(ServerMessage::ConnectionAck { .. }) => {}
                            Err(e) => {
                                if tx.send(Err(Error::SubscriptionDecode(e))).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        let _ = tx.send(Err(Error::SubscriptionStopped(Box::new(Error::WsClosed)))).await;
                        return;
                    }
                    Some(Ok(_)) => continue,
                    Some(Err(e)) => {
                        let _ = tx.send(Err(Error::SubscriptionStopped(Box::new(Error::WsReceive(Box::new(e)))))).await;
                        return;
                    }
                }
            }
        }
    }
}
