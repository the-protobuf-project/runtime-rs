use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{Message, WebSocketClient};
use crate::error::{Error, Result};

const RETRY_SEND_DELAY: Duration = Duration::from_secs(2);

impl WebSocketClient {
    /// Writes a single WebSocket frame. Returns an error if the connection is closed or the
    /// write fails.
    pub async fn send(&self, message: Message) -> Result<()> {
        let mut guard = self.0.sink.lock().await;
        let sink = guard.as_mut().ok_or(Error::WsNotConnected)?;
        sink.send(message).await.map_err(|e| Error::WsSend(Box::new(e)))
    }

    /// Reads the next WebSocket message. Blocks until a message is available, the connection is
    /// closed, or the read fails. Ping frames are answered with a matching Pong and not surfaced;
    /// Pong frames are swallowed; a Close frame surfaces as [`Error::WsClosed`].
    pub async fn receive(&self) -> Result<Message> {
        loop {
            let cancel = self.0.state.read().await.cancel.clone();
            let mut guard = self.0.stream.lock().await;
            let stream = guard.as_mut().ok_or(Error::WsNotConnected)?;

            let next = tokio::select! {
                item = stream.next() => item,
                _ = cancel.cancelled() => return Err(Error::WsClosed),
            };
            drop(guard);

            match next {
                Some(Ok(Message::Ping(payload))) => {
                    let mut sink_guard = self.0.sink.lock().await;
                    if let Some(sink) = sink_guard.as_mut() {
                        let _ = sink.send(Message::Pong(payload)).await;
                    }
                    continue;
                }
                Some(Ok(Message::Pong(_))) => continue,
                Some(Ok(Message::Close(_))) | None => return Err(Error::WsClosed),
                Some(Ok(msg)) => return Ok(msg),
                Some(Err(e)) => return Err(Error::WsReceive(Box::new(e))),
            }
        }
    }

    /// Sends a message with up to `max_retries` attempts, sleeping 2 seconds between attempts.
    /// Returns `Ok` on first success, or an error after all retries fail.
    pub async fn retry_send(&self, message: Message, max_retries: usize) -> Result<()> {
        let mut last_err = Error::WsNotConnected;
        for _ in 0..max_retries {
            match self.send(message.clone()).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    last_err = e;
                    tokio::time::sleep(RETRY_SEND_DELAY).await;
                }
            }
        }
        Err(Error::WsRetryExhausted { retries: max_retries, source: Box::new(last_err) })
    }

    /// Reads messages in a loop and forwards each on the returned channel. The channel receives
    /// one terminal error when the loop stops (external cancellation, connection closed, or a
    /// read error with auto-reconnect disabled or failing), then closes. If auto-reconnect is
    /// enabled (see [`WebSocketClient::set_auto_reconnect`]), a read error instead sleeps for the
    /// reconnect delay, reconnects, and continues listening on success.
    pub fn listen(&self, cancel: Option<CancellationToken>) -> mpsc::Receiver<Result<Message>> {
        let (tx, rx) = mpsc::channel(32);
        let client = self.clone();
        tokio::spawn(async move {
            loop {
                let received = match &cancel {
                    Some(external) => {
                        tokio::select! {
                            r = client.receive() => r,
                            _ = external.cancelled() => {
                                let _ = tx.send(Err(Error::Cancelled)).await;
                                return;
                            }
                        }
                    }
                    None => client.receive().await,
                };

                match received {
                    Ok(msg) => {
                        if tx.send(Ok(msg)).await.is_err() {
                            return;
                        }
                    }
                    Err(err) => {
                        let auto_reconnect = client.0.state.read().await.auto_reconnect;
                        if auto_reconnect {
                            let delay = client.0.state.read().await.reconnect_delay;
                            tokio::time::sleep(delay).await;
                            if let Err(e) = client.reconnect().await {
                                let _ = tx.send(Err(Error::WsReconnect(Box::new(e)))).await;
                                return;
                            }
                            continue;
                        }
                        let _ = tx.send(Err(err)).await;
                        return;
                    }
                }
            }
        });
        rx
    }
}
