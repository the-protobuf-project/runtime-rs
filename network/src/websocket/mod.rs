mod io;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex as AsyncMutex, RwLock as AsyncRwLock};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite};
use tokio_util::sync::CancellationToken;

pub use tokio_tungstenite::tungstenite::Message;

use crate::error::{Error, Result};
use crate::options::{ConnectionOptions, DEFAULT_TIMEOUT};
use crate::url::build_full_url;

type WsSink = futures_util::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;
type WsRead = futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

const DEFAULT_RECONNECT_DELAY: Duration = Duration::from_secs(5);
const PING_INTERVAL: Duration = Duration::from_secs(30);

struct State {
    options: ConnectionOptions,
    path_index: i64,
    cancel: CancellationToken,
    connected: bool,
    auto_reconnect: bool,
    reconnect_delay: Duration,
}

struct Inner {
    sink: AsyncMutex<Option<WsSink>>,
    stream: AsyncMutex<Option<WsRead>>,
    state: AsyncRwLock<State>,
}

/// A WebSocket connection client. Create with [`Default`] and configure with
/// [`WebSocketClient::connect`]. A cheap `Arc` handle to shared state — cloning it shares the
/// same connection, and all methods are safe for concurrent use, including across clones.
#[derive(Clone)]
pub struct WebSocketClient(Arc<Inner>);

impl Default for WebSocketClient {
    fn default() -> Self {
        Self(Arc::new(Inner {
            sink: AsyncMutex::new(None),
            stream: AsyncMutex::new(None),
            state: AsyncRwLock::new(State {
                options: ConnectionOptions::default(),
                path_index: 0,
                cancel: CancellationToken::new(),
                connected: false,
                auto_reconnect: false,
                reconnect_delay: DEFAULT_RECONNECT_DELAY,
            }),
        }))
    }
}

impl WebSocketClient {
    /// Establishes the WebSocket connection to the URL derived from `opts` and the client's path
    /// index. If `opts.timeout` is zero, [`DEFAULT_TIMEOUT`] is used for the handshake. On success
    /// a ping/pong keepalive task is started. Returns an error if the handshake fails.
    pub async fn connect(&self, mut opts: ConnectionOptions) -> Result<()> {
        if opts.timeout.is_zero() {
            opts.timeout = DEFAULT_TIMEOUT;
        }

        let path_index = self.0.state.read().await.path_index;
        let full_url =
            build_full_url(&opts.url, path_index).map_err(|e| Error::BuildUrl(Box::new(e)))?;
        let request = build_ws_request(&full_url, &opts.url.host, &opts.headers)?;

        let dial = tokio_tungstenite::connect_async(request);
        let (ws_stream, _resp) = match tokio::time::timeout(opts.timeout, dial).await {
            Ok(Ok(pair)) => pair,
            Ok(Err(err)) => {
                return Err(Error::WsConnect {
                    host: opts.url.host.clone(),
                    source: Box::new(err),
                });
            }
            Err(_) => {
                let timeout_err = tungstenite::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "WebSocket handshake timed out",
                ));
                return Err(Error::WsConnect {
                    host: opts.url.host.clone(),
                    source: Box::new(timeout_err),
                });
            }
        };

        let (sink, stream) = ws_stream.split();
        let cancel = CancellationToken::new();

        {
            let mut state = self.0.state.write().await;
            state.options = opts;
            state.cancel = cancel.clone();
            state.connected = true;
        }
        *self.0.sink.lock().await = Some(sink);
        *self.0.stream.lock().await = Some(stream);

        self.start_ping_pong(cancel);
        Ok(())
    }

    /// Sends a close frame, closes the connection, and cancels the connection's cancellation
    /// token (stopping the ping/pong task and unblocking any in-flight `receive`/`listen`). Safe
    /// to call repeatedly.
    pub async fn close(&self) -> Result<()> {
        let cancel = {
            let mut state = self.0.state.write().await;
            state.connected = false;
            state.cancel.clone()
        };
        cancel.cancel();

        if let Some(mut sink) = self.0.sink.lock().await.take() {
            let _ = sink.send(Message::Close(None)).await;
            let _ = sink.close().await;
        }
        *self.0.stream.lock().await = None;
        Ok(())
    }

    /// Closes the current connection (if any) and reconnects with the same [`ConnectionOptions`].
    pub async fn reconnect(&self) -> Result<()> {
        let (was_connected, opts) = {
            let state = self.0.state.read().await;
            (state.connected, state.options.clone())
        };
        if was_connected {
            self.close().await?;
        }
        self.connect(opts).await
    }

    /// Enables or disables automatic reconnection in [`WebSocketClient::listen`]. When enabled, a
    /// read error triggers a sleep for `delay` (or the default 5s if `delay` is `None` or zero),
    /// then a reconnect attempt; on success, listening continues.
    pub async fn set_auto_reconnect(&self, enabled: bool, delay: Option<Duration>) {
        let mut state = self.0.state.write().await;
        state.auto_reconnect = enabled;
        if let Some(d) = delay
            && !d.is_zero()
        {
            state.reconnect_delay = d;
        }
    }

    /// Sends a ping every 30 seconds. Exits when `cancel` fires (e.g. on close) or the connection
    /// is gone.
    fn start_ping_pong(&self, cancel: CancellationToken) {
        let client = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(PING_INTERVAL);
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let mut guard = client.0.sink.lock().await;
                        let Some(sink) = guard.as_mut() else { return };
                        if sink.send(Message::Ping(Vec::new().into())).await.is_err() {
                            return;
                        }
                    }
                    _ = cancel.cancelled() => return,
                }
            }
        });
    }
}

fn build_ws_request(
    full_url: &str,
    host: &str,
    headers: &HashMap<String, String>,
) -> Result<http::Request<()>> {
    let mut request = full_url
        .into_client_request()
        .map_err(|e| Error::WsConnect {
            host: host.to_string(),
            source: Box::new(e),
        })?;
    for (k, v) in headers {
        let name = http::HeaderName::from_bytes(k.as_bytes())
            .map_err(|_| Error::InvalidHeader(k.clone()))?;
        let value = http::HeaderValue::from_str(v).map_err(|_| Error::InvalidHeader(v.clone()))?;
        request.headers_mut().insert(name, value);
    }
    Ok(request)
}
