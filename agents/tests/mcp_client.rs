//! Just enough Streamable HTTP client to hold a real MCP conversation.
//!
//! rmcp ships a client, but it would exercise rmcp against itself. Speaking the wire format
//! directly is what proves the runtime mounted something a *foreign* client can talk to.

#![allow(dead_code)]

use serde_json::{Value, json};

/// One MCP session: the endpoint, the negotiated session id, and a request counter.
pub struct McpClient {
    http: reqwest::Client,
    endpoint: String,
    session: Option<String>,
    next_id: u64,
}

impl McpClient {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            http: reqwest::Client::new(),
            endpoint: endpoint.into(),
            session: None,
            next_id: 0,
        }
    }

    /// Performs the handshake every MCP client opens with, and keeps the session id the
    /// server assigns — without it, every later call is refused.
    pub async fn initialize(&mut self) -> Value {
        let response = self
            .send(json!({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "runtime-rs-test", "version": "1.0.0"}
                }
            }))
            .await;

        // The server is only ready for requests once it has been told the handshake landed.
        self.notify("notifications/initialized").await;
        response.1
    }

    /// Sends a request and returns its parsed response.
    pub async fn call(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id + 1;
        self.send(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
            .await
            .1
    }

    /// Sends a notification, which by definition has no id and no response.
    async fn notify(&mut self, method: &str) {
        let _ = self.send(json!({"jsonrpc": "2.0", "method": method})).await;
    }

    async fn send(&mut self, body: Value) -> (reqwest::StatusCode, Value) {
        let mut request = self
            .http
            .post(&self.endpoint)
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream");
        if let Some(session) = &self.session {
            request = request.header("mcp-session-id", session.clone());
        }

        let response = request.json(&body).send().await.expect("mcp request");
        let status = response.status();

        if self.session.is_none()
            && let Some(assigned) = response.headers().get("mcp-session-id")
            && let Ok(assigned) = assigned.to_str()
        {
            self.session = Some(assigned.to_string());
        }

        let text = response.text().await.unwrap_or_default();
        (status, parse(&text))
    }
}

/// Unwraps whichever framing the transport chose. Streamable HTTP answers either as plain
/// JSON or as SSE, and the SSE stream opens with an empty priming frame carrying the retry
/// interval — so the first `data:` line is not the answer, the first non-empty one is.
fn parse(text: &str) -> Value {
    let payload = text
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .find(|payload| !payload.trim().is_empty())
        .unwrap_or(text);
    serde_json::from_str(payload).unwrap_or(Value::Null)
}
