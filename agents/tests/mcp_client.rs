//! Just enough Streamable HTTP client to hold a real MCP conversation.
//!
//! rmcp ships a client, but it would exercise rmcp against itself. Speaking the wire format
//! directly is what proves the runtime mounted something a *foreign* client can talk to.

#![allow(dead_code)]

use futures::StreamExt;
use serde_json::{Value, json};

/// One MCP session: the endpoint, the negotiated session id, and a request counter.
pub struct McpClient {
    http: reqwest::Client,
    endpoint: String,
    session: Option<String>,
    next_id: u64,
    headers: Vec<(String, String)>,
}

impl McpClient {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            http: reqwest::Client::new(),
            endpoint: endpoint.into(),
            session: None,
            next_id: 0,
            headers: Vec::new(),
        }
    }

    /// Sends `name: value` on every request from now on.
    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }

    /// The session id the server assigned, if it assigned one.
    pub fn session(&self) -> Option<&str> {
        self.session.as_deref()
    }

    /// Performs the handshake every MCP client opens with, and keeps the session id the
    /// server assigns — without it, every later call is refused.
    pub async fn initialize(&mut self) -> Value {
        self.initialize_with(json!({})).await
    }

    /// [`McpClient::initialize`] declaring `capabilities`.
    pub async fn initialize_with(&mut self, capabilities: Value) -> Value {
        let response = self
            .send(json!({
                "jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": capabilities,
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

    /// Sends a request and answers every elicitation the server sends while it runs with
    /// `answer`, returning the request's own response.
    pub async fn call_answering(&mut self, method: &str, params: Value, answer: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id + 1;
        let response = self
            .request(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
            .send()
            .await
            .expect("mcp request");
        let mut body = response.bytes_stream();
        let mut buffer = String::new();
        while let Some(chunk) = body.next().await {
            buffer.push_str(&String::from_utf8_lossy(&chunk.expect("chunk")));
            while let Some(end) = buffer.find("\n\n") {
                let event: String = buffer.drain(..end + 2).collect();
                for line in event.lines() {
                    let Some(data) = line.strip_prefix("data: ") else {
                        continue;
                    };
                    let Ok(message) = serde_json::from_str::<Value>(data) else {
                        continue;
                    };
                    if message["method"] == "elicitation/create" {
                        let reply = json!({
                            "jsonrpc": "2.0", "id": message["id"], "result": answer,
                        });
                        let _ = self.send(reply).await;
                    } else if message["id"] == json!(id) {
                        return message;
                    }
                }
            }
        }
        Value::Null
    }

    /// Sends a notification, which by definition has no id and no response.
    async fn notify(&mut self, method: &str) {
        let _ = self.send(json!({"jsonrpc": "2.0", "method": method})).await;
    }

    fn request(&self, body: Value) -> reqwest::RequestBuilder {
        let mut request = self
            .http
            .post(&self.endpoint)
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream");
        if let Some(session) = &self.session {
            request = request.header("mcp-session-id", session.clone());
        }
        for (name, value) in &self.headers {
            request = request.header(name, value);
        }
        request.json(&body)
    }

    pub async fn send(&mut self, body: Value) -> (reqwest::StatusCode, Value) {
        let response = self.request(body).send().await.expect("mcp request");
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
pub fn parse(text: &str) -> Value {
    let payload = text
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .find(|payload| !payload.trim().is_empty())
        .unwrap_or(text);
    serde_json::from_str(payload).unwrap_or(Value::Null)
}

/// The text of a tool result's first content block, parsed as JSON when it is JSON.
pub fn tool_text(response: &Value) -> Value {
    let text = response["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_default();
    serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.to_string()))
}
