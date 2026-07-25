//! End-to-end WebSocket example against a public echo service
//! (<wss://echo.websocket.org>, no auth required) — the same service the Go package's examples
//! use. Run with:
//!
//! ```text
//! cargo run -p network --example websocket_echo
//! ```

use std::collections::HashMap;
use std::time::Duration;

use network::{ClientType, ConnectionOptions, Message, Network, UrlOptions, UrlScheme};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== WebSocket Client Examples ===");
    println!("Using wss://echo.websocket.org\n");

    let mut conn = Network::new_connection(ClientType::WebSocket)?;
    conn.with_opts(ConnectionOptions {
        url: UrlOptions {
            scheme: UrlScheme::Wss,
            host: "echo.websocket.org".to_string(),
            paths: vec!["/".to_string()],
            params: HashMap::new(),
        },
        timeout: Duration::from_secs(10),
        ..Default::default()
    })
    .await?;
    println!("Connected!\n");
    let ws = conn.as_websocket()?;

    // The service greets new connections with a banner message before echoing; drain it first.
    println!("1. Simple Echo");
    if let Ok(Ok(msg)) = tokio::time::timeout(Duration::from_secs(3), ws.receive()).await {
        println!("   Greeting: {}", msg.into_text().unwrap_or_default());
    }

    println!("   Sending: Hello, WebSocket!");
    ws.send(Message::Text("Hello, WebSocket!".into())).await?;
    let echoed = ws.receive().await?;
    println!("   Received: {}", echoed.into_text()?);
    println!("   Echo successful!\n");

    // Auto-reconnect + listen loop, matching the Go example's SetAutoReconnect/Listen usage.
    println!("2. Listen Loop with a Few Messages");
    ws.set_auto_reconnect(true, Some(Duration::from_secs(5)))
        .await;
    let mut updates = ws.listen(None);

    let sender = ws.clone();
    tokio::spawn(async move {
        for i in 1..=3 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let _ = sender
                .send(Message::Text(format!("message #{i}").into()))
                .await;
        }
    });

    let mut received = 0;
    while received < 3 {
        match tokio::time::timeout(Duration::from_secs(5), updates.recv()).await {
            Ok(Some(Ok(msg))) => {
                println!("   Received: {}", msg.into_text().unwrap_or_default());
                received += 1;
            }
            Ok(Some(Err(e))) => {
                println!("   Connection error: {e}");
                break;
            }
            _ => {
                println!("   Timed out waiting for a message");
                break;
            }
        }
    }

    conn.as_websocket_mut()?.close().await?;
    println!("\nAll WebSocket examples completed successfully.");
    Ok(())
}
