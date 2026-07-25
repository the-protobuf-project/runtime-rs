//! End-to-end HTTP example against httpbingo.org (<https://httpbingo.org>, no auth required), a
//! Go-based httpbin-compatible clone used here in place of the Go examples' httpbin.org, which
//! was returning 503s at the time this was written. Run with:
//!
//! ```text
//! cargo run -p network --example httpbin_http
//! ```

use std::collections::HashMap;
use std::time::Duration;

use network::{ClientType, ConnectionOptions, HttpMethod, Network, UrlOptions, UrlScheme};

fn base_opts(path: &str, params: HashMap<String, String>) -> UrlOptions {
    UrlOptions {
        scheme: UrlScheme::Https,
        host: "httpbingo.org".to_string(),
        paths: vec![path.to_string()],
        params,
    }
}

/// httpbingo.org's Fly.io hosting rejects requests with no `User-Agent` header (a bot-protection
/// heuristic) with `402 Payment Required`. Go's `net/http` sends a default User-Agent
/// automatically; `reqwest` does not, so — unlike Go's `Request` calls, which can pass `nil`
/// headers here — this example must set one explicitly.
fn headers_with_ua(extra: &[(&str, &str)]) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    headers.insert(
        "User-Agent".to_string(),
        "runtime-rs-example/0.1".to_string(),
    );
    for (k, v) in extra {
        headers.insert(k.to_string(), v.to_string());
    }
    headers
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== HTTP Client Examples ===");
    println!("Using httpbingo.org\n");

    let mut conn = Network::new_connection(ClientType::Http)?;
    conn.with_opts(ConnectionOptions {
        url: base_opts("/get", HashMap::new()),
        timeout: Duration::from_secs(10),
        ..Default::default()
    })
    .await?;
    println!("Connected (HEAD/GET connectivity check passed)\n");
    let http = conn.as_http()?;

    // 1. Simple GET request.
    println!("1. Simple GET Request");
    let url = base_opts("/get", HashMap::new());
    let body = http
        .request(
            HttpMethod::Get,
            &url,
            Vec::new(),
            &headers_with_ua(&[]),
            0,
            0,
            None,
        )
        .await?;
    println!("   Success! Response length: {} bytes", body.len());
    println!(
        "   Preview: {}\n",
        &String::from_utf8_lossy(&body)[..120.min(body.len())]
    );

    // 2. GET with query parameters.
    println!("2. GET with Query Parameters");
    let mut params = HashMap::new();
    params.insert("hello".to_string(), "world".to_string());
    params.insert("lang".to_string(), "rust".to_string());
    let url = base_opts("/get", params);
    let body = http
        .request(
            HttpMethod::Get,
            &url,
            Vec::new(),
            &headers_with_ua(&[]),
            0,
            0,
            None,
        )
        .await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;
    println!("   Success! Echoed args: {}\n", json["args"]);

    // 3. POST with a JSON body.
    println!("3. POST with JSON Body");
    let url = base_opts("/post", HashMap::new());
    let payload = serde_json::json!({"message": "hello from runtime-rs", "n": 42})
        .to_string()
        .into_bytes();
    let headers = headers_with_ua(&[("Content-Type", "application/json")]);
    let body = http
        .request(HttpMethod::Post, &url, payload, &headers, 0, 0, None)
        .await?;
    let json: serde_json::Value = serde_json::from_slice(&body)?;
    println!("   Success! Server saw JSON: {}\n", json["json"]);

    // 4. Automatic retries against a flaky endpoint (httpbin's /status/500 always fails, so this
    //    demonstrates retry-exhaustion wrapping the last error).
    println!("4. Automatic Retries (against an endpoint that always 500s)");
    let url = base_opts("/status/500", HashMap::new());
    match http
        .request(
            HttpMethod::Get,
            &url,
            Vec::new(),
            &headers_with_ua(&[]),
            0,
            2,
            None,
        )
        .await
    {
        Ok(_) => println!("   Unexpected success"),
        Err(e) => println!("   Got expected error after retries: {e}\n"),
    }

    // 5. Cancellation.
    println!("5. Cancellation (aborts a slow /delay/5 request after 300ms)");
    let url = base_opts("/delay/5", HashMap::new());
    let cancel = tokio_util::sync::CancellationToken::new();
    let cancel_clone = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        cancel_clone.cancel();
    });
    let start = std::time::Instant::now();
    match http
        .request(
            HttpMethod::Get,
            &url,
            Vec::new(),
            &headers_with_ua(&[]),
            0,
            0,
            Some(&cancel),
        )
        .await
    {
        Ok(_) => println!("   Unexpected success"),
        Err(e) => println!("   Cancelled after {:?}: {e}\n", start.elapsed()),
    }

    println!("All HTTP examples completed successfully.");
    Ok(())
}
