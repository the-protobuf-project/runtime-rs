# HTTP client

[Network guide](../../README.md)

Construct `HttpClient`, connect it with `ConnectionOptions`, and send a request
against a `UrlOptions` path. `request` returns body/error together in
`HttpResponse`; `request_sync` is also async and unpacks this into `Result`.

```rust
use std::collections::HashMap;
use network::{ConnectionOptions, HttpClient, HttpMethod, UrlOptions, UrlScheme};

#[tokio::main]
async fn main() -> network::Result<()> {
    let url = UrlOptions {
        scheme: UrlScheme::Http, host: "127.0.0.1:8080".into(),
        paths: vec!["/health".into()], ..Default::default()
    };
    let mut client = HttpClient::default();
    client.connect(ConnectionOptions {
        url: url.clone(), skip_connectivity_check: true, ..Default::default()
    }).await?;
    let body = client.request_sync(HttpMethod::Get, &url, Vec::new(),
        &HashMap::new(), 0, 0, None).await?;
    println!("{}", String::from_utf8_lossy(&body));
    client.close().await?;
    Ok(())
}
```

This expects an HTTP service on port 8080 with `/health`. The path index is 0,
and the last `0` selects zero retries. Supply a `CancellationToken` reference
as the final argument to cancel an attempt or retry wait.

`mod.rs` handles connect/probe/close/reconnect and verb conversion. The default
connectivity probe tries HEAD, falling back to GET for unsupported HEAD.
`request.rs` constructs each URL/request, applies supplied headers and optional
trace propagation, reads bytes, validates status, and packages errors.

A call makes one initial attempt plus `max_retries`; every attempt has a fresh
timeout. Failed attempts, including status failures, enter the retry loop.
Retry delay defaults to two seconds. Choose retries deliberately for writes:
the client does not infer whether a partially completed write is safe to replay.
Only 2xx succeeds; error responses expose the error rather than response bytes.

```bash
cargo test -p tpp-network --test http
cargo run -p tpp-network --example httpbin_http
```
