# HTTP surface and listeners

[Hybrid server](../../README.md)

`Surface` owns a router composition seam. It does not generate REST endpoints
or transcode protobuf; a generated gateway or application axum router supplies
the routes. The same completed router can be served by multiple transports.

```rust
use axum::{Router, routing::get};
use grpc::{HybridServer, options::{Endpoint, Options}};

#[tokio::main]
async fn main() -> grpc::Result<()> {
    let options = Options {
        service_name: "shop".into(),
        grpc: Endpoint::new("127.0.0.1", 50051),
        http: Endpoint::new("127.0.0.1", 8080),
        enable_http: true,
        ..Default::default()
    };
    let routes = Router::new().route("/hello", get(|| async { "hello" }));
    HybridServer::new(options).with_http_handler(routes).serve().await
}
```

With this program running, `curl http://127.0.0.1:8080/hello` returns `hello`,
and `GET /health` is mounted alongside it. `health.rs` provides that HTTP
liveness route independently of the standard gRPC health-service toggle.

`h1.rs` binds TCP, optionally accepts TLS, and serves requests through hyper.
Ordinary gRPC remains on its HTTP/2 tonic listener; the HTTP/1.1 leg serves
REST/JSON. `h3.rs` adapts QUIC HTTP/3 requests to the same router. It is compiled
with the `http3` feature and requires TLS. Hybrid startup binds it at
`0.0.0.0:(http.port + 1)` and advertises Alt-Svc on the HTTP/1.1 responses.

Bind failures are returned before startup reports success. Cancellation stops
acceptance and drains serving tasks. CORS is applied to the completed router,
including health and application routes, before transport cloning.

```bash
cargo test -p tpp-grpc --test http_surface --test cors --test gateway
```

These local tests cover routing, conflicts, cancellation, CORS, and loopback
gateway dispatch; they do not establish interoperability with every QUIC client.
