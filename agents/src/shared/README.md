# Header forwarding and per-call context

[Agent source map](../README.md)

MCP and A2A HTTP requests can call gRPC services in process. These helpers carry
the original caller's headers through that hop so downstream interceptors see
the intended credentials and metadata.

`mod.rs` defines `HeaderMapping`, `ForwardedHeaders`, and the axum forwarding
layer. The layer captures configured headers from each HTTP request. In
`incoming.rs`, `incoming_headers` replaces mapped gRPC keys, clearing values
left by earlier calls even when the current request has no such header.
Transport-owned `grpc-` keys are excluded.

Hosting runs the handler future inside `with_call_context(CallContext, future)`.
`forward_metadata` combines incoming metadata with forwarded HTTP headers;
HTTP values win collisions. The hybrid server's `Loopback` calls
`apply_call_context` to copy that result onto the outgoing request before the
gRPC interceptor chain runs.

The context is Tokio task-local and scoped to the future. It does not follow
`tokio::spawn`; explicitly capture and scope it again when moving work to
another task. Outside a call, applying context changes nothing. Forwarding
transports identity data; application interceptors still decide authorization.

Verify with `cargo test -p tpp-agents --test mcp_parity` and
`cargo test -p tpp-grpc --test mcp` from the workspace root. These cover
replacement rules and the complete HTTP-to-loopback interceptor path.

## Using scoped forwarding

```rust
use agents::shared::{CallContext, HeaderMapping, incoming_headers, with_call_context};
use tonic::metadata::MetadataMap;

async fn scoped_call() {
    let mut headers = http::HeaderMap::new();
    headers.insert("x-request-id", "request-42".parse().unwrap());
    let incoming = incoming_headers(&MetadataMap::new(),
        &[HeaderMapping::new("x-request-id", "x-request-id")], Some(&headers));
    with_call_context(CallContext { incoming, ..Default::default() }, async {
        let context = agents::shared::current_call_context().unwrap();
        assert_eq!(context.incoming.get("x-request-id").unwrap(), "request-42");
    }).await;
}
```

Normal protocol hosting scopes this automatically; use the helper directly
when adapting a custom entrypoint or moving call work to a different task.
