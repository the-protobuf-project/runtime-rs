# MCP hosting and generated-handler support

[Agent runtime](../../README.md) · [Source map](../README.md)

This module wraps `rmcp`. Generated or handwritten `ServerHandler` values own
tools and dispatch; this layer adds runtime placement, transport lifecycle,
shared policies, and helpers that generated MCP code calls.

## From handler to a tool response

1. Build a `Service` from a per-session factory (`new` / `try_new`) or a
   cloneable handler (`from_handler`). Set its base path, transports, header
   mappings, extra properties, and interceptor chain before registration.
2. `serve.rs` implements runtime mounting. Streamable HTTP mounts at the base
   path; legacy SSE mounts at the base path plus `/`. Stdio runs over process
   pipes. The default service uses streamable HTTP at `/mcp`.
3. Each session gets a handler wrapped by `Hosted`. List responses expose
   configured schema extras and cache hints; reads can use resource overrides.
4. A tool call builds call-scoped header/metadata context, extracts extras from
   arguments, maps the tool name to its RPC method where configured, and runs
   tool interceptors around the inner handler. Generated code uses `invoke_unary`
   and result/error helpers rather than knowing where the server is hosted.
5. A streaming RPC can feed `InProcessServerStream`; progress helpers turn
   chunks/tokens into notifications. Unary results can carry both readable text
   and structured JSON. gRPC failures retain code and rich details in the MCP
   result payload.
6. Cancellation ends session work and long-lived HTTP streams so the owning
   runtime can drain its listener.

## Multiple services and input requests

A `ServiceGroup` normally keeps each member's path. `combined()` inventories
handlers through in-memory MCP sessions, rejects duplicate tool/prompt/resource
and template identities, then exposes one server and routes each call to its
owner. Group placement settings override member settings; group interceptors
wrap member interceptors. `BoxedHandler` erases differing handler types while
preserving `ServerHandler` compatibility.

`elicitation.rs` supports input-required results and ordinary inline requests.
For older clients, `Hosted` performs an ordinary server-to-client elicitation
round trip and invokes the handler again. `ask.rs` encodes service-raised
questions in gRPC `ErrorInfo` and merges answered fields on retry. Configure a
resource handler when a generated placeholder needs real application content.

Stateless streamable HTTP omits sessions, standalone GET streams, DELETE, and
server-to-client requests. Choose it only when that reduced conversation fits
the client; it permits independent requests to reach different replicas.

## File map

| Files | Role |
| --- | --- |
| [mod.rs](mod.rs), [sdk.rs](sdk.rs) | Public runtime and SDK vocabulary, including `rmcp` re-exports |
| [service.rs](service.rs), [server.rs](server.rs), [serve.rs](serve.rs), [transport.rs](transport.rs) | Registrable services, standalone configuration, placement, and transport choice |
| [hosted.rs](hosted.rs), [handler.rs](handler.rs), [combine.rs](combine.rs) | Hosting policies, type erasure, inventory, and combined dispatch |
| [config.rs](config.rs), [extras.rs](extras.rs), [cache.rs](cache.rs) | Handler settings, extra schema fields, and result freshness hints |
| [interceptor.rs](interceptor.rs), [invoke.rs](invoke.rs), [metadata.rs](metadata.rs) | Middleware invocation and progress-token metadata |
| [tool.rs](tool.rs), [error.rs](error.rs), [resources.rs](resources.rs) | Tool schemas/results, rich errors, resource/prompt declarations |
| [elicitation.rs](elicitation.rs), [ask.rs](ask.rs), [completion.rs](completion.rs) | Missing input, service questions, and enum completions |
| [stream.rs](stream.rs), [progress.rs](progress.rs) | In-process streaming and progress conversion |
| [sse.rs](sse.rs), [health.rs](health.rs) | Legacy SSE sessions and optional ping-shaped health probes |

From the repository root, `cargo test -p tpp-agents` exercises actual MCP HTTP
conversations, hosting policies, duplicate detection, generated handlers, and
helper parity. See [tests](../../tests/README.md). SDK wire types stay in the
SDK; changes here should preserve the generated-handler seam.

## Runnable echo tool

This handler advertises one tool and echoes its arguments. It uses the SDK
re-export, so the SDK types match the runtime's pinned dependency.

```rust
use agents::{Config, Identity, Runtime, mcp};
use mcp::rmcp::{ErrorData, ServerHandler, model::*, service::{RequestContext, RoleServer}};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct Echo;

impl ServerHandler for Echo {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("echo", "1.0.0"))
    }
    async fn list_tools(&self, _: Option<PaginatedRequestParams>, _: RequestContext<RoleServer>)
        -> Result<ListToolsResult, ErrorData>
    {
        Ok(ListToolsResult::with_all_items(vec![mcp::create_tool(
            "echo", "Echo JSON arguments", r#"{"type":"object","properties":{}}"#,
        )]))
    }
    async fn call_tool(&self, request: CallToolRequestParams, _: RequestContext<RoleServer>)
        -> Result<CallToolResponse, ErrorData>
    {
        if request.name.as_ref() != "echo" {
            return Err(ErrorData::invalid_params("unknown tool", None));
        }
        Ok(mcp::structured_result(&serde_json::Value::Object(
            request.arguments.unwrap_or_default()
        )).into())
    }
}

#[tokio::main]
async fn main() -> agents::Result<()> {
    let runtime = Runtime::new(Config {
        identity: Identity { name: "echo".into(), version: "1.0.0".into(), ..Default::default() },
        host: "127.0.0.1".into(), port: 9000, ..Default::default()
    }).register(mcp::Service::from_handler(Echo).base_path("/mcp"));
    runtime.serve(CancellationToken::new()).await
}
```

Use `tpp-agents`, `serde_json`, `tokio`, and `tokio-util` in the application.
Connect an MCP streamable-HTTP client to `http://127.0.0.1:9000/mcp`, initialize,
list tools, then call `echo`. This long-running example supplies an uncanceled
token; wire it to your application's shutdown signal for graceful termination.
Generated handlers can replace `Echo` in the same `from_handler` call.
