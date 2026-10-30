//! Dispatching a tool call the way a gRPC server dispatches an RPC.

use rmcp::ErrorData as McpError;
use serde_json::Value;

use super::{Interceptors, ToolHandler};

/// Runs `handler` through `interceptors` with the RPC's full method name, so middleware
/// written against the wire call sees an MCP call the same way. With no chain it is a direct
/// call.
///
/// Go also hands the service over as `info.Server`. A [`super::ToolInterceptor`] has no use
/// for an untyped server, so there is no counterpart here.
pub async fn invoke_unary<'a>(
    interceptors: Option<&'a Interceptors>,
    full_method: &'a str,
    arguments: Value,
    handler: ToolHandler<'a>,
) -> Result<Value, McpError> {
    match interceptors {
        Some(chain) => chain.invoke(full_method, arguments, handler).await,
        None => handler(arguments).await,
    }
}

/// Composes chains in order, the first outermost, as `grpc.ChainUnaryInterceptor` does.
pub fn chain_unary_interceptors(chains: impl IntoIterator<Item = Interceptors>) -> Interceptors {
    chains
        .into_iter()
        .fold(Interceptors::new(), Interceptors::extend)
}
