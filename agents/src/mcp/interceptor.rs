//! Cross-cutting middleware for tool dispatch.
//!
//! A tool call and the RPC behind it are the same operation reached two ways, so the
//! middleware that guards one should guard the other. This is the seam that makes an MCP call
//! run the validation, auth and tracing a wire call runs.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use rmcp::ErrorData as McpError;
use serde_json::Value;

/// The dispatch a [`ToolInterceptor`] wraps: arguments in, result out.
///
/// It is boxed rather than generic because an interceptor chain holds a different handler at
/// every layer, and each has to name the next one's type.
pub type ToolHandler<'a> = Box<
    dyn FnOnce(Value) -> Pin<Box<dyn Future<Output = Result<Value, McpError>> + Send + 'a>>
        + Send
        + 'a,
>;

/// Middleware around one unary tool call.
///
/// Go passes a `grpc.UnaryServerInterceptor` here, which works because Go's interceptor
/// operates on `any`. tonic's interceptor only sees request metadata, so it cannot stand in —
/// this trait is the equivalent seam at the level tool dispatch actually happens.
pub trait ToolInterceptor: Send + Sync + 'static {
    /// Runs around `next`, which invokes the tool.
    ///
    /// `method` is the RPC's full name, e.g. `/todo.v1.TodoService/CreateTodo`, so middleware
    /// written against the wire call can key on exactly the same string.
    fn intercept<'a>(
        &'a self,
        method: &'a str,
        arguments: Value,
        next: ToolHandler<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<Value, McpError>> + Send + 'a>>;
}

/// A chain of interceptors, run in the order they were added.
#[derive(Clone, Default)]
pub struct Interceptors(Vec<Arc<dyn ToolInterceptor>>);

impl Interceptors {
    /// An empty chain, which dispatches straight to the tool.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds one interceptor to the end of the chain.
    #[must_use]
    pub fn with(mut self, interceptor: Arc<dyn ToolInterceptor>) -> Self {
        self.0.push(interceptor);
        self
    }

    /// Whether anything is installed.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Dispatches a tool call through the chain.
    ///
    /// With nothing installed this is just `handler(arguments)`, so an unconfigured server
    /// pays no more than a direct call.
    pub async fn invoke<'a>(
        &'a self,
        method: &'a str,
        arguments: Value,
        handler: ToolHandler<'a>,
    ) -> Result<Value, McpError> {
        self.invoke_from(0, method, arguments, handler).await
    }

    /// Runs the chain from `index` onward. Separate from [`Interceptors::invoke`] because
    /// each layer has to hand the next its own position.
    fn invoke_from<'a>(
        &'a self,
        index: usize,
        method: &'a str,
        arguments: Value,
        handler: ToolHandler<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<Value, McpError>> + Send + 'a>> {
        match self.0.get(index) {
            None => Box::pin(async move { handler(arguments).await }),
            Some(interceptor) => {
                let next: ToolHandler<'a> =
                    Box::new(move |args: Value| self.invoke_from(index + 1, method, args, handler));
                interceptor.intercept(method, arguments, next)
            }
        }
    }
}

impl std::fmt::Debug for Interceptors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Interceptors")
            .field("count", &self.0.len())
            .finish()
    }
}
