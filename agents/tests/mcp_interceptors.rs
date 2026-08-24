//! The interceptor chain: what makes an MCP tool call run the same middleware — validation,
//! auth, tracing — as the wire RPC behind it.

#![cfg(feature = "mcp")]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use agents::mcp::{Interceptors, ToolHandler, ToolInterceptor};
use rmcp::ErrorData as McpError;
use serde_json::{Value, json};

/// Records the order layers ran in, and tags the arguments on the way through.
struct Recorder {
    tag: &'static str,
    order: Arc<std::sync::Mutex<Vec<&'static str>>>,
}

impl ToolInterceptor for Recorder {
    fn intercept<'a>(
        &'a self,
        method: &'a str,
        mut arguments: Value,
        next: ToolHandler<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<Value, McpError>> + Send + 'a>> {
        Box::pin(async move {
            self.order.lock().unwrap().push(self.tag);
            if let Some(map) = arguments.as_object_mut() {
                map.insert("seen_method".into(), Value::String(method.to_string()));
            }
            next(arguments).await
        })
    }
}

/// Refuses the call, standing in for a validation or auth layer.
struct Reject;

impl ToolInterceptor for Reject {
    fn intercept<'a>(
        &'a self,
        _method: &'a str,
        _arguments: Value,
        _next: ToolHandler<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<Value, McpError>> + Send + 'a>> {
        Box::pin(async move { Err(McpError::invalid_params("rejected", None)) })
    }
}

#[tokio::test]
async fn an_empty_chain_dispatches_straight_to_the_tool() {
    let result = Interceptors::new()
        .invoke(
            "/counter.v1.CounterService/Count",
            json!({"to": 3}),
            Box::new(|args| Box::pin(async move { Ok(args) })),
        )
        .await
        .expect("dispatch");
    assert_eq!(result, json!({"to": 3}));
}

#[tokio::test]
async fn interceptors_run_in_order_and_see_the_rpc_method_name() {
    let order = Arc::new(std::sync::Mutex::new(Vec::new()));
    let chain = Interceptors::new()
        .with(Arc::new(Recorder {
            tag: "outer",
            order: Arc::clone(&order),
        }))
        .with(Arc::new(Recorder {
            tag: "inner",
            order: Arc::clone(&order),
        }));

    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);

    let result = chain
        .invoke(
            "/counter.v1.CounterService/Count",
            json!({"to": 3}),
            Box::new(move |args| {
                Box::pin(async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    Ok(args)
                })
            }),
        )
        .await
        .expect("dispatch");

    assert_eq!(*order.lock().unwrap(), vec!["outer", "inner"]);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the tool runs exactly once"
    );
    assert_eq!(
        result["seen_method"], "/counter.v1.CounterService/Count",
        "middleware keys on the same method name the wire call carries"
    );
}

#[tokio::test]
async fn an_interceptor_that_refuses_stops_the_tool_from_running() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&calls);

    let error = Interceptors::new()
        .with(Arc::new(Reject))
        .invoke(
            "/counter.v1.CounterService/Count",
            json!({}),
            Box::new(move |args| {
                Box::pin(async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    Ok(args)
                })
            }),
        )
        .await
        .expect_err("the chain should refuse");

    assert!(error.message.contains("rejected"), "{error:?}");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "a refused call must never reach the implementation"
    );
}
