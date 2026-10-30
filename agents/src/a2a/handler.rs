//! The transport-agnostic request handler every transport wraps.
//!
//! The SDK's constructors take their executor and stores by value, while a config here is
//! `Clone` and may be served more than once — a host that restarts replays it. The adapters
//! below let shared `Arc`s stand where the SDK wants owned values, so each start builds a
//! fresh handler around the same agent, which is what Go's `StartServer` does too.

use std::sync::Arc;

use a2a::{
    A2AError, AgentCard, CancelTaskRequest, DeleteTaskPushNotificationConfigRequest,
    GetExtendedAgentCardRequest, GetTaskPushNotificationConfigRequest, GetTaskRequest,
    ListTaskPushNotificationConfigsRequest, ListTaskPushNotificationConfigsResponse,
    ListTasksRequest, ListTasksResponse, SendMessageRequest, SendMessageResponse, StreamResponse,
    SubscribeToTaskRequest, Task, TaskPushNotificationConfig,
};
use a2a_server::task_store::TaskVersion;
use a2a_server::{
    AgentExecutor, DefaultRequestHandler, ExecutorContext, InMemoryTaskStore, PushConfigStore,
    RequestHandler, ServiceParams, TaskStore,
};
use futures::stream::BoxStream;

use super::card::build_card;
use super::config::ServerConfig;
use super::executor::Events;

/// The handler [`new_request_handler`] builds: the SDK's, with the card attached as the
/// extended agent card and the caller's layers around it.
///
/// It is a named type rather than an `Arc<dyn RequestHandler>` because the SDK's routers and
/// its gRPC binding are generic over a sized handler.
pub struct Handler {
    inner: Arc<dyn RequestHandler>,
}

impl std::fmt::Debug for Handler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handler").finish_non_exhaustive()
    }
}

/// Builds the transport-agnostic handler every transport wraps.
///
/// Most callers want [`super::start_server`] instead; this is for a host that mounts the
/// transports itself. The card is attached as the extended agent card, which is what an
/// authenticated client receives when it asks for more detail than the public card carries.
pub fn new_request_handler(cfg: &ServerConfig, executor: Arc<dyn AgentExecutor>) -> Arc<Handler> {
    let opts = &cfg.handler_options;
    let executor = SharedExecutor(executor);
    let mut sdk = match &opts.task_store {
        Some(store) => DefaultRequestHandler::new(executor, SharedTaskStore(Arc::clone(store))),
        None => DefaultRequestHandler::new(executor, InMemoryTaskStore::new()),
    };
    if let Some(store) = &opts.push_config_store {
        sdk = sdk.with_push_config_store(SharedPushConfigStore(Arc::clone(store)));
    }

    let mut handler: Arc<dyn RequestHandler> = Arc::new(ExtendedCard {
        inner: sdk,
        card: build_card(cfg),
    });
    for layer in &opts.layers {
        handler = layer(handler);
    }
    tracing::debug!(
        layers = opts.layers.len(),
        task_store = opts.task_store.is_some(),
        push = opts.push_config_store.is_some(),
        "a2a: request handler built"
    );
    Arc::new(Handler { inner: handler })
}

/// The SDK's handler, answering the extended card call with `card` — Go's
/// `a2asrv.WithExtendedAgentCard`.
struct ExtendedCard {
    inner: DefaultRequestHandler,
    card: AgentCard,
}

/// Forwards every [`RequestHandler`] method to `$inner`, so the wrappers below spell out only
/// the method they change.
macro_rules! delegating_handler {
    ($ty:ty, $inner:ident, $($extra:tt)*) => {
        #[async_trait::async_trait]
        impl RequestHandler for $ty {
        async fn send_message(
            &self,
            params: &ServiceParams,
            req: SendMessageRequest,
        ) -> Result<SendMessageResponse, A2AError> {
            self.$inner.send_message(params, req).await
        }

        async fn send_streaming_message(
            &self,
            params: &ServiceParams,
            req: SendMessageRequest,
        ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, A2AError> {
            self.$inner.send_streaming_message(params, req).await
        }

        async fn get_task(
            &self,
            params: &ServiceParams,
            req: GetTaskRequest,
        ) -> Result<Task, A2AError> {
            self.$inner.get_task(params, req).await
        }

        async fn list_tasks(
            &self,
            params: &ServiceParams,
            req: ListTasksRequest,
        ) -> Result<ListTasksResponse, A2AError> {
            self.$inner.list_tasks(params, req).await
        }

        async fn cancel_task(
            &self,
            params: &ServiceParams,
            req: CancelTaskRequest,
        ) -> Result<Task, A2AError> {
            self.$inner.cancel_task(params, req).await
        }

        async fn subscribe_to_task(
            &self,
            params: &ServiceParams,
            req: SubscribeToTaskRequest,
        ) -> Result<BoxStream<'static, Result<StreamResponse, A2AError>>, A2AError> {
            self.$inner.subscribe_to_task(params, req).await
        }

        async fn create_push_config(
            &self,
            params: &ServiceParams,
            req: TaskPushNotificationConfig,
        ) -> Result<TaskPushNotificationConfig, A2AError> {
            self.$inner.create_push_config(params, req).await
        }

        async fn get_push_config(
            &self,
            params: &ServiceParams,
            req: GetTaskPushNotificationConfigRequest,
        ) -> Result<TaskPushNotificationConfig, A2AError> {
            self.$inner.get_push_config(params, req).await
        }

        async fn list_push_configs(
            &self,
            params: &ServiceParams,
            req: ListTaskPushNotificationConfigsRequest,
        ) -> Result<ListTaskPushNotificationConfigsResponse, A2AError> {
            self.$inner.list_push_configs(params, req).await
        }

        async fn delete_push_config(
            &self,
            params: &ServiceParams,
            req: DeleteTaskPushNotificationConfigRequest,
        ) -> Result<(), A2AError> {
            self.$inner.delete_push_config(params, req).await
        }

        $($extra)*
        }
    };
}

delegating_handler! {
    ExtendedCard, inner,

    async fn get_extended_agent_card(
        &self,
        _params: &ServiceParams,
        _req: GetExtendedAgentCardRequest,
    ) -> Result<AgentCard, A2AError> {
        Ok(self.card.clone())
    }
}

delegating_handler! {
    Handler, inner,

    async fn get_extended_agent_card(
        &self,
        params: &ServiceParams,
        req: GetExtendedAgentCardRequest,
    ) -> Result<AgentCard, A2AError> {
        self.inner.get_extended_agent_card(params, req).await
    }
}

/// A shared executor where the SDK wants an owned one.
pub(super) struct SharedExecutor(pub(super) Arc<dyn AgentExecutor>);

impl AgentExecutor for SharedExecutor {
    fn execute(&self, ctx: ExecutorContext) -> Events {
        self.0.execute(ctx)
    }

    fn cancel(&self, ctx: ExecutorContext) -> Events {
        self.0.cancel(ctx)
    }
}

/// A shared task store where the SDK wants an owned one.
struct SharedTaskStore(Arc<dyn TaskStore>);

#[async_trait::async_trait]
impl TaskStore for SharedTaskStore {
    async fn create(&self, task: Task) -> Result<TaskVersion, A2AError> {
        self.0.create(task).await
    }

    async fn update(&self, task: Task) -> Result<TaskVersion, A2AError> {
        self.0.update(task).await
    }

    async fn get(&self, task_id: &str) -> Result<Option<Task>, A2AError> {
        self.0.get(task_id).await
    }

    async fn list(&self, req: &ListTasksRequest) -> Result<ListTasksResponse, A2AError> {
        self.0.list(req).await
    }

    async fn begin_cancel(&self, task_id: &str) -> Result<Task, A2AError> {
        // Forwarded whole: the inner store holds the check-and-transition atomic, and
        // splitting it here would reopen the race the method exists to close.
        self.0.begin_cancel(task_id).await
    }
}

/// A shared push config store where the SDK wants an owned one.
struct SharedPushConfigStore(Arc<dyn PushConfigStore>);

#[async_trait::async_trait]
impl PushConfigStore for SharedPushConfigStore {
    async fn save(
        &self,
        config: TaskPushNotificationConfig,
    ) -> Result<TaskPushNotificationConfig, A2AError> {
        self.0.save(config).await
    }

    async fn get(
        &self,
        task_id: &str,
        config_id: &str,
    ) -> Result<TaskPushNotificationConfig, A2AError> {
        self.0.get(task_id, config_id).await
    }

    async fn list(&self, task_id: &str) -> Result<Vec<TaskPushNotificationConfig>, A2AError> {
        self.0.list(task_id).await
    }

    async fn delete(&self, task_id: &str, config_id: &str) -> Result<(), A2AError> {
        self.0.delete(task_id, config_id).await
    }

    async fn delete_all(&self, task_id: &str) -> Result<(), A2AError> {
        self.0.delete_all(task_id).await
    }
}
