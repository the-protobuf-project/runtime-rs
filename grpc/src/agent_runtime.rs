//! The agent protocols: one [`agents::Runtime`] for every MCP and A2A service this server
//! speaks.
//!
//! One runtime rather than one per protocol is what keeps the two honest with each other:
//! the identity on an agent card and the identity an MCP client is told are the same string
//! because they came from the same options, and the shutdown that drains one drains the
//! other.

use std::sync::Arc;

use agents::{Config, GrpcRoutes, Identity, Runtime};
use tokio_util::sync::CancellationToken;

use crate::error::Result;
use crate::hybrid::HybridServer;

impl HybridServer {
    /// Builds and starts the agent runtime, when any agent service was registered.
    ///
    /// Called while the gRPC routes are still being assembled, and the ordering is
    /// load-bearing: A2A's gRPC binding registers on `routes`, and tonic takes the routes by
    /// value the moment the server is built, so the runtime has to be up first.
    ///
    /// The MCP and A2A ports stay separate, as they are in Go: each service carries a listen
    /// address of its own rather than sharing the runtime's. A process that wants them on
    /// one port builds a runtime directly instead of going through a `HybridServer`.
    pub(crate) async fn start_agents(&mut self, routes: &GrpcRoutes) -> Result<()> {
        if self.agent_services.is_empty() {
            tracing::debug!("agents: nothing registered, no runtime started");
            return Ok(());
        }

        let runtime = self.agent_services.iter().cloned().fold(
            Runtime::new(Config {
                identity: Identity {
                    name: self.opts.service_name.clone(),
                    description: self.opts.description.clone(),
                    version: self.opts.version.clone(),
                },
                host: self.opts.a2a.host.clone(),
                port: self.opts.a2a.port,
                public_url: self.opts.a2a.public_url.clone(),
                grpc_routes: Some(routes.clone()),
                ..Default::default()
            }),
            Runtime::register_dyn,
        );

        let cancel = CancellationToken::new();
        runtime.start(cancel.clone()).await?;
        let endpoints = runtime.endpoints();
        tracing::debug!(count = endpoints.len(), "agents: runtime up");

        let mut state = self.state.lock().expect("grpc: server state poisoned");
        state.agent_runtime = Some(Arc::new(runtime));
        state.agent_cancel = Some(cancel);
        state.endpoints = endpoints;
        Ok(())
    }

    /// Drains the agent runtime, if one came up.
    pub(crate) async fn stop_agents(&self) -> Result<()> {
        let (runtime, cancel) = {
            let mut state = self.state.lock().expect("grpc: server state poisoned");
            state.endpoints.clear();
            (state.agent_runtime.take(), state.agent_cancel.take())
        };
        if let Some(cancel) = cancel {
            cancel.cancel();
        }
        if let Some(runtime) = runtime {
            runtime.shutdown().await?;
        }
        Ok(())
    }
}
