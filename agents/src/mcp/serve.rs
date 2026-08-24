//! Mounting the tool server into the placement the runtime handed it.

use std::sync::Arc;

use rmcp::ServerHandler;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use tokio_util::sync::CancellationToken;

use crate::agents::{Endpoint, Placement, Protocol, Ready, Requirements};
use crate::error::ServiceError;
use crate::shared::with_header_forwarding;

use super::Transport;
use super::service::Service;

#[async_trait::async_trait]
impl<H: ServerHandler> crate::Service for Service<H> {
    fn protocol(&self) -> Protocol {
        Protocol::Mcp
    }

    /// MCP never registers on a gRPC server — it dispatches to one in-process — so it asks
    /// only for a place to listen, and only when a transport it speaks actually listens.
    fn requires(&self) -> Requirements {
        Requirements {
            addr: self.addr.clone(),
            http: self.serves_http(),
            grpc: false,
        }
    }

    async fn serve(
        &self,
        cancel: CancellationToken,
        placement: Placement,
        ready: Arc<Ready>,
    ) -> Result<(), ServiceError> {
        let mut endpoints = Vec::new();

        if self.serves_http() {
            let factory = Arc::clone(&self.factory);
            // The config is #[non_exhaustive], so it is built from Default and adjusted
            // rather than named field by field.
            let mut config = StreamableHttpServerConfig::default();
            // The runtime owns the drain, so the transport is told to stop through the same
            // token everything else on this listener stops on.
            config.cancellation_token = cancel.clone();
            let service = StreamableHttpService::new(
                move || factory(),
                Arc::new(LocalSessionManager::default()),
                config,
            );

            let router = with_header_forwarding(
                axum::Router::new().fallback_service(service),
                self.headers.clone(),
            );

            let base_path = self.base_path.clone();
            placement
                .mux
                .mount(move |mux| mux.nest_service(&base_path, router));

            let base_url = if placement.public_url.is_empty() {
                format!("http://{}", placement.addr)
            } else {
                placement.public_url.trim_end_matches('/').to_string()
            };

            for transport in self.transports.iter().filter(|t| t.listens()) {
                endpoints.push(Endpoint {
                    protocol: Protocol::Mcp,
                    transport: transport.as_str().to_string(),
                    url: format!("{base_url}{}", self.base_path),
                    detail: self.base_path.clone(),
                });
            }
        }

        if self.transports.contains(&Transport::Stdio) {
            endpoints.push(Endpoint {
                protocol: Protocol::Mcp,
                transport: Transport::Stdio.as_str().to_string(),
                url: "stdio".to_string(),
                detail: "process stdin/stdout".to_string(),
            });
        }

        ready.ready(endpoints);

        if self.transports.contains(&Transport::Stdio) {
            use rmcp::ServiceExt;
            let handler = (self.factory)()?;
            let running = handler.serve(rmcp::transport::io::stdio()).await?;
            tokio::select! {
                _ = cancel.cancelled() => {}
                result = running.waiting() => { result?; }
            }
            return Ok(());
        }

        cancel.cancelled().await;
        Ok(())
    }
}
