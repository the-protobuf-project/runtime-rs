//! Mounting the agent into the placement the runtime handed it.

use std::sync::Arc;

use axum::Router;
use tokio_util::sync::CancellationToken;

use a2a_server::StaticAgentCard;

use crate::agents::{Endpoint, Placement, Protocol, Ready, Requirements};
use crate::error::ServiceError;
use crate::shared::with_header_forwarding;

use super::Transport;
use super::error::Error;
use super::service::Service;

#[async_trait::async_trait]
impl crate::Service for Service {
    fn protocol(&self) -> Protocol {
        Protocol::A2a
    }

    /// Which of the two it asks for depends on the transports: the gRPC binding registers on
    /// the runtime's registry and needs no listener of its own, and an agent serving only that
    /// one should not cause a port to be bound.
    fn requires(&self) -> Requirements {
        Requirements {
            addr: self.addr.clone(),
            http: self.transports.iter().any(|t| t.listens()),
            grpc: self.transports.contains(&Transport::Grpc),
        }
    }

    async fn serve(
        &self,
        cancel: CancellationToken,
        placement: Placement,
        ready: Arc<Ready>,
    ) -> Result<(), ServiceError> {
        // A config that resolves to nothing would start a server no client can reach.
        if self.transports.is_empty() {
            return Err(Error::NoTransports.into());
        }

        let serves_jsonrpc = self.transports.contains(&Transport::JsonRpc);
        let serves_rest = self.transports.contains(&Transport::Rest);

        // At "/" the exact route JSON-RPC wants and the subtree REST wants are the same
        // pattern, and whichever mounts second is the one that never answers.
        if serves_jsonrpc && serves_rest && self.base_path == "/" {
            return Err(Error::BasePathConflict.into());
        }

        if self.transports.contains(&Transport::Grpc) {
            // Failing loudly beats mounting the HTTP transports and quietly not serving the
            // gRPC one, which would leave the card advertising a binding that answers nothing.
            return Err(
                "a2a: the gRPC transport is not wired yet — it registers through \
                 a2a-pb's A2aServiceServer on the runtime's GrpcRoutes, which is pending the \
                 gRPC layer"
                    .into(),
            );
        }

        let card = self.build_card(&placement);
        let resolved = self.endpoints(&placement);

        // JSON-RPC claims only `POST /`, and REST claims named sub-paths, so the two merge
        // without collision and nest together under one base path.
        let mut protocol_router = Router::new();
        if serves_jsonrpc {
            protocol_router =
                protocol_router.merge(a2a_server::jsonrpc::jsonrpc_router(self.handler.clone()));
        }
        if serves_rest {
            protocol_router =
                protocol_router.merge(a2a_server::rest::rest_router(self.handler.clone()));
        }
        protocol_router = with_header_forwarding(protocol_router, self.headers.clone());

        let base_path = self.base_path.clone();
        let serve_card = self.serve_agent_card;
        placement.mux.mount(move |router| {
            let mut router = router.nest(&base_path, protocol_router);
            if serve_card {
                // The well-known path admits one card per host, so on a shared listener the
                // first agent to mount takes it.
                let producer = Arc::new(StaticAgentCard::new(card));
                router = router.merge(a2a_server::agent_card::agent_card_router(producer));
            }
            router
        });

        ready.ready(
            resolved
                .into_iter()
                .map(|endpoint| Endpoint {
                    protocol: Protocol::A2a,
                    transport: endpoint.transport.as_str().to_string(),
                    url: endpoint.url,
                    detail: endpoint.card_url,
                })
                .collect(),
        );

        cancel.cancelled().await;
        Ok(())
    }
}
