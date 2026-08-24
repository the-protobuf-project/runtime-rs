//! Mounting the agent into the placement the runtime handed it.

use std::sync::Arc;

use axum::Router;
use tokio_util::sync::CancellationToken;

use a2a_server::StaticAgentCard;

use crate::agents::{Endpoint, Placement, Protocol, Ready, Requirements};
use crate::error::ServiceError;
use crate::shared::with_header_forwarding;

use super::service::Service;
use super::{AGENT_CARD_PATH, Transport};

#[async_trait::async_trait]
impl crate::Service for Service {
    fn protocol(&self) -> Protocol {
        Protocol::A2a
    }

    /// Which of the two it asks for depends on the transports: the gRPC binding registers on
    /// the runtime's registry and needs no listener of its own, and an agent serving only
    /// that one should not cause a port to be bound.
    fn requires(&self) -> Requirements {
        Requirements {
            addr: self.addr.clone(),
            http: self.transports.iter().any(Transport::listens),
            grpc: self.transports.contains(&Transport::Grpc),
        }
    }

    async fn serve(
        &self,
        cancel: CancellationToken,
        placement: Placement,
        ready: Arc<Ready>,
    ) -> Result<(), ServiceError> {
        if self.transports.contains(&Transport::Grpc) {
            // Failing loudly beats mounting the HTTP transports and quietly not serving the
            // gRPC one, which would leave the card advertising a binding that answers
            // nothing.
            return Err(
                "a2a: the gRPC transport is not wired yet — it registers through a2a-pb's \
                 A2aServiceServer on the runtime's GrpcRoutes, which is pending the gRPC layer"
                    .into(),
            );
        }

        let base_url = self.base_url(&placement);
        let card = self.build_card(&placement, &base_url);

        // JSON-RPC claims only `POST /`, and REST claims named sub-paths, so the two merge
        // without collision and nest together under one base path.
        let mut protocol_router = Router::new();
        if self.transports.contains(&Transport::JsonRpc) {
            protocol_router =
                protocol_router.merge(a2a_server::jsonrpc::jsonrpc_router(self.handler.clone()));
        }
        if self.transports.contains(&Transport::Rest) {
            protocol_router =
                protocol_router.merge(a2a_server::rest::rest_router(self.handler.clone()));
        }
        protocol_router = with_header_forwarding(protocol_router, self.headers.clone());

        let base_path = self.base_path.clone();
        let serve_card = self.serve_agent_card;
        let card_for_mount = card.clone();
        placement.mux.mount(move |router| {
            let mut router = router.nest(&base_path, protocol_router);
            if serve_card {
                let producer = Arc::new(StaticAgentCard::new(card_for_mount));
                router = router.merge(a2a_server::agent_card::agent_card_router(producer));
            }
            router
        });

        let card_url = format!("{base_url}{AGENT_CARD_PATH}");
        let endpoints = self
            .transports
            .iter()
            .map(|transport| Endpoint {
                protocol: Protocol::A2a,
                transport: transport.as_str().to_string(),
                url: format!("{base_url}{}", self.base_path),
                detail: if serve_card {
                    card_url.clone()
                } else {
                    String::new()
                },
            })
            .collect();

        ready.ready(endpoints);
        cancel.cancelled().await;
        Ok(())
    }
}
