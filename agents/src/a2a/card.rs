//! Assembling the card a client fetches first.
//!
//! The identity on it is the runtime's, not the agent's own, so the card and the process's
//! other protocols cannot disagree about what this service is.

use a2a::{AgentCard, AgentInterface};

use crate::agents::Placement;

use super::Transport;
use super::service::Service;

impl Service {
    /// The base URL clients should be told to use.
    pub(super) fn base_url(&self, placement: &Placement) -> String {
        if placement.public_url.is_empty() {
            format!("http://{}", placement.addr)
        } else {
            placement.public_url.trim_end_matches('/').to_string()
        }
    }

    /// Builds the card from the runtime's identity and this agent's skills, unless the
    /// caller supplied one.
    pub(super) fn build_card(&self, placement: &Placement, base_url: &str) -> AgentCard {
        if let Some(card) = &self.card {
            return card.clone();
        }

        let mut supported_interfaces = Vec::new();
        for transport in &self.transports {
            let url = match transport {
                // A gRPC client dials a host:port authority, not a path under the gateway.
                Transport::Grpc => base_url.to_string(),
                _ => format!("{base_url}{}", self.base_path),
            };
            supported_interfaces.push(AgentInterface::new(url, transport.protocol_binding()));
        }

        AgentCard {
            name: placement.identity.name.clone(),
            description: placement.identity.description.clone(),
            version: placement.identity.version.clone(),
            supported_interfaces,
            capabilities: self.capabilities.clone(),
            default_input_modes: vec!["text/plain".to_string()],
            default_output_modes: vec!["text/plain".to_string()],
            skills: self.skills.clone(),
            provider: None,
            documentation_url: None,
            icon_url: None,
            security_schemes: None,
            security_requirements: None,
            signatures: None,
        }
    }
}
