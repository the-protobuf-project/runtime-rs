//! Assembling the card a client fetches first, and resolving the URLs that go on it.
//!
//! The identity on it is the runtime's, not the agent's own, so the card and the process's
//! other protocols cannot disagree about what this service is. Everything else here exists so
//! the common case does not have to restate what the runtime already knows — that is how a
//! card and the server behind it drift apart.

use a2a::{AgentCard, AgentInterface};

use crate::agents::Placement;

use super::endpoint::{Endpoint, host_port_or_default};
use super::service::Service;
use super::{AGENT_CARD_PATH, DEFAULT_MODE, Transport};

impl Service {
    /// The base URL clients should be told to use.
    ///
    /// [`crate::Config::public_url`] wins when it is set, because a server behind a proxy or
    /// inside a container cannot infer the address a client will use. Without one this falls
    /// back to the listen address, which is right locally and wrong past any hop.
    pub(super) fn base_url(&self, placement: &Placement) -> String {
        if placement.public_url.is_empty() {
            format!("http://{}", host_port_or_default(&placement.addr))
        } else {
            placement.public_url.trim_end_matches('/').to_string()
        }
    }

    /// Where a client should reach this agent over `transport`.
    pub(super) fn url_for(&self, transport: Transport, base_url: &str) -> String {
        if transport == Transport::Grpc {
            // A gRPC target is host:port, not a URL. A scheme here is what turns a client's
            // dial into a confusing name-resolution error.
            return base_url
                .split_once("://")
                .map_or_else(|| base_url.to_string(), |(_, host)| host.to_string());
        }
        // REST routes on paths of its own beneath the base path; JSON-RPC answers at the base
        // path itself. Both are reached through the same prefix.
        format!("{base_url}{}", self.base_path)
    }

    /// Where this server publishes its card, or empty when it publishes none.
    pub(super) fn card_url(&self, base_url: &str) -> String {
        if !self.serve_agent_card {
            return String::new();
        }
        // The card is at the host root by protocol, not under the agent's base path — clients
        // look for exactly one place and this is it.
        format!("{base_url}{AGENT_CARD_PATH}")
    }

    /// Every endpoint this agent resolves to, in the order they are advertised.
    ///
    /// An agent serving both JSON-RPC and gRPC has two, and a host printing a startup summary
    /// wants both lines.
    pub fn endpoints(&self, placement: &Placement) -> Vec<Endpoint> {
        let base_url = self.base_url(placement);
        let card_url = self.card_url(&base_url);
        self.transports
            .iter()
            .map(|transport| Endpoint {
                transport: *transport,
                protocol: transport.protocol_binding(),
                url: self.url_for(*transport, &base_url),
                card_url: card_url.clone(),
            })
            .collect()
    }

    /// Builds the card from the runtime's identity and this agent's skills, unless the caller
    /// supplied one.
    ///
    /// A caller who already has a card — from a config file, a registry, or a signed manifest
    /// — gets it back untouched, because rebuilding it would invalidate the signature.
    pub fn build_card(&self, placement: &Placement) -> AgentCard {
        if let Some(card) = &self.card {
            return card.clone();
        }

        let base_url = self.base_url(placement);
        // Every transport the agent serves is declared, in configured order. A client picks
        // from this list, so a transport served but unlisted is one nobody will use, and one
        // listed but unserved is a failed dial.
        let supported_interfaces = self
            .transports
            .iter()
            .map(|transport| {
                AgentInterface::new(
                    self.url_for(*transport, &base_url),
                    transport.protocol_binding(),
                )
            })
            .collect();

        AgentCard {
            name: placement.identity.name.clone(),
            description: placement.identity.description.clone(),
            version: placement.identity.version.clone(),
            supported_interfaces,
            capabilities: self.capabilities.clone(),
            default_input_modes: vec![DEFAULT_MODE.to_string()],
            default_output_modes: vec![DEFAULT_MODE.to_string()],
            // The field is required on the wire, and a null there is a client-side failure in
            // a place that is hard to trace back to here.
            skills: self.skills.clone(),
            provider: self.provider.clone(),
            documentation_url: self.documentation_url.clone(),
            icon_url: self.icon_url.clone(),
            security_schemes: None,
            security_requirements: None,
            signatures: None,
        }
    }
}
