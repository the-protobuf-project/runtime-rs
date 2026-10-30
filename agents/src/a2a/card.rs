//! Assembling the card a client fetches first.
//!
//! A caller who already has a card — from a config file, a registry, or a signed manifest —
//! sets [`ServerConfig::card`] and gets it back untouched. Everything else here exists so the
//! common case does not have to: the identity, the skills, and the addresses the agent
//! actually listens on are all things the config knows, and restating them in a card by hand
//! is how the card and the server drift apart.

use a2a::{AgentCard, AgentInterface};

use crate::agents::Placement;

use super::DEFAULT_MODE;
use super::config::ServerConfig;
use super::endpoint::{Endpoint, public_url_for, server_endpoints};
use super::service::Service;

/// Assembles the public agent card from `cfg`.
///
/// The advertised URLs come from [`ServerConfig::public_url`] when it is set, because a
/// server behind a proxy or inside a container cannot infer the address a client will use.
/// Without one they fall back to the listen address, which is right for local development
/// and wrong the moment there is a hop.
pub fn build_card(cfg: &ServerConfig) -> AgentCard {
    if let Some(card) = &cfg.card {
        return card.clone();
    }

    let modes_or_default = |modes: &[String]| {
        if modes.is_empty() {
            vec![DEFAULT_MODE.to_string()]
        } else {
            modes.to_vec()
        }
    };

    // Every transport the agent serves is declared, in configured order. A client picks from
    // this list, so a transport served but unlisted is one nobody will use, and one listed
    // but unserved is a failed dial.
    let supported_interfaces = cfg
        .resolved_transports()
        .into_iter()
        .map(|transport| {
            AgentInterface::new(public_url_for(cfg, transport), transport.protocol_binding())
        })
        .collect();

    AgentCard {
        name: cfg.name.clone(),
        description: cfg.description.clone(),
        version: cfg.version.clone(),
        supported_interfaces,
        capabilities: cfg.capabilities.clone(),
        default_input_modes: modes_or_default(&cfg.default_input_modes),
        default_output_modes: modes_or_default(&cfg.default_output_modes),
        // The field is required on the wire; a Vec is never null, so an agent with no skills
        // still sends `[]` rather than a client-side failure that is hard to trace back here.
        skills: cfg.skills.clone(),
        provider: cfg.provider.clone(),
        documentation_url: cfg.documentation_url.clone(),
        icon_url: cfg.icon_url.clone(),
        security_schemes: None,
        security_requirements: None,
        signatures: None,
    }
}

impl Service {
    /// Every endpoint this agent resolves to at `placement`, in the order they are
    /// advertised.
    ///
    /// An agent serving both JSON-RPC and gRPC has two, and a host printing a startup summary
    /// wants both lines.
    pub fn endpoints(&self, placement: &Placement) -> Vec<Endpoint> {
        server_endpoints(&self.config_for(placement))
    }

    /// Builds the card this agent serves at `placement`, from the runtime's identity and this
    /// agent's skills, unless the caller supplied one.
    pub fn build_card(&self, placement: &Placement) -> AgentCard {
        build_card(&self.config_for(placement))
    }
}
