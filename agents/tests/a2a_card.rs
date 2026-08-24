//! What a client is told: the card, the URLs on it, and the base path everything hangs off.

#![cfg(feature = "a2a")]

mod common;

use agents::a2a::{self, Transport};
use agents::{Config, Identity, Placement, Runtime};
use common::echo_agent;
use tokio_util::sync::CancellationToken;

/// A placement standing in for one the runtime would hand a service.
fn placement(addr: &str, public_url: &str) -> Placement {
    Placement {
        identity: Identity {
            name: "my-agent".into(),
            description: "does things".into(),
            version: "2.0.0".into(),
        },
        addr: addr.into(),
        mux: agents::Mux::new(),
        grpc_routes: None,
        public_url: public_url.into(),
    }
}

#[test]
fn a_card_takes_its_identity_from_the_runtime() {
    let service = a2a::Service::new(echo_agent(), vec![a2a::skill("echo", "Echo", "Repeats")]);
    let card = service.build_card(&placement("127.0.0.1:9000", ""));

    assert_eq!(card.name, "my-agent");
    assert_eq!(card.description, "does things");
    assert_eq!(card.version, "2.0.0");
    assert_eq!(card.skills[0].id, "echo");
    assert_eq!(card.default_input_modes, vec!["text/plain"]);
}

#[test]
fn every_configured_transport_is_declared_in_order() {
    let service =
        a2a::Service::new(echo_agent(), vec![]).transports([Transport::JsonRpc, Transport::Rest]);
    let card = service.build_card(&placement("127.0.0.1:9000", ""));

    let bindings: Vec<&str> = card
        .supported_interfaces
        .iter()
        .map(|i| i.protocol_binding.as_str())
        .collect();
    assert_eq!(bindings, vec!["JSONRPC", "HTTP+JSON"]);
}

#[test]
fn a_public_url_overrides_the_listen_address() {
    let service = a2a::Service::new(echo_agent(), vec![]);
    let card = service.build_card(&placement("0.0.0.0:9000", "https://agents.example.com/"));

    assert_eq!(
        card.supported_interfaces[0].url, "https://agents.example.com/a2a",
        "past a proxy the listen address is the wrong thing to advertise"
    );
}

#[test]
fn a_wildcard_bind_advertises_loopback_instead() {
    let service = a2a::Service::new(echo_agent(), vec![]);
    let card = service.build_card(&placement("0.0.0.0:9000", ""));

    // 0.0.0.0 is a bind instruction, not a destination.
    assert_eq!(
        card.supported_interfaces[0].url,
        "http://127.0.0.1:9000/a2a"
    );
}

#[test]
fn a_caller_supplied_card_is_served_untouched() {
    let mut supplied =
        a2a::Service::new(echo_agent(), vec![]).build_card(&placement("127.0.0.1:9000", ""));
    supplied.name = "signed-and-registered".into();

    let service = a2a::Service::new(echo_agent(), vec![]).card(supplied.clone());
    let built = service.build_card(&placement("127.0.0.1:9000", ""));

    // Rebuilding a signed card would invalidate the signature.
    assert_eq!(built.name, "signed-and-registered");
}

#[test]
fn a_grpc_binding_advertises_a_dial_target_not_a_url() {
    let service = a2a::Service::new(echo_agent(), vec![]).transports([Transport::Grpc]);
    let endpoints = service.endpoints(&placement("127.0.0.1:50051", ""));

    assert_eq!(
        endpoints[0].url, "127.0.0.1:50051",
        "a scheme here turns a gRPC dial into a name-resolution error"
    );
}

#[test]
fn endpoints_report_where_the_card_is() {
    let service = a2a::Service::new(echo_agent(), vec![]);
    let endpoints = service.endpoints(&placement("127.0.0.1:9000", ""));

    assert_eq!(
        endpoints[0].card_url,
        format!("http://127.0.0.1:9000{}", a2a::AGENT_CARD_PATH)
    );

    // An agent that does not claim the well-known path reports no card.
    let quiet = a2a::Service::new(echo_agent(), vec![]).serve_agent_card(false);
    assert!(
        quiet.endpoints(&placement("127.0.0.1:9000", ""))[0]
            .card_url
            .is_empty()
    );
}

#[test]
fn a_base_path_is_rooted_and_unslashed_whatever_was_written() {
    use agents::a2a::normalize_base_path;
    assert_eq!(normalize_base_path("a2a"), "/a2a");
    assert_eq!(normalize_base_path("/a2a/"), "/a2a");
    assert_eq!(normalize_base_path("/"), "/");
    assert_eq!(normalize_base_path(""), a2a::DEFAULT_BASE_PATH);
}

#[test]
fn a_generated_base_path_wins_over_a_configured_one() {
    use agents::a2a::resolve_base_path;
    // A generated path is part of a contract clients already hold; a hand-set one is a
    // preference.
    assert_eq!(
        resolve_base_path(Some("/todo/v1/agent"), Some("/mine")),
        "/todo/v1/agent"
    );
    assert_eq!(resolve_base_path(None, Some("/mine")), "/mine");
    assert_eq!(resolve_base_path(None, None), a2a::DEFAULT_BASE_PATH);
    assert_eq!(resolve_base_path(Some("  "), Some("/mine")), "/mine");
}

#[tokio::test]
async fn jsonrpc_and_rest_cannot_share_a_root_base_path() {
    let runtime = Runtime::new(Config {
        host: "127.0.0.1".into(),
        port: common::free_port().await,
        ..Default::default()
    })
    .register(
        a2a::Service::new(echo_agent(), vec![])
            .transports([Transport::JsonRpc, Transport::Rest])
            .base_path("/"),
    );

    let err = runtime.start(CancellationToken::new()).await.unwrap_err();
    assert!(err.to_string().contains("base path"), "{err}");
}

#[tokio::test]
async fn an_agent_with_no_transports_is_refused() {
    let runtime = Runtime::new(Config {
        host: "127.0.0.1".into(),
        port: common::free_port().await,
        ..Default::default()
    })
    .register(a2a::Service::new(echo_agent(), vec![]).transports([]));

    let err = runtime.start(CancellationToken::new()).await.unwrap_err();
    assert!(err.to_string().contains("no transports"), "{err}");
}
