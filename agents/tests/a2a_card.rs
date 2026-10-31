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
fn a_base_path_is_cleaned_the_way_go_cleans_it() {
    use agents::a2a::normalize_base_path;
    // Go runs path.Clean, so doubled slashes, dot segments and climbs resolve the same way.
    assert_eq!(normalize_base_path("//a2a//v1/"), "/a2a/v1");
    assert_eq!(normalize_base_path("/a2a/./v1"), "/a2a/v1");
    assert_eq!(normalize_base_path("/a2a/x/../v1"), "/a2a/v1");
    assert_eq!(normalize_base_path("/../.."), "/");
    assert_eq!(normalize_base_path("."), "/");
}

#[test]
fn a_generated_base_path_wins_over_a_configured_one() {
    use agents::a2a::{ServerConfig, resolve_base_path};
    let cfg = |generated: &str, configured: &str| ServerConfig {
        generated_base_path: generated.into(),
        base_path: configured.into(),
        ..Default::default()
    };
    // A generated path is part of a contract clients already hold; a hand-set one is a
    // preference.
    assert_eq!(
        resolve_base_path(&cfg("/todo/v1/agent", "/mine"), ""),
        "/todo/v1/agent"
    );
    assert_eq!(resolve_base_path(&cfg("", "/mine"), ""), "/mine");
    assert_eq!(resolve_base_path(&cfg("", ""), ""), a2a::DEFAULT_BASE_PATH);
    assert_eq!(resolve_base_path(&cfg("  ", "/mine"), ""), "/mine");
    // The caller's generated default outranks both.
    assert_eq!(
        resolve_base_path(&cfg("/gen", "/mine"), "/first/"),
        "/first"
    );
}

#[test]
fn a_card_url_is_empty_only_when_a_host_mux_serves_the_card() {
    use agents::a2a::{ServerConfig, server_endpoints};
    let off = ServerConfig {
        addr: "127.0.0.1:9000".into(),
        serve_agent_card: false,
        ..Default::default()
    };
    // Without a mux the server owns its listener and always serves the card.
    assert_eq!(
        server_endpoints(&off)[0].card_url,
        "http://127.0.0.1:9000/.well-known/agent-card.json"
    );
    let shared = ServerConfig {
        mux: Some(agents::Mux::new()),
        ..off
    };
    assert!(server_endpoints(&shared)[0].card_url.is_empty());
}

#[test]
fn the_preferred_endpoint_is_the_first_transport() {
    use agents::a2a::{ServerConfig, server_endpoint};
    let cfg = ServerConfig {
        transports: vec![Transport::Grpc, Transport::JsonRpc],
        addr: "127.0.0.1:50051".into(),
        ..Default::default()
    };
    let first = server_endpoint(&cfg).expect("an endpoint");
    assert_eq!(first.transport, Transport::Grpc);
    assert_eq!(first.url, "127.0.0.1:50051");

    // The singular field is the common case; the list wins when both are set.
    let single = ServerConfig {
        transport: Some(Transport::Rest),
        ..Default::default()
    };
    assert_eq!(server_endpoint(&single).unwrap().transport, Transport::Rest);
    assert_eq!(
        server_endpoint(&ServerConfig::default()).unwrap().url,
        "http://127.0.0.1:9000/a2a"
    );
}

#[test]
fn a_public_url_replaces_the_path_as_go_does() {
    use agents::a2a::{ServerConfig, server_endpoints};
    let cfg = ServerConfig {
        public_url: "https://agents.example.com/ignored/".into(),
        transports: vec![Transport::JsonRpc, Transport::Grpc],
        ..Default::default()
    };
    let endpoints = server_endpoints(&cfg);
    assert_eq!(endpoints[0].url, "https://agents.example.com/a2a");
    assert_eq!(endpoints[1].url, "agents.example.com");
    assert_eq!(
        endpoints[0].card_url,
        "https://agents.example.com/.well-known/agent-card.json"
    );
}

#[test]
fn default_modes_are_configurable() {
    use agents::a2a::{ServerConfig, build_card};
    let card = build_card(&ServerConfig {
        default_input_modes: vec!["application/json".into()],
        ..Default::default()
    });
    assert_eq!(card.default_input_modes, vec!["application/json"]);
    assert_eq!(card.default_output_modes, vec!["text/plain"]);

    let service = a2a::Service::new(echo_agent(), vec![])
        .default_output_modes(["image/png".to_string()])
        .provider(a2a::Provider {
            organization: "Acme".into(),
            url: "https://acme.example".into(),
        })
        .documentation_url("https://docs.example")
        .icon_url("https://icon.example/i.png");
    let card = service.build_card(&placement("127.0.0.1:9000", ""));
    assert_eq!(card.default_output_modes, vec!["image/png"]);
    assert_eq!(card.provider.unwrap().organization, "Acme");
    assert_eq!(
        card.documentation_url.as_deref(),
        Some("https://docs.example")
    );
    assert_eq!(card.icon_url.as_deref(), Some("https://icon.example/i.png"));
}

#[test]
fn an_ipv6_listen_address_keeps_its_brackets() {
    use agents::a2a::host_port_or_default;
    assert_eq!(host_port_or_default("[::1]:9000"), "[::1]:9000");
    assert_eq!(host_port_or_default("[::]:9000"), "127.0.0.1:9000");
    assert_eq!(host_port_or_default(":9000"), "127.0.0.1:9000");
    assert_eq!(host_port_or_default("example.com"), "example.com");
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
