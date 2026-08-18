//! Port of runtime-go/network/network_test.go: the NewConnection factory, the Network
//! lifecycle (WithOpts/Close/Reconnect), and the As*ConnectionType cast helpers.

use std::collections::HashMap;
use std::time::Duration;

use network::{ClientType, ConnectionOptions, Error, Network, UrlOptions, UrlScheme};

/// Options pointing at a host nothing listens on, with the connectivity check skipped so no
/// server is required — the shape runtime-go's TestNetworkReconnect uses.
fn local_opts() -> ConnectionOptions {
    ConnectionOptions {
        url: UrlOptions {
            scheme: UrlScheme::Http,
            host: "localhost".to_string(),
            paths: vec!["/test".to_string()],
            params: HashMap::new(),
        },
        timeout: Duration::from_secs(5),
        skip_connectivity_check: true,
        ..Default::default()
    }
}

#[test]
fn new_connection_builds_each_client_type() {
    for (client_type, expected) in [
        (ClientType::Http, ClientType::Http),
        (ClientType::GraphQL, ClientType::GraphQL),
        (ClientType::WebSocket, ClientType::WebSocket),
    ] {
        let net = Network::new_connection(client_type).expect("client type is supported");
        assert_eq!(net.client().client_type(), expected);
    }
}

#[test]
fn unsupported_client_type_is_rejected() {
    // Go's NewConnection takes a bare string and errors on an unknown one. ClientType is a
    // closed enum here, so the same rejection happens when the string is parsed.
    let err = "invalid".parse::<ClientType>().unwrap_err();
    assert!(matches!(err, Error::UnsupportedClientType(ref s) if s == "invalid"));
    assert_eq!(err.to_string(), "client type not supported: invalid");
}

#[test]
fn client_type_round_trips_through_its_wire_name() {
    for client_type in [ClientType::GraphQL, ClientType::Http, ClientType::WebSocket] {
        let rendered = client_type.to_string();
        assert_eq!(rendered.parse::<ClientType>().unwrap(), client_type);
    }
    assert_eq!(ClientType::GraphQL.to_string(), "graphql");
    assert_eq!(ClientType::Http.to_string(), "http");
    assert_eq!(ClientType::WebSocket.to_string(), "websocket");
}

#[tokio::test]
async fn network_close_succeeds() {
    let mut net = Network::new_connection(ClientType::Http).unwrap();
    net.close()
        .await
        .expect("closing an unconnected client is a no-op");
}

#[tokio::test]
async fn network_with_opts_then_reconnect() {
    let mut net = Network::new_connection(ClientType::Http).unwrap();
    net.with_opts(local_opts())
        .await
        .expect("with_opts succeeds when the connectivity check is skipped");
    net.reconnect()
        .await
        .expect("reconnect reuses the stored options");
}

#[tokio::test]
async fn with_opts_records_options_and_chains() {
    let mut net = Network::new_connection(ClientType::Http).unwrap();
    let http = net
        .with_opts(local_opts())
        .await
        .unwrap()
        .as_http_connection_type()
        .expect("an HTTP Network casts to HttpClient");
    assert_eq!(http.options.url.host, "localhost");
    assert_eq!(net.options().timeout, Duration::from_secs(5));
}

#[tokio::test]
async fn with_opts_records_options_even_when_connect_fails() {
    let mut net = Network::new_connection(ClientType::Http).unwrap();
    let mut bad = local_opts();
    bad.url.paths.clear(); // build_full_url rejects an empty paths list

    assert!(net.with_opts(bad).await.is_err());
    // Go assigns n.options before Connect and keeps it on failure; so does this port.
    assert_eq!(net.options().url.host, "localhost");
}

#[test]
fn cast_helpers_reject_the_wrong_client_type() {
    let mut http = Network::new_connection(ClientType::Http).unwrap();
    assert!(http.as_http_connection_type().is_ok());
    assert!(matches!(
        http.as_graphql_connection_type(),
        Err(Error::ClientCast("GraphQLClient"))
    ));
    assert!(matches!(
        http.as_websocket_connection_type(),
        Err(Error::ClientCast("WebSocketClient"))
    ));

    let mut graphql = Network::new_connection(ClientType::GraphQL).unwrap();
    assert!(graphql.as_graphql_connection_type().is_ok());
    assert!(matches!(
        graphql.as_http_connection_type(),
        Err(Error::ClientCast("HTTPClient"))
    ));

    let mut ws = Network::new_connection(ClientType::WebSocket).unwrap();
    assert!(ws.as_websocket_connection_type().is_ok());
    assert!(matches!(
        ws.as_http_connection_type(),
        Err(Error::ClientCast("HTTPClient"))
    ));
}

#[test]
fn cast_error_message_matches_go() {
    let mut http = Network::new_connection(ClientType::Http).unwrap();
    // `.err()` rather than `unwrap_err()`: the Ok type is `&mut GraphQLClient`, and the
    // crate's client types deliberately carry no Debug impl.
    let err = http
        .as_graphql_connection_type()
        .err()
        .expect("wrong cast fails");
    assert_eq!(err.to_string(), "failed to cast to GraphQLClient");
}

#[tokio::test]
async fn client_lifecycle_is_reachable_through_the_uniform_client_type() {
    let mut net = Network::new_connection(ClientType::Http).unwrap();
    net.client_mut()
        .connect(local_opts())
        .await
        .expect("Client::connect dispatches to HttpClient::connect");
    net.client_mut().reconnect().await.unwrap();
    net.client_mut().close().await.unwrap();
}
