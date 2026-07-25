//! Stable, single-import facade that generated GraphQL clients depend on. Re-exports the
//! essentials of the underlying [`network`] crate so generated code references one crate (this
//! one) instead of reaching into transport internals.
//!
//! Generated code typically does:
//!
//! ```no_run
//! # async fn example() -> runtime::Result<()> {
//! let mut network = runtime::new_connection(runtime::ClientType::GraphQL)?;
//! network.with_opts(runtime::ConnectionOptions {
//!     url: runtime::UrlOptions {
//!         scheme: runtime::HTTP,
//!         host: "localhost:3280".to_string(),
//!         paths: vec!["/graphql".to_string()],
//!         params: Default::default(),
//!     },
//!     ..Default::default()
//! }).await?;
//! let gql = network.as_graphql()?;
//! # Ok(())
//! # }
//! ```
//!
//! Go's facade also re-exports `Boolean`/`Float`/`Int`/`String` GraphQL scalar aliases. Those
//! existed only to support `hasura/go-graphql-client`'s reflection-based query building, which
//! this workspace's `network` crate does not use (typed GraphQL operations take an explicit
//! query string instead) — so only [`Id`] is carried forward here.

#![warn(missing_docs)]

mod tx;

pub use network::{
    BatchOp, ClientType, ConnectionOptions, Error, FieldArg, GraphQLClient, HttpClient, HttpMethod,
    Id, Message, Network, NetworkClient, Result, Subscription, UrlOptions, UrlScheme,
    WebSocketClient, DEFAULT_GRAPHQL_CONNECTIVITY_QUERY, DEFAULT_TIMEOUT,
};
pub use tx::Tx;

// Client-type and URL-scheme constants, re-exported for call-site parity with the Go facade
// (`runtime.GraphQLConnClient`-style usage).

/// Creates a [`GraphQLClient`].
pub const GRAPHQL_CONN_CLIENT: ClientType = ClientType::GraphQL;
/// Creates an [`HttpClient`].
pub const HTTP_CONN_CLIENT: ClientType = ClientType::Http;
/// Creates a [`WebSocketClient`].
pub const WEBSOCKET_CONN_CLIENT: ClientType = ClientType::WebSocket;

/// Plain HTTP.
pub const HTTP: UrlScheme = UrlScheme::Http;
/// TLS-secured HTTP.
pub const HTTPS: UrlScheme = UrlScheme::Https;
/// Plain WebSocket.
pub const WS: UrlScheme = UrlScheme::Ws;
/// TLS-secured WebSocket.
pub const WSS: UrlScheme = UrlScheme::Wss;

/// Creates a network client of the given type using the factory. See
/// [`network::Network::new_connection`].
pub fn new_connection(client_type: ClientType) -> Result<Network> {
    Network::new_connection(client_type)
}

/// Converts a parsed [`url::Url`] into [`UrlOptions`] for [`ConnectionOptions`], so generated
/// clients can connect straight from `Url::parse` output.
pub fn url_options_from_std(u: &url::Url) -> Result<UrlOptions> {
    network::url_options_from_std(u)
}
