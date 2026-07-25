//! Stable, single-import facade that generated GraphQL clients depend on. Re-exports the
//! essentials of the underlying [`network`] crate so generated code references one crate (this
//! one) instead of reaching into transport internals.
//!
//! Generated code typically does:
//!
//! ```no_run
//! # async fn example() -> runtime::Result<()> {
//! let mut gql = runtime::GraphQLClient::default();
//! gql.connect(runtime::ConnectionOptions {
//!     url: runtime::UrlOptions {
//!         scheme: runtime::HTTP,
//!         host: "localhost:3280".to_string(),
//!         paths: vec!["/graphql".to_string()],
//!         params: Default::default(),
//!     },
//!     ..Default::default()
//! }).await?;
//! # Ok(())
//! # }
//! ```
//!
//! Go's facade also re-exports a `NewConnection` factory and a `ClientType` enum, needed there
//! because Go's `Client` interface is the only way to have one constructor return any of three
//! concrete types. Rust doesn't need that indirection — the client type is always known at the
//! call site, so [`GraphQLClient`], [`HttpClient`], and [`WebSocketClient`] are constructed
//! directly via [`Default`]. Go's `Boolean`/`Float`/`Int`/`String` scalar aliases are dropped for
//! the same reason typed operations don't need them (see the crate-level docs on [`network`]) —
//! only [`Id`] is carried forward here.

#![warn(missing_docs)]

mod tx;

pub use network::{
    BatchOp, ConnectionOptions, DEFAULT_GRAPHQL_CONNECTIVITY_QUERY, DEFAULT_TIMEOUT, Error,
    FieldArg, GraphQLClient, HttpClient, HttpMethod, Id, Message, Result, Subscription, UrlOptions,
    UrlScheme, WebSocketClient,
};
pub use tx::Tx;

/// Plain HTTP.
pub const HTTP: UrlScheme = UrlScheme::Http;
/// TLS-secured HTTP.
pub const HTTPS: UrlScheme = UrlScheme::Https;
/// Plain WebSocket.
pub const WS: UrlScheme = UrlScheme::Ws;
/// TLS-secured WebSocket.
pub const WSS: UrlScheme = UrlScheme::Wss;

/// Converts a parsed [`url::Url`] into [`UrlOptions`] for [`ConnectionOptions`], so generated
/// clients can connect straight from `Url::parse` output.
pub fn url_options_from_std(u: &url::Url) -> Result<UrlOptions> {
    network::url_options_from_std(u)
}
