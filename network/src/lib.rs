//! GraphQL, HTTP, and WebSocket clients behind a single factory ([`Network::new_connection`])
//! with consistent connection options and optional connectivity verification.
//!
//! All three client types are created via [`Network::new_connection`] and configured with
//! [`Network::with_opts`]:
//!
//! - [`GraphQLClient`]: queries, mutations, and subscriptions against a GraphQL endpoint.
//! - [`HttpClient`]: GET, POST, PUT, PATCH, DELETE with retries and cancellation support.
//! - [`WebSocketClient`]: full-duplex send/receive with optional auto-reconnect and keepalive.
//!
//! ```no_run
//! # async fn example() -> network::Result<()> {
//! use network::{ClientType, ConnectionOptions, Network};
//!
//! let mut net = Network::new_connection(ClientType::Http)?;
//! net.with_opts(ConnectionOptions::default()).await?;
//! let http = net.as_http_connection_type()?;
//! # let _ = http;
//! # Ok(())
//! # }
//! ```
//!
//! Each client is also usable on its own: build it with [`Default`] and call its `connect`
//! method directly, skipping the [`Network`] wrapper.
//!
//! ```no_run
//! # async fn example() -> network::Result<()> {
//! use network::{ConnectionOptions, HttpClient};
//!
//! let mut http = HttpClient::default();
//! http.connect(ConnectionOptions::default()).await?;
//! # Ok(())
//! # }
//! ```
//!
//! By default `connect` verifies the target is reachable before returning; set
//! [`ConnectionOptions::skip_connectivity_check`] to skip the HTTP/GraphQL check. Errors are
//! always returned to the caller; the crate performs no logging of its own.

#![warn(missing_docs)]

mod error;
pub mod graphql;
mod http;
mod network;
mod options;
mod transport;
mod url;
mod websocket;

pub use error::{Error, Result};
pub use graphql::{
    BatchOp, DEFAULT_GRAPHQL_CONNECTIVITY_QUERY, FieldArg, GraphQLClient, Subscription,
    build_graphql_args, struct_to_map,
};
pub use http::{HttpClient, HttpMethod};
pub use network::{Client, Network};
pub use options::{ClientType, ConnectionOptions, DEFAULT_TIMEOUT, Id, UrlOptions, UrlScheme};
pub use url::{url_options_from_std, websocket_url};
pub use websocket::{Message, WebSocketClient};
