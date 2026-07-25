//! GraphQL, HTTP, and WebSocket clients, each constructed directly and configured with a shared
//! [`ConnectionOptions`]:
//!
//! - [`GraphQLClient`]: queries, mutations, and subscriptions against a GraphQL endpoint.
//! - [`HttpClient`]: GET, POST, PUT, PATCH, DELETE with retries and cancellation support.
//! - [`WebSocketClient`]: full-duplex send/receive with optional auto-reconnect and keepalive.
//!
//! Each client is built with [`Default`] and configured with its own `connect` method:
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
mod options;
mod transport;
mod url;
mod websocket;

pub use error::{Error, Result};
pub use graphql::{
    BatchOp, DEFAULT_GRAPHQL_CONNECTIVITY_QUERY, FieldArg, GraphQLClient, Subscription,
};
pub use http::{HttpClient, HttpMethod};
pub use options::{ConnectionOptions, DEFAULT_TIMEOUT, Id, UrlOptions, UrlScheme};
pub use url::{url_options_from_std, websocket_url};
pub use websocket::{Message, WebSocketClient};
