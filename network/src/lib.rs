//! GraphQL, HTTP, and WebSocket clients behind a single factory ([`Network::new_connection`])
//! with consistent connection options and optional connectivity verification.
//!
//! All three client types are created via [`Network::new_connection`] and configured with
//! [`Network::with_opts`]:
//!
//! - GraphQL: queries, mutations, and subscriptions against a GraphQL endpoint.
//! - HTTP: GET, POST, PUT, PATCH, DELETE with retries and cancellation support.
//! - WebSocket: full-duplex send/receive with optional auto-reconnect and keepalive.
//!
//! By default `Connect` verifies the target is reachable before returning; set
//! [`ConnectionOptions::skip_connectivity_check`] to skip the HTTP/GraphQL check. Errors are
//! always returned to the caller; the crate performs no logging of its own.

#![warn(missing_docs)]

mod client;
mod error;
pub mod graphql;
mod http;
mod options;
mod transport;
mod url;
mod websocket;

pub use client::{Network, NetworkClient};
pub use error::{Error, Result};
pub use graphql::{
    BatchOp, FieldArg, GraphQLClient, Subscription, DEFAULT_GRAPHQL_CONNECTIVITY_QUERY,
};
pub use http::{HttpClient, HttpMethod};
pub use options::{
    ClientType, ConnectionOptions, Id, UrlOptions, UrlScheme, DEFAULT_TIMEOUT,
};
pub use url::{url_options_from_std, websocket_url};
pub use websocket::{Message, WebSocketClient};
