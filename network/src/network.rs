//! Factory and lifecycle for network clients.
//!
//! This module defines the common [`Client`] type, the [`Network`] wrapper that owns a single
//! client, the [`Network::new_connection`] factory, and the type-cast helpers used to obtain a
//! concrete client for type-specific operations.

use crate::error::{Error, Result};
use crate::graphql::GraphQLClient;
use crate::http::HttpClient;
use crate::options::{ClientType, ConnectionOptions};
use crate::websocket::WebSocketClient;

/// The connection lifecycle implemented by the GraphQL, HTTP, and WebSocket clients. Use it to
/// treat connections uniformly; use the `Network::as_*_connection_type` helpers to obtain the
/// concrete type for type-specific operations.
///
/// Go models this as an `interface` satisfied by exactly three types. Rust gets the same closed
/// set as an enum rather than a `dyn` trait: the three `connect` methods differ in receiver
/// mutability ([`WebSocketClient`] connects through `&self`, the other two through `&mut self`),
/// and an `async fn` trait is not object-safe without boxing every call. An enum keeps the
/// dispatch exhaustive, allocation-free, and lets the cast helpers return a genuine `&mut` to
/// the concrete client instead of a downcast.
pub enum Client {
    /// A GraphQL client, created from [`ClientType::GraphQL`].
    GraphQL(GraphQLClient),
    /// An HTTP client, created from [`ClientType::Http`].
    Http(HttpClient),
    /// A WebSocket client, created from [`ClientType::WebSocket`].
    WebSocket(WebSocketClient),
}

impl Client {
    /// Applies `opts` and establishes the connection, dispatching to the concrete client's own
    /// `connect`.
    pub async fn connect(&mut self, opts: ConnectionOptions) -> Result<()> {
        match self {
            Self::GraphQL(c) => c.connect(opts).await,
            Self::Http(c) => c.connect(opts).await,
            Self::WebSocket(c) => c.connect(opts).await,
        }
    }

    /// Closes the underlying connection.
    pub async fn close(&mut self) -> Result<()> {
        match self {
            Self::GraphQL(c) => c.close().await,
            Self::Http(c) => c.close().await,
            Self::WebSocket(c) => c.close().await,
        }
    }

    /// Re-establishes the connection using the client's most recent options.
    pub async fn reconnect(&mut self) -> Result<()> {
        match self {
            Self::GraphQL(c) => c.reconnect().await,
            Self::Http(c) => c.reconnect().await,
            Self::WebSocket(c) => c.reconnect().await,
        }
    }

    /// Reports which [`ClientType`] this client was created from.
    pub fn client_type(&self) -> ClientType {
        match self {
            Self::GraphQL(_) => ClientType::GraphQL,
            Self::Http(_) => ClientType::Http,
            Self::WebSocket(_) => ClientType::WebSocket,
        }
    }
}

/// Wraps a single client (GraphQL, HTTP, or WebSocket) and exposes the connection lifecycle and
/// type-cast helpers. Create with [`Network::new_connection`]; configure with
/// [`Network::with_opts`].
pub struct Network {
    client: Client,
    options: ConnectionOptions,
}

impl Network {
    /// Creates a new network client of the given type using the factory pattern. The client is
    /// not connected until [`Network::with_opts`] is called.
    ///
    /// Go returns an error here for a `clientType` outside the three constants. [`ClientType`] is
    /// a closed enum, so that case is unrepresentable at this point and is rejected earlier, when
    /// a string is parsed into a [`ClientType`] (see its [`std::str::FromStr`] impl, which yields
    /// [`Error::UnsupportedClientType`]). The `Result` is kept so the factory's signature still
    /// matches Go's and stays source-compatible if a fallible client type is ever added.
    #[allow(clippy::unnecessary_wraps)] // mirrors runtime-go/network's NewConnection signature
    pub fn new_connection(client_type: ClientType) -> Result<Self> {
        let client = match client_type {
            ClientType::GraphQL => Client::GraphQL(GraphQLClient::default()),
            ClientType::Http => Client::Http(HttpClient::default()),
            ClientType::WebSocket => Client::WebSocket(WebSocketClient::default()),
        };
        Ok(Self {
            client,
            options: ConnectionOptions::default(),
        })
    }

    /// Applies the given connection options and establishes the connection (including the
    /// optional connectivity check). Returns the receiver for chaining and an error if the
    /// connection fails. As in Go, `opts` is recorded even when the connection attempt fails, so
    /// a later [`Network::reconnect`] retries with the options that were asked for.
    pub async fn with_opts(&mut self, opts: ConnectionOptions) -> Result<&mut Self> {
        self.options = opts.clone();
        self.client.connect(opts).await?;
        Ok(self)
    }

    /// Closes the underlying client connection. Safe to call multiple times.
    pub async fn close(&mut self) -> Result<()> {
        self.client.close().await
    }

    /// Re-establishes the connection using the most recent options.
    pub async fn reconnect(&mut self) -> Result<()> {
        self.client.reconnect().await
    }

    /// Returns the underlying [`Client`].
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Returns the underlying [`Client`] mutably, for lifecycle calls made through the uniform
    /// [`Client`] type rather than a concrete client.
    pub fn client_mut(&mut self) -> &mut Client {
        &mut self.client
    }

    /// Returns the options most recently passed to [`Network::with_opts`].
    pub fn options(&self) -> &ConnectionOptions {
        &self.options
    }

    /// Returns the underlying client as a [`GraphQLClient`]. Returns [`Error::ClientCast`] if
    /// this `Network` was not created with [`ClientType::GraphQL`].
    pub fn as_graphql_connection_type(&mut self) -> Result<&mut GraphQLClient> {
        match &mut self.client {
            Client::GraphQL(c) => Ok(c),
            _ => Err(Error::ClientCast("GraphQLClient")),
        }
    }

    /// Returns the underlying client as an [`HttpClient`]. Returns [`Error::ClientCast`] if this
    /// `Network` was not created with [`ClientType::Http`].
    pub fn as_http_connection_type(&mut self) -> Result<&mut HttpClient> {
        match &mut self.client {
            Client::Http(c) => Ok(c),
            _ => Err(Error::ClientCast("HTTPClient")),
        }
    }

    /// Returns the underlying client as a [`WebSocketClient`]. Returns [`Error::ClientCast`] if
    /// this `Network` was not created with [`ClientType::WebSocket`].
    pub fn as_websocket_connection_type(&mut self) -> Result<&mut WebSocketClient> {
        match &mut self.client {
            Client::WebSocket(c) => Ok(c),
            _ => Err(Error::ClientCast("WebSocketClient")),
        }
    }
}
