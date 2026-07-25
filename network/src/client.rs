use crate::error::{Error, Result};
use crate::graphql::GraphQLClient;
use crate::http::HttpClient;
use crate::options::{ClientType, ConnectionOptions};
use crate::websocket::WebSocketClient;

/// The concrete client behind a [`Network`]. A closed, three-variant set (mirroring the Go
/// package's `Client` interface, which only ever has these three implementations) — enum
/// dispatch here avoids the boxing and dyn-compatibility friction a trait-object design would
/// need for async methods, while `match` exhaustiveness gives compile-time coverage a Go type
/// switch cannot.
pub enum NetworkClient {
    /// A [`GraphQLClient`], present when this `Network` was created with [`ClientType::GraphQL`].
    GraphQL(GraphQLClient),
    /// An [`HttpClient`], present when this `Network` was created with [`ClientType::Http`].
    Http(HttpClient),
    /// A [`WebSocketClient`], present when this `Network` was created with
    /// [`ClientType::WebSocket`].
    WebSocket(WebSocketClient),
}

impl NetworkClient {
    async fn connect(&mut self, opts: ConnectionOptions) -> Result<()> {
        match self {
            Self::GraphQL(c) => c.connect(opts).await,
            Self::Http(c) => c.connect(opts).await,
            Self::WebSocket(c) => c.connect(opts).await,
        }
    }

    /// Closes the underlying client connection. Safe to call multiple times.
    pub async fn close(&mut self) -> Result<()> {
        match self {
            Self::GraphQL(c) => c.close().await,
            Self::Http(c) => c.close().await,
            Self::WebSocket(c) => c.close().await,
        }
    }

    /// Re-establishes the connection using the most recently applied options.
    pub async fn reconnect(&mut self) -> Result<()> {
        match self {
            Self::GraphQL(c) => c.reconnect().await,
            Self::Http(c) => c.reconnect().await,
            Self::WebSocket(c) => c.reconnect().await,
        }
    }
}

/// Wraps a single client (GraphQL, HTTP, or WebSocket) and exposes the connection lifecycle and
/// type-cast accessors. Create with [`Network::new_connection`]; configure with
/// [`Network::with_opts`].
pub struct Network {
    client: NetworkClient,
    options: ConnectionOptions,
}

impl Network {
    /// Creates a new network client of the given type using the factory pattern. The client is
    /// not connected until [`Network::with_opts`] (or the underlying client's `connect`) is
    /// called.
    pub fn new_connection(client_type: ClientType) -> Result<Self> {
        let client = match client_type {
            ClientType::GraphQL => NetworkClient::GraphQL(GraphQLClient::default()),
            ClientType::Http => NetworkClient::Http(HttpClient::default()),
            ClientType::WebSocket => NetworkClient::WebSocket(WebSocketClient::default()),
        };
        Ok(Self { client, options: ConnectionOptions::default() })
    }

    /// Applies the given connection options and establishes the connection (including the
    /// optional connectivity check).
    pub async fn with_opts(&mut self, opts: ConnectionOptions) -> Result<()> {
        self.options = opts.clone();
        self.client.connect(opts).await
    }

    /// Closes the underlying client connection. Safe to call multiple times.
    pub async fn close(&mut self) -> Result<()> {
        self.client.close().await
    }

    /// Re-establishes the connection using the most recent options.
    pub async fn reconnect(&mut self) -> Result<()> {
        self.client.reconnect().await
    }

    /// Returns the underlying client enum.
    pub fn client(&self) -> &NetworkClient {
        &self.client
    }

    /// Returns the underlying client enum, mutably.
    pub fn client_mut(&mut self) -> &mut NetworkClient {
        &mut self.client
    }

    /// Returns the underlying client as a [`GraphQLClient`]. Errors if this `Network` was not
    /// created with [`ClientType::GraphQL`].
    pub fn as_graphql(&self) -> Result<&GraphQLClient> {
        match &self.client {
            NetworkClient::GraphQL(c) => Ok(c),
            _ => Err(Error::TypeCast("GraphQLClient")),
        }
    }

    /// Mutable counterpart of [`Network::as_graphql`].
    pub fn as_graphql_mut(&mut self) -> Result<&mut GraphQLClient> {
        match &mut self.client {
            NetworkClient::GraphQL(c) => Ok(c),
            _ => Err(Error::TypeCast("GraphQLClient")),
        }
    }

    /// Returns the underlying client as an [`HttpClient`]. Errors if this `Network` was not
    /// created with [`ClientType::Http`].
    pub fn as_http(&self) -> Result<&HttpClient> {
        match &self.client {
            NetworkClient::Http(c) => Ok(c),
            _ => Err(Error::TypeCast("HttpClient")),
        }
    }

    /// Mutable counterpart of [`Network::as_http`].
    pub fn as_http_mut(&mut self) -> Result<&mut HttpClient> {
        match &mut self.client {
            NetworkClient::Http(c) => Ok(c),
            _ => Err(Error::TypeCast("HttpClient")),
        }
    }

    /// Returns the underlying client as a [`WebSocketClient`]. Errors if this `Network` was not
    /// created with [`ClientType::WebSocket`].
    pub fn as_websocket(&self) -> Result<&WebSocketClient> {
        match &self.client {
            NetworkClient::WebSocket(c) => Ok(c),
            _ => Err(Error::TypeCast("WebSocketClient")),
        }
    }

    /// Mutable counterpart of [`Network::as_websocket`].
    pub fn as_websocket_mut(&mut self) -> Result<&mut WebSocketClient> {
        match &mut self.client {
            NetworkClient::WebSocket(c) => Ok(c),
            _ => Err(Error::TypeCast("WebSocketClient")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_connection_constructs_each_type() {
        assert!(Network::new_connection(ClientType::Http).is_ok());
        assert!(Network::new_connection(ClientType::GraphQL).is_ok());
        assert!(Network::new_connection(ClientType::WebSocket).is_ok());
    }

    #[tokio::test]
    async fn close_is_safe_before_connect() {
        let mut n = Network::new_connection(ClientType::Http).unwrap();
        assert!(n.close().await.is_ok());
    }

    #[test]
    fn as_accessors_reject_wrong_type() {
        let n = Network::new_connection(ClientType::Http).unwrap();
        assert!(n.as_graphql().is_err());
        assert!(n.as_websocket().is_err());
        assert!(n.as_http().is_ok());
    }
}
