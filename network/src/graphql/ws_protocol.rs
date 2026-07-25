//! Message types for the [graphql-transport-ws protocol](https://github.com/enisdenjo/graphql-ws/blob/master/PROTOCOL.md),
//! spoken over the `graphql-transport-ws` WebSocket subprotocol. Go's counterpart delegates this
//! entirely to `hasura/go-graphql-client`'s internal `GraphQLWS` implementation; this module is
//! the hand-rolled equivalent this workspace's architecture calls for instead of taking on an
//! external GraphQL-WS client dependency.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The subprotocol name sent as `Sec-WebSocket-Protocol` during the WebSocket handshake.
pub(crate) const SUBPROTOCOL: &str = "graphql-transport-ws";

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ClientMessage<'a> {
    ConnectionInit {
        #[serde(skip_serializing_if = "Option::is_none")]
        payload: Option<Value>,
    },
    Subscribe {
        id: String,
        payload: SubscribePayload<'a>,
    },
    Complete {
        id: String,
    },
    Pong {
        #[serde(skip_serializing_if = "Option::is_none")]
        payload: Option<Value>,
    },
}

#[derive(Debug, Serialize)]
pub(crate) struct SubscribePayload<'a> {
    pub query: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variables: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ServerMessage {
    ConnectionAck {
        #[serde(default)]
        #[allow(dead_code)]
        payload: Option<Value>,
    },
    Next {
        #[allow(dead_code)]
        id: String,
        payload: Value,
    },
    Error {
        #[allow(dead_code)]
        id: String,
        payload: Vec<Value>,
    },
    Complete {
        #[allow(dead_code)]
        id: String,
    },
    Ping {
        #[serde(default)]
        payload: Option<Value>,
    },
    Pong {
        #[serde(default)]
        #[allow(dead_code)]
        payload: Option<Value>,
    },
}
