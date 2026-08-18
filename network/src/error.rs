//! The single error type returned by every fallible operation in this crate.

/// Every error this crate can return. One flat enum (rather than a per-domain type) mirrors the
/// Go package's single, untyped `error` return: callers generally just read or format it, and
/// [`std::error::Error::source`] walks the same causal chain Go's `errors.Unwrap` does.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A client-type string wasn't one of `graphql`, `http`, or `websocket`. Returned by
    /// [`crate::ClientType`]'s [`std::str::FromStr`] impl, the Rust home of the error Go's
    /// `NewConnection` returns for an unrecognized `ClientType` string.
    #[error("client type not supported: {0}")]
    UnsupportedClientType(String),
    /// An `as_*_connection_type` helper was called on a [`crate::Network`] created with a
    /// different [`crate::ClientType`]. The payload is the Go type name that was requested
    /// (e.g. `"HTTPClient"`).
    #[error("failed to cast to {0}")]
    ClientCast(&'static str),
    /// A [`crate::UrlOptions::scheme`] value wasn't one of `http`, `https`, `ws`, or `wss`.
    #[error("invalid URL scheme: {0}. Must be 'http', 'https', 'ws', or 'wss'")]
    InvalidScheme(String),
    /// [`crate::UrlOptions::host`] was empty.
    #[error("host cannot be empty")]
    EmptyHost,
    /// [`crate::UrlOptions::paths`] was empty.
    #[error("paths array cannot be empty")]
    EmptyPaths,
    /// The requested path index was negative or beyond the end of [`crate::UrlOptions::paths`].
    #[error("pathIndex {index} out of bounds for paths array of length {len}")]
    PathIndexOutOfBounds {
        /// The out-of-range index that was requested.
        index: i64,
        /// The number of paths actually available.
        len: usize,
    },
    /// URL construction failed; wraps one of the URL-validation variants above.
    #[error("failed to build URL: {0}")]
    BuildUrl(Box<Error>),
    /// [`crate::url_from_std`] was given a URL the `url` crate itself rejected.
    #[error("failed to parse URL: {0}")]
    UrlParse(#[from] ::url::ParseError),

    /// The initial HTTP reachability check (HEAD, falling back to GET on 405) failed.
    #[error("failed to connect to HTTP server at {host}: {source}")]
    HttpConnect {
        /// The host that was unreachable.
        host: String,
        /// The underlying transport error.
        #[source]
        source: reqwest::Error,
    },
    /// An [`crate::HttpClient`] method was called before `connect` (or after `close`).
    #[error("HTTP client is not connected")]
    HttpNotConnected,
    /// The `reqwest` request builder rejected the method, URL, or headers.
    #[error("failed to create HTTP request: {0}")]
    BuildRequest(#[source] reqwest::Error),
    /// The request was sent but no response was received (DNS, TCP, or TLS failure).
    #[error("error making request: {0}")]
    SendRequest(#[source] reqwest::Error),
    /// A response was received but its body could not be read.
    #[error("failed to read response body: {0}")]
    ReadBody(#[source] reqwest::Error),
    /// A well-known 4xx status was returned; the second field is a short, human-readable reason
    /// phrase (e.g. `"Not Found"`).
    #[error("client error: status code {0} ({1})")]
    ClientStatus(u16, &'static str),
    /// A 5xx status was returned.
    #[error("server error: status code {0}")]
    ServerStatus(u16),
    /// A status outside the recognized 2xx/4xx/5xx ranges handled above was returned.
    #[error("unexpected status code: {0}")]
    UnexpectedStatus(u16),
    /// [`crate::HttpClient::request`] exhausted its retry budget; wraps the last attempt's error.
    #[error("request failed after {retries} retries: {source}")]
    RetryExhausted {
        /// How many retries were attempted before giving up.
        retries: usize,
        /// The error from the final attempt.
        source: Box<Error>,
    },
    /// The operation's cancellation token fired before it completed.
    #[error("request cancelled")]
    Cancelled,
    /// A single attempt exceeded [`crate::ConnectionOptions::timeout`].
    #[error("request timed out")]
    Timeout,

    /// The WebSocket handshake (dial) failed.
    #[error("failed to dial WebSocket at {host}: {source}")]
    WsConnect {
        /// The host that could not be reached.
        host: String,
        /// The underlying handshake error.
        #[source]
        source: Box<tokio_tungstenite::tungstenite::Error>,
    },
    /// A [`crate::WebSocketClient`] method was called before `connect` (or after `close`).
    #[error("no WebSocket connection available")]
    WsNotConnected,
    /// Writing a frame to the socket failed.
    #[error("failed to send message: {0}")]
    WsSend(#[source] Box<tokio_tungstenite::tungstenite::Error>),
    /// Reading a frame from the socket failed.
    #[error("failed to read message: {0}")]
    WsReceive(#[source] Box<tokio_tungstenite::tungstenite::Error>),
    /// [`crate::WebSocketClient::retry_send`] exhausted its retry budget; wraps the last
    /// attempt's error.
    #[error("failed to send message after {retries} retries: {source}")]
    WsRetryExhausted {
        /// How many retries were attempted before giving up.
        retries: usize,
        /// The error from the final attempt.
        source: Box<Error>,
    },
    /// Auto-reconnect (in [`crate::WebSocketClient::listen`]) failed; wraps the reconnect error.
    #[error("reconnection failed: {0}")]
    WsReconnect(Box<Error>),
    /// The connection ended (a Close frame, EOF, or explicit `close()`).
    #[error("WebSocket connection closed")]
    WsClosed,
    /// A header key or value supplied in [`crate::ConnectionOptions::headers`] was not valid
    /// HTTP header syntax.
    #[error("invalid header {0:?}")]
    InvalidHeader(String),

    /// A [`crate::GraphQLClient`] method was called before `connect` (or after `close`).
    #[error("GraphQL client is not initialized")]
    GraphQLNotInitialized,
    /// The connectivity-check query failed during `connect`/`reconnect`.
    #[error("failed to connect to GraphQL server at {host}: {source}")]
    GraphQLConnect {
        /// The host that failed the connectivity check.
        host: String,
        /// The underlying error from running the check query.
        source: Box<Error>,
    },
    /// [`crate::GraphQLClient::execute_raw_query`] failed; wraps the transport or GraphQL error.
    #[error("failed to execute raw query: {0}")]
    GraphQLRawQuery(Box<Error>),
    /// [`crate::GraphQLClient::exec_raw_mutation`] failed; wraps the transport or GraphQL error.
    #[error("failed to execute raw mutation: {0}")]
    GraphQLRawMutation(Box<Error>),
    /// A typed or field-based query/mutation failed; wraps the transport or GraphQL error.
    #[error("failed to execute operation: {0}")]
    GraphQLOperation(Box<Error>),
    /// [`crate::GraphQLClient::batch_mutate`] failed; wraps the transport or GraphQL error.
    #[error("failed to execute batch mutation: {0}")]
    GraphQLBatch(Box<Error>),
    /// Opening a subscription (handshake, `connection_init`/`connection_ack`, or `subscribe`)
    /// failed.
    #[error("failed to start subscription: {0}")]
    SubscriptionStart(Box<Error>),
    /// A live subscription ended abnormally (a transport error or an unexpected close).
    #[error("subscription stopped: {0}")]
    SubscriptionStopped(Box<Error>),
    /// A subscription's `next` payload could not be decoded into the requested type.
    #[error("failed to decode subscription message: {0}")]
    SubscriptionDecode(#[source] serde_json::Error),
    /// A GraphQL response body could not be parsed as JSON.
    #[error("failed to unmarshal raw response: {0}")]
    GraphQLDecode(#[source] serde_json::Error),
    /// The server returned a non-empty top-level `errors` array.
    #[error("GraphQL errors: {0:?}")]
    GraphQLErrors(Vec<serde_json::Value>),
    /// [`crate::GraphQLClient::mutation_with_input`] was given a value that doesn't serialize to
    /// a JSON object.
    #[error("input must serialize to a JSON object")]
    InputNotAnObject,

    /// An HTTP-transport error not covered by a more specific variant above.
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    /// A JSON encode/decode error not covered by a more specific variant above.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// An I/O error not covered by a more specific variant above.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Shorthand for `Result<T, Error>`, this crate's error type.
pub type Result<T> = std::result::Result<T, Error>;
