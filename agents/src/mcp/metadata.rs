//! Carrying an MCP progress token across the hop into gRPC.
//!
//! Go writes these onto a `context.Context`'s outgoing or incoming metadata. A tonic request
//! carries its metadata itself, so here they write onto a [`MetadataMap`] the caller then
//! puts on the request.

use rmcp::model::{NumberOrString, ProgressToken};
use tonic::metadata::{Ascii, MetadataMap, MetadataValue};

/// The gRPC metadata key for the MCP progress token.
///
/// When a client sends `progressToken` in `params._meta`, the token is forwarded under this
/// key, so a gRPC server can skip producing progress nobody is watching.
pub const GRPC_PROGRESS_TOKEN_KEY: &str = "mcp-progress-token";

/// The token as metadata text: a token is a string or an integer, and metadata carries text.
fn token_value(token: &ProgressToken) -> Option<MetadataValue<Ascii>> {
    let text = match &token.0 {
        NumberOrString::Number(number) => number.to_string(),
        NumberOrString::String(text) => text.to_string(),
    };
    text.parse().ok()
}

/// Adds the progress token to outgoing metadata, replacing any already set.
///
/// Call it before forwarding to a gRPC backend. With no token the metadata is left alone.
pub fn with_progress_token(metadata: &mut MetadataMap, token: Option<&ProgressToken>) {
    if let Some(value) = token.and_then(token_value) {
        metadata.insert(GRPC_PROGRESS_TOKEN_KEY, value);
    }
}

/// Incoming metadata holding only the progress token, for calling a streaming handler
/// in-process so that its check of the incoming metadata finds it.
///
/// Like Go's `WithIncomingProgressToken` this replaces rather than merges; with no token there
/// is nothing to replace with, so the result is `None`.
pub fn with_incoming_progress_token(token: Option<&ProgressToken>) -> Option<MetadataMap> {
    let value = token_value(token?)?;
    let mut metadata = MetadataMap::new();
    metadata.insert(GRPC_PROGRESS_TOKEN_KEY, value);
    Some(metadata)
}
