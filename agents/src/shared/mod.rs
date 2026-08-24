//! What the two protocol modules would otherwise copy.
//!
//! Both MCP and A2A face the same problem: a request arrives over HTTP carrying headers, and
//! the work behind it is a gRPC call that wants metadata. Neither protocol has an opinion
//! about which headers cross that boundary, so the answer lives here once.

use std::collections::HashMap;
use std::sync::Arc;

use axum::Router;
use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;

/// Names one HTTP header to read and the gRPC metadata key to write it as.
///
/// Mappings are explicit rather than a blanket copy because the two namespaces are not the
/// same shape: gRPC reserves the `grpc-` prefix, keys are lowercase there and
/// case-insensitive here, and a proxy that forwarded everything would leak hop-by-hop
/// headers a backend has no business seeing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderMapping {
    /// The HTTP header to read, matched case-insensitively.
    pub http_header: String,
    /// The gRPC metadata key to write. Use lowercase.
    pub grpc_key: String,
}

impl HeaderMapping {
    /// One mapping, from an HTTP header name to a gRPC metadata key.
    pub fn new(http_header: impl Into<String>, grpc_key: impl Into<String>) -> Self {
        Self {
            http_header: http_header.into(),
            grpc_key: grpc_key.into(),
        }
    }
}

/// The three headers worth forwarding by default: the credential the backend authenticates
/// on, and the two ids that make a request traceable across the hop.
pub fn default_header_mappings() -> Vec<HeaderMapping> {
    vec![
        HeaderMapping::new("authorization", "authorization"),
        HeaderMapping::new("x-request-id", "x-request-id"),
        HeaderMapping::new("x-trace-id", "x-trace-id"),
    ]
}

/// The headers lifted off one request, waiting to become gRPC metadata.
///
/// Go stashes these on the request `context.Context` under an unexported key. A request
/// extension is the same idea with the same privacy: the type is what addresses it, so only
/// code that can name [`ForwardedHeaders`] can plant one.
#[derive(Debug, Clone, Default)]
pub struct ForwardedHeaders(pub HashMap<String, String>);

impl ForwardedHeaders {
    /// Whether anything was forwarded. A request with nothing to forward carries no
    /// extension at all, so a caller can tell "nothing to forward" from "forwarded nothing".
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Writes every forwarded header onto a gRPC metadata map.
    ///
    /// Keys the transport reserves are skipped: gRPC owns the `grpc-` prefix, and a value
    /// planted there would be rejected or, worse, believed.
    pub fn apply(&self, metadata: &mut tonic::metadata::MetadataMap) {
        for (key, value) in &self.0 {
            let key = key.to_ascii_lowercase();
            if key.starts_with("grpc-") {
                continue;
            }
            if let (Ok(name), Ok(value)) = (
                key.parse::<tonic::metadata::MetadataKey<_>>(),
                value.parse::<tonic::metadata::MetadataValue<_>>(),
            ) {
                metadata.insert(name, value);
            }
        }
    }
}

/// Wraps `router` so the mapped headers are lifted onto each request's extensions, where
/// [`ForwardedHeaders::apply`] finds them later.
///
/// It is two steps rather than one because the read and the write happen in different
/// places: the headers exist while the HTTP request is being served, and the gRPC call that
/// needs them is made further in, by a handler that never sees the request. With no mappings
/// the router is returned unwrapped, so the unconfigured case costs nothing.
pub fn with_header_forwarding(router: Router, mappings: Vec<HeaderMapping>) -> Router {
    if mappings.is_empty() {
        return router;
    }
    let mappings = Arc::new(mappings);
    router.layer(axum::middleware::from_fn(
        move |mut request: Request, next: Next| {
            let mappings = Arc::clone(&mappings);
            async move {
                let mut pairs = HashMap::with_capacity(mappings.len());
                for mapping in mappings.iter() {
                    if let Some(value) = request.headers().get(&mapping.http_header)
                        && let Ok(value) = value.to_str()
                        && !value.is_empty()
                    {
                        pairs.insert(mapping.grpc_key.clone(), value.to_string());
                    }
                }
                if !pairs.is_empty() {
                    request.extensions_mut().insert(ForwardedHeaders(pairs));
                }
                let response: Response = next.run(request).await;
                response
            }
        },
    ))
}
