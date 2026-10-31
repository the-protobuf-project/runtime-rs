//! Handing an HTTP caller's headers to in-process gRPC: Go's `IncomingHeaders` and its
//! `ForwardMetadata`.
//!
//! Go keeps both on the `context.Context` a handler runs with. Rust has no ambient context,
//! so the same data travels in a task-local [`CallContext`] for the length of one call. That
//! is what lets a hand-written implementation behind a generated handler — which is given
//! arguments and nothing else — still call a gRPC service as its caller: the hybrid server's
//! in-process `Loopback` reads the context as it sends, so the server's interceptor chain
//! sees the caller's `authorization` exactly as it would on a wire call.

use std::future::Future;

use http::HeaderMap;
use tonic::metadata::{KeyAndValueRef, MetadataKey, MetadataMap, MetadataValue};

use super::{ForwardedHeaders, HeaderMapping};

tokio::task_local! {
    static CALL_CONTEXT: CallContext;
}

/// What one call knows about its caller, while it runs.
#[derive(Debug, Clone, Default)]
pub struct CallContext {
    /// The caller's mapped headers as incoming gRPC metadata, which is where a gRPC server's
    /// interceptor reads credentials from.
    pub incoming: MetadataMap,
    /// The headers lifted off the HTTP request by [`super::with_header_forwarding`].
    pub forwarded: Option<ForwardedHeaders>,
}

/// Runs `future` with `context` as its [`current_call_context`].
///
/// The context is scoped to the future and does not follow a `tokio::spawn`; work moved onto
/// another task takes it along with another `with_call_context`.
pub async fn with_call_context<F: Future>(context: CallContext, future: F) -> F::Output {
    CALL_CONTEXT.scope(context, future).await
}

/// The context of the call this task is serving, if it is serving one.
pub fn current_call_context() -> Option<CallContext> {
    CALL_CONTEXT.try_with(Clone::clone).ok()
}

/// `existing` with the mapped headers of `headers` written over it — Go's `IncomingHeaders`.
///
/// Every mapped key is removed first and then set from `headers` alone, so a value an earlier
/// request on the same session left behind never reaches an interceptor as this caller's;
/// `None` (a request that did not arrive over HTTP) therefore clears them. Mappings onto the
/// `grpc-` prefix, which the transport owns, are ignored.
pub fn incoming_headers(
    existing: &MetadataMap,
    mappings: &[HeaderMapping],
    headers: Option<&HeaderMap>,
) -> MetadataMap {
    let mut metadata = existing.clone();
    for mapping in mappings {
        let key = mapping.grpc_key.to_ascii_lowercase();
        if key.is_empty() || key.starts_with("grpc-") {
            continue;
        }
        let Ok(name) = key.parse::<MetadataKey<_>>() else {
            continue;
        };
        metadata.remove(key.as_str());
        let Some(headers) = headers else {
            continue;
        };
        for value in headers.get_all(mapping.http_header.as_str()) {
            if let Ok(value) = MetadataValue::try_from(value.as_bytes()) {
                metadata.append(name.clone(), value);
            }
        }
    }
    metadata
}

/// The metadata to send onward, from two sources — Go's `ForwardMetadata`.
///
/// First the incoming metadata, for the gRPC-to-gRPC case, minus the `grpc-` keys the
/// transport owns; then the forwarded HTTP headers, which win a clash because they came from
/// the client that actually made this call. `None` means there was nothing to forward, so a
/// caller can tell that from having forwarded nothing.
pub fn forward_metadata(
    incoming: Option<&MetadataMap>,
    forwarded: Option<&ForwardedHeaders>,
) -> Option<MetadataMap> {
    let mut metadata = MetadataMap::new();
    if let Some(incoming) = incoming {
        for entry in incoming.iter() {
            match entry {
                KeyAndValueRef::Ascii(key, value) if !key.as_str().starts_with("grpc-") => {
                    metadata.append(key.clone(), value.clone());
                }
                KeyAndValueRef::Binary(key, value) if !key.as_str().starts_with("grpc-") => {
                    metadata.append_bin(key.clone(), value.clone());
                }
                _ => {}
            }
        }
    }
    if let Some(forwarded) = forwarded {
        forwarded.apply(&mut metadata);
    }
    (!metadata.is_empty()).then_some(metadata)
}

/// Writes the current call's forwarded metadata onto an outgoing request's headers.
///
/// Outside a call this does nothing, so a transport can call it unconditionally. A header the
/// request already carries under a forwarded key is replaced: code between the caller and the
/// service should not be able to shadow the caller's identity by accident.
pub fn apply_call_context(headers: &mut HeaderMap) {
    let Some(context) = current_call_context() else {
        return;
    };
    let Some(metadata) = forward_metadata(Some(&context.incoming), context.forwarded.as_ref())
    else {
        return;
    };
    let forwarded = metadata.into_headers();
    for key in forwarded.keys() {
        headers.remove(key);
    }
    for (key, value) in &forwarded {
        headers.append(key.clone(), value.clone());
    }
}
