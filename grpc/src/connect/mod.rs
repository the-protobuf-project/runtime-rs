//! The Connect protocol's unary calls, served beside the REST surface.
//!
//! A Connect client — `connect-go`, `connect-es`, the TypeScript runtime's `connectTransport`
//! — posts one message to `/<package.Service>/<Method>` as `application/proto` or
//! `application/json`. [`ConnectBridge`] turns that into a gRPC call on the server's own
//! [`Loopback`], so it runs the same interceptor chain as a gRPC client, and turns the reply
//! or the `grpc-status` back into a Connect response. Unary calls first; server, client
//! and bidi streaming follow.
//!
//! A binary call needs no type information: the Connect body is exactly the message a gRPC
//! frame carries. It therefore reaches every unary method, with or without a
//! `google.api.http` binding. A JSON call is converted with `prost-reflect` against the
//! descriptor sets registered with [`HybridServer::with_reflection`]; a method they do not
//! describe answers `unimplemented` for JSON and still works in binary.
//!
//! This mirrors Go's `WithConnect`, and the two answer alike: the same error codes, HTTP
//! statuses and details encoding.
//!
//! [`HybridServer::with_reflection`]: crate::HybridServer::with_reflection

mod error;
mod wire;

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::extract::Request;
use axum::response::Response;
use prost_reflect::DescriptorPool;
use tonic::Code;

use crate::loopback::Loopback;
use error::{connect_error, unsupported_media_type};
use wire::Format;

/// Serves Connect unary calls by calling the gRPC services through the [`Loopback`].
#[derive(Clone)]
pub(crate) struct ConnectBridge {
    loopback: Loopback,
    pool: Arc<DescriptorPool>,
}

impl ConnectBridge {
    pub(crate) fn new(loopback: Loopback, pool: DescriptorPool) -> Self {
        Self {
            loopback,
            pool: Arc::new(pool),
        }
    }

    /// Wraps `next`, sending Connect calls here and everything else on.
    pub(crate) fn route(self, next: axum::Router) -> axum::Router {
        axum::Router::new().fallback_service(ConnectRouting { bridge: self, next })
    }

    async fn serve(self, request: Request) -> Response {
        let (parts, body) = request.into_parts();
        let content_type = parts
            .headers
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        let Some(format) = Format::from_content_type(content_type) else {
            return unsupported_media_type();
        };
        if let Some(encoding) = parts.headers.get(http::header::CONTENT_ENCODING)
            && encoding.as_bytes() != b"identity"
        {
            return connect_error(
                Code::Unimplemented,
                "content encoding is not supported",
                &[],
                None,
            );
        }
        let Ok(body) = axum::body::to_bytes(body, crate::http::MAX_REQUEST_BODY).await else {
            let message = format!(
                "request body exceeds {} bytes",
                crate::http::MAX_REQUEST_BODY
            );
            return connect_error(Code::ResourceExhausted, &message, &[], None);
        };
        let path = parts.uri.path().to_owned();
        let request = format
            .request(&self.pool, &path, body)
            .and_then(|message| wire::grpc_request(&path, &parts.headers, message));
        let request = match request {
            Ok(request) => request,
            Err(status) => return connect_error(status.code(), status.message(), &[], None),
        };
        let reply = wire::call(self.loopback, request).await;
        match reply.status {
            Some(status) if status.code() != Code::Ok => connect_error(
                status.code(),
                status.message(),
                status.details(),
                Some(&reply.metadata),
            ),
            _ => match format.response(&self.pool, &path, reply.message) {
                Ok(body) => wire::connect_response(format, body, &reply.metadata),
                Err(status) => {
                    connect_error(status.code(), status.message(), &[], Some(&reply.metadata))
                }
            },
        }
    }
}

/// Whether `path` names a method, `/pkg.Service/Method`: a Connect path's first segment is
/// a fully qualified service name, so it contains a dot, which a REST route's does not.
pub(crate) fn is_connect_path(path: &str) -> bool {
    let Some((service, method)) = path.trim_start_matches('/').split_once('/') else {
        return false;
    };
    service.contains('.') && !method.is_empty() && !method.contains('/')
}

/// Sends `POST /pkg.Service/Method` to the bridge and everything else to `next`.
#[derive(Clone)]
struct ConnectRouting {
    bridge: ConnectBridge,
    next: axum::Router,
}

impl tower::Service<Request> for ConnectRouting {
    type Response = Response;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        if request.method() == http::Method::POST && is_connect_path(request.uri().path()) {
            let bridge = self.bridge.clone();
            return Box::pin(async move { Ok(bridge.serve(request).await) });
        }
        let mut next = self.next.clone();
        Box::pin(async move { tower::Service::call(&mut next, request).await })
    }
}

#[cfg(test)]
mod tests {
    use super::is_connect_path;

    #[test]
    fn connect_paths_are_told_from_rest_ones() {
        assert!(is_connect_path("/grpc.health.v1.Health/Check"));
        for rest in [
            "/v1/shelves/1",
            "/grpc.health.v1.Health",
            "/a.B/c/d",
            "/health/x",
            "/",
        ] {
            assert!(!is_connect_path(rest), "{rest}");
        }
    }
}
