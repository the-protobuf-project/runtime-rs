//! The in-process path to the gRPC services: Go's `bufconn`, without the socket.
//!
//! Go's gateway dials its own server over an in-memory listener, so a REST call is a gRPC
//! call in every way that matters — it runs the same interceptor chain, and the services
//! cannot tell the two apart. [`Loopback`] keeps that property and drops the dial: it is the
//! server's routes behind the same interceptor chain, as a [`tower::Service`] a generated
//! tonic client accepts in place of a channel.
//!
//! ```ignore
//! let client = BookServiceClient::new(loopback);
//! ```

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tonic::body::Body;
use tonic::metadata::MetadataMap;
use tonic::service::Routes;

use crate::intercept::InterceptLayer;
use crate::types::UnaryInterceptorFn;

/// The server's gRPC routes behind its interceptor chain, callable in-process.
///
/// Cheap to clone: a generated client clones its transport per call.
#[derive(Clone)]
pub struct Loopback {
    inner: tower::util::BoxCloneSyncService<http::Request<Body>, http::Response<Body>, Infallible>,
    intercepted: bool,
}

impl Loopback {
    pub(crate) fn new(
        routes: Routes,
        chain: Option<UnaryInterceptorFn>,
        messages: InterceptLayer,
    ) -> Self {
        use tower::Layer;
        let intercepted = chain.is_some();
        Self {
            inner: tower::util::BoxCloneSyncService::new(Chained::new(
                messages.layer(routes),
                chain,
            )),
            intercepted,
        }
    }
}

impl std::fmt::Debug for Loopback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Loopback")
            .field("intercepted", &self.intercepted)
            .finish_non_exhaustive()
    }
}

impl tower::Service<http::Request<Body>> for Loopback {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        tower::Service::<http::Request<Body>>::poll_ready(&mut self.inner, cx)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        self.inner.call(request)
    }
}

/// A service behind the server's unary interceptor chain.
///
/// The same wrapper sits in front of the TCP listener and the [`Loopback`], which is what
/// makes "a REST call runs the same interceptors as a gRPC one" true by construction rather
/// than by two registrations kept in step.
#[derive(Clone)]
pub(crate) struct Chained<S> {
    inner: S,
    chain: Option<UnaryInterceptorFn>,
}

impl<S> Chained<S> {
    pub(crate) fn new(inner: S, chain: Option<UnaryInterceptorFn>) -> Self {
        Self { inner, chain }
    }
}

impl<S, B> tower::Service<http::Request<B>> for Chained<S>
where
    S: tower::Service<http::Request<B>, Response = http::Response<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    B: Send + 'static,
{
    type Response = http::Response<Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let Some(chain) = &self.chain else {
            return Box::pin(self.inner.call(request));
        };

        let (mut parts, body) = request.into_parts();
        // The path names the method, and a request over the TCP listener carries nothing
        // else that does: tonic sets `GrpcMethod` only on a client's own requests, so only a
        // loopback call has one. Recorded here, before the chain runs, so every interceptor
        // can name the method either way.
        parts
            .extensions
            .insert(MethodPath(parts.uri.path().to_string()));
        let intercepted = tonic::Request::from_parts(
            MetadataMap::from_headers(std::mem::take(&mut parts.headers)),
            std::mem::take(&mut parts.extensions),
            (),
        );
        match chain(intercepted) {
            Ok(request) => {
                let (metadata, extensions, ()) = request.into_parts();
                parts.headers = metadata.into_headers();
                parts.extensions = extensions;
                Box::pin(self.inner.call(http::Request::from_parts(parts, body)))
            }
            // A rejection is a gRPC status on an ordinary response, as tonic's own
            // interceptors produce it — not a transport error.
            Err(status) => Box::pin(std::future::ready(Ok(status.into_http()))),
        }
    }
}

/// The full method path of a call, `/package.Service/Method`, as a request extension.
///
/// Set on every call before the metadata-level interceptors run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MethodPath(pub String);

/// Installs [`Chained`] in front of whatever tonic serves.
#[derive(Clone)]
pub(crate) struct ChainLayer(pub(crate) Option<UnaryInterceptorFn>);

impl<S> tower::Layer<S> for ChainLayer {
    type Service = Chained<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Chained::new(inner, self.0.clone())
    }
}
