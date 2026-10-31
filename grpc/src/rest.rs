//! The REST surface: `transcode` gateways over the [`Loopback`].
//!
//! Go mounts grpc-gateway's `ServeMux`, which dials the gRPC server. Here a gateway is a
//! [`transcode::Gateway`] whose dispatcher calls the services through the [`Loopback`], so
//! nothing is dialled and nothing is re-encoded onto a socket.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::extract::Request;
use axum::response::{IntoResponse, Response};
use transcode::route::Resolution;

use crate::loopback::Loopback;

/// A REST gateway the server can mount: anything that can say which requests are its own
/// and serve them.
///
/// Implemented for every [`transcode::Gateway`], which is the only implementation most
/// servers need.
pub trait RestGateway: Send + Sync + 'static {
    /// Whether a request's method and path resolve against this gateway's routes — matched,
    /// or matched on the path with the wrong method, which is still this gateway's `405`.
    fn claims(&self, method: &http::Method, path: &str) -> bool;

    /// Serves one request.
    fn serve(&self, request: Request) -> Pin<Box<dyn Future<Output = Response> + Send>>;
}

impl<D: transcode::Dispatcher> RestGateway for transcode::Gateway<D> {
    fn claims(&self, method: &http::Method, path: &str) -> bool {
        !matches!(
            self.table().resolve(method.as_str(), path),
            Resolution::NotFound
        )
    }

    fn serve(&self, request: Request) -> Pin<Box<dyn Future<Output = Response> + Send>> {
        let gateway = self.clone();
        Box::pin(async move { gateway.handle(request).await.map(axum::body::Body::new) })
    }
}

/// Builds a gateway over the loopback. An `Fn`, not an `FnOnce`, because a restart builds
/// it again over the new server's routes.
pub type GatewayFn = Arc<dyn Fn(Loopback) -> Arc<dyn RestGateway> + Send + Sync>;

/// Every mounted gateway as one service.
///
/// A request goes to the first gateway whose routes claim it. One no gateway claims goes to
/// the first gateway anyway, so an unmatched path still gets the AIP-193 `404` envelope
/// rather than a bare one — every failure leaving through the same funnel is the point.
#[derive(Clone)]
pub(crate) struct RestSurface {
    gateways: Arc<Vec<Arc<dyn RestGateway>>>,
}

impl RestSurface {
    pub(crate) fn new(gateways: Vec<Arc<dyn RestGateway>>) -> Self {
        Self {
            gateways: Arc::new(gateways),
        }
    }
}

impl tower::Service<Request> for RestSurface {
    type Response = Response;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        let chosen = self
            .gateways
            .iter()
            .find(|gateway| gateway.claims(request.method(), request.uri().path()))
            .or_else(|| self.gateways.first())
            .cloned();
        Box::pin(async move {
            Ok(match chosen {
                Some(gateway) => gateway.serve(request).await,
                None => http::StatusCode::NOT_FOUND.into_response(),
            })
        })
    }
}
