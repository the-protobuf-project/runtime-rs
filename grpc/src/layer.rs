//! Caller-supplied [`tower`] layers around every gRPC service: the seam for middleware that
//! needs the request body itself — metering decoded bytes against a memory budget, gating on
//! a protocol version header — which neither interceptor sees.
//!
//! tonic states a server's layers in its type, so a list of them chosen at run time cannot be
//! a tonic layer stack. Each is instead applied to a boxed service, and the result boxed
//! again: one allocation per layer per call, which is the price of choosing them at run time.

use std::convert::Infallible;
use std::sync::Arc;

use tonic::body::Body;

/// Every gRPC service the server routes to, as a caller's layer sees it: requests in, with the
/// body still the framed gRPC stream the client sent, and responses out.
///
/// A layer passed to [`crate::HybridServer::with_grpc_layer`] wraps this type. `Clone`,
/// `Send` and `Sync`, and cheap to clone, as tonic clones its service per call.
pub type GrpcService =
    tower::util::BoxCloneSyncService<http::Request<Body>, http::Response<Body>, Infallible>;

/// One registered layer, type-erased.
pub(crate) type GrpcLayerFn = Arc<dyn Fn(GrpcService) -> GrpcService + Send + Sync>;

/// Erases `layer` into a [`GrpcLayerFn`].
pub(crate) fn erase<L>(layer: L) -> GrpcLayerFn
where
    L: tower::Layer<GrpcService> + Send + Sync + 'static,
    L::Service: tower::Service<http::Request<Body>, Response = http::Response<Body>, Error = Infallible>
        + Clone
        + Send
        + Sync
        + 'static,
    <L::Service as tower::Service<http::Request<Body>>>::Future: Send + 'static,
{
    Arc::new(move |inner| GrpcService::new(layer.layer(inner)))
}

/// The registered layers, in the order they were registered: the first is outermost.
#[derive(Clone, Default)]
pub(crate) struct GrpcLayers(pub(crate) Arc<[GrpcLayerFn]>);

impl<S> tower::Layer<S> for GrpcLayers
where
    S: tower::Service<http::Request<Body>, Response = http::Response<Body>, Error = Infallible>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    type Service = GrpcService;

    fn layer(&self, inner: S) -> GrpcService {
        self.0
            .iter()
            .rev()
            .fold(GrpcService::new(inner), |service, layer| layer(service))
    }
}
