//! Request validation, and the seam it plugs into.
//!
//! Go builds this on `buf.build/go/protovalidate`, which reads `buf.validate` rules out of
//! the generated descriptors and checks a message against them. Rust has no published
//! equivalent — the `protovalidate` crate name is reserved but holds no implementation — so
//! this crate supplies the position in the chain and the failure mapping, and the rules
//! engine is a [`Validator`] the caller provides.
//!
//! When a protovalidate crate does ship, it becomes one impl of this trait and nothing else
//! moves.

use std::sync::Arc;

/// Checks a request message against whatever rules it carries.
///
/// A violation must map to [`tonic::Code::InvalidArgument`], which is what mirrors AIP error
/// semantics and what a gateway then turns into a 400.
pub trait Validator: Send + Sync + 'static {
    /// Validates one decoded request message.
    ///
    /// `message` is the request as a `prost`-decodable byte payload is not yet available
    /// at this layer, so implementations that need the typed message should register a
    /// [`tower`] layer instead; this seam exists for metadata- and descriptor-driven checks.
    fn validate(
        &self,
        method: &str,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<(), tonic::Status>;
}

impl<F> Validator for F
where
    F: Fn(&str, &tonic::metadata::MetadataMap) -> Result<(), tonic::Status> + Send + Sync + 'static,
{
    fn validate(
        &self,
        method: &str,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<(), tonic::Status> {
        self(method, metadata)
    }
}

/// Wraps a [`Validator`] into the interceptor shape the server chains.
///
/// It runs before the caller-supplied interceptors, so validation always goes first.
pub fn validation_interceptor(validator: Arc<dyn Validator>) -> crate::types::UnaryInterceptorFn {
    Arc::new(move |request: tonic::Request<()>| {
        // A loopback call carries the method as `GrpcMethod`, set by the client that made
        // it. A call over the TCP listener has only its path, which the chain recorded.
        let method = request
            .extensions()
            .get::<tonic::GrpcMethod<'static>>()
            .map(|m| format!("/{}/{}", m.service(), m.method()))
            .or_else(|| {
                request
                    .extensions()
                    .get::<crate::loopback::MethodPath>()
                    .map(|path| path.0.clone())
            })
            .unwrap_or_default();
        validator.validate(&method, request.metadata())?;
        Ok(request)
    })
}

/// Chains interceptors into one, applied in the order given.
///
/// Go's `grpc.ChainUnaryInterceptor` does this at the server; tonic takes a single
/// interceptor, so the chaining happens here instead.
pub fn chain(
    interceptors: Vec<crate::types::UnaryInterceptorFn>,
) -> crate::types::UnaryInterceptorFn {
    Arc::new(move |mut request: tonic::Request<()>| {
        for interceptor in &interceptors {
            request = interceptor(request)?;
        }
        Ok(request)
    })
}
