//! Request validation, and the seam it plugs into.
//!
//! Go builds this on `buf.build/go/protovalidate`, which reads `buf.validate` rules out of
//! the generated descriptors and checks a message against them. The Rust engine is
//! [`prost_protovalidate`], which does the same against a [`prost_reflect`] message —
//! including CEL rules — so validation needs no generated code: the request is decoded
//! against the descriptor sets registered with [`HybridServer::with_reflection`] and
//! handed to the engine as is. That is [`ProtoValidator`], what
//! [`HybridServer::with_validation`] runs.
//!
//! The engine stays behind the [`Validator`] trait, so a caller can supply another one with
//! [`HybridServer::with_validator`] — a hand-written check, or a metadata rule — and still
//! get validation's place in the chain.
//!
//! # Where it runs
//!
//! Validation is a message-level [`Interceptor`], [`ValidationInterceptor`], and it runs
//! before every interceptor registered with [`HybridServer::with_interceptor`], for gRPC
//! and REST callers alike. Go reaches the same order by prepending it to the unary chain.
//!
//! One difference: Go finds every message's descriptor in its global registry, so it can
//! validate every request. A method whose descriptor set was not registered cannot be
//! decoded here, so the default engine lets it through with a debug log rather than
//! failing a call it cannot read.
//!
//! [`HybridServer::with_reflection`]: crate::HybridServer::with_reflection
//! [`HybridServer::with_validation`]: crate::HybridServer::with_validation
//! [`HybridServer::with_validator`]: crate::HybridServer::with_validator
//! [`HybridServer::with_interceptor`]: crate::HybridServer::with_interceptor

use std::convert::Infallible;
use std::sync::Arc;

use tonic::Status;

use crate::hybrid::HybridServer;
use crate::intercept::{InterceptLayer, Intercepted, Interceptor, Next, UnaryCall, UnaryReply};

/// Checks a request against whatever rules it carries.
///
/// A violation must map to [`tonic::Code::InvalidArgument`], which is what mirrors AIP error
/// semantics and what a gateway then turns into a 400.
pub trait Validator: Send + Sync + 'static {
    /// Validates one unary call: its method, its metadata, and — through
    /// [`UnaryCall::message`] — the decoded request.
    ///
    /// # Errors
    ///
    /// The status the call is rejected with; it reaches the caller as is.
    fn validate(&self, call: &UnaryCall) -> Result<(), Status>;
}

impl<F> Validator for F
where
    F: Fn(&UnaryCall) -> Result<(), Status> + Send + Sync + 'static,
{
    fn validate(&self, call: &UnaryCall) -> Result<(), Status> {
        self(call)
    }
}

/// The default engine: the request's `buf.validate` rules, checked by
/// [`prost_protovalidate`]. Go's `protovalidate.New()`.
///
/// Its compiled rules are cached per message type, so build it once and share it.
pub struct ProtoValidator {
    engine: prost_protovalidate::Validator,
}

impl ProtoValidator {
    /// A validator with the engine's default options.
    #[must_use]
    pub fn new() -> Self {
        Self::with_engine(prost_protovalidate::Validator::new())
    }

    /// A validator over an engine the caller configured — fail-fast, a filter, a clock.
    #[must_use]
    pub fn with_engine(engine: prost_protovalidate::Validator) -> Self {
        Self { engine }
    }
}

impl Default for ProtoValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ProtoValidator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProtoValidator").finish_non_exhaustive()
    }
}

impl Validator for ProtoValidator {
    fn validate(&self, call: &UnaryCall) -> Result<(), Status> {
        if call.descriptor().is_none() {
            tracing::debug!(
                method = call.full_method(),
                "validation: no descriptor registered for the method — skipping"
            );
            return Ok(());
        }
        let message = call.message()?;
        self.engine
            .validate(&message)
            .map_err(|err| crate::validation_hints::validation_status(&err))
    }
}

/// The message-level interceptor that runs a [`Validator`] before the handler. Go's
/// `NewValidationInterceptor`.
///
/// It is a value rather than something only the server builds, so an in-process test
/// harness can chain exactly what a production server runs; see [`standalone_layer`].
#[derive(Clone)]
pub struct ValidationInterceptor {
    validator: Arc<dyn Validator>,
}

impl ValidationInterceptor {
    /// Runs `validator` on every unary call.
    #[must_use]
    pub fn new(validator: Arc<dyn Validator>) -> Self {
        Self { validator }
    }
}

impl Default for ValidationInterceptor {
    /// Runs the default engine, [`ProtoValidator`].
    fn default() -> Self {
        Self::new(Arc::new(ProtoValidator::new()))
    }
}

impl std::fmt::Debug for ValidationInterceptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValidationInterceptor")
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl Interceptor for ValidationInterceptor {
    async fn intercept(&self, call: UnaryCall, next: Next<'_>) -> Result<UnaryReply, Status> {
        self.validator.validate(&call)?;
        next.run(call).await
    }
}

impl HybridServer {
    /// The validation interceptor this server runs, or `None` when validation is off.
    ///
    /// Resolved at start, from [`HybridServer::with_validator`]'s engine or else the
    /// default one.
    pub(crate) fn validation(&self) -> Option<Arc<dyn Interceptor>> {
        if !self.enable_validation {
            return None;
        }
        let interceptor = match &self.validator {
            Some(validator) => ValidationInterceptor::new(Arc::clone(validator)),
            None => ValidationInterceptor::default(),
        };
        Some(Arc::new(interceptor))
    }

    /// The message-level chain: validation first, then the caller's interceptors in the
    /// order they were chained — or validation last, after them, with
    /// [`HybridServer::with_validation_last`].
    pub(crate) fn message_chain(&self) -> Vec<Arc<dyn Interceptor>> {
        let mut chain: Vec<_> = self.message_interceptors.iter().cloned().collect();
        if let Some(validation) = self.validation() {
            if self.validation_last {
                chain.push(validation);
            } else {
                chain.insert(0, validation);
            }
        }
        chain
    }
}

/// A tower layer carrying the same validation-first chain a [`HybridServer`] runs with
/// [`HybridServer::with_validation`], for a plain tonic server. Go's
/// `NewStandaloneGRPCServer`.
///
/// It is the seam for an in-process test harness that needs production-identical
/// middleware without the hybrid transports:
///
/// ```ignore
/// tonic::transport::Server::builder()
///     .layer(grpc::validation::standalone_layer(&[pb::FILE_DESCRIPTOR_SET], vec![]))
///     .add_service(pb::shop_server::ShopServer::new(shop))
/// ```
///
/// `descriptor_sets` are what requests are decoded against, as
/// [`HybridServer::with_reflection`] takes them; `extra` run after validation, in order.
pub fn standalone_layer(
    descriptor_sets: &[&'static [u8]],
    extra: Vec<Arc<dyn Interceptor>>,
) -> StandaloneLayer {
    let mut chain: Vec<Arc<dyn Interceptor>> = vec![Arc::new(ValidationInterceptor::default())];
    chain.extend(extra);
    StandaloneLayer(InterceptLayer::new(
        chain,
        crate::intercept::descriptor_pool(descriptor_sets),
    ))
}

/// The layer [`standalone_layer`] builds.
#[derive(Clone)]
pub struct StandaloneLayer(InterceptLayer);

impl<S> tower::Layer<S> for StandaloneLayer {
    type Service = Standalone<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Standalone(tower::Layer::layer(&self.0, inner))
    }
}

/// A service behind [`StandaloneLayer`]'s chain.
#[derive(Clone)]
pub struct Standalone<S>(Intercepted<S>);

impl<S> tower::Service<http::Request<tonic::body::Body>> for Standalone<S>
where
    S: tower::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
            Error = Infallible,
        > + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Infallible>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        tower::Service::poll_ready(&mut self.0, cx)
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        tower::Service::call(&mut self.0, request)
    }
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
