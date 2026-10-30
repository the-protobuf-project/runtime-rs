//! Interceptors that see the message, not just its metadata.
//!
//! Go's `grpc.UnaryServerInterceptor` receives the request as `any` and the handler's reply
//! as `any`, which is what lets validation, problem details, idempotency and hints work at
//! all: each needs a field of the request or the shape of the response. tonic's own
//! interceptors see only metadata, so this module supplies the Go shape on top of a tower
//! layer.
//!
//! The message arrives as bytes and is decoded on demand — into a
//! [`prost_reflect::DynamicMessage`] against the method's descriptor, or into a concrete
//! prost type when the interceptor knows it. An interceptor that keys on the method alone
//! never pays for a decode.
//!
//! # Descriptors
//!
//! Go finds a method's descriptor in the global registry. Rust has none, so the descriptor
//! sets come from [`HybridServer::with_reflection`] — the same sets reflection serves. A
//! method whose descriptor was not registered is still intercepted; only
//! [`UnaryCall::message`] fails for it, because there is nothing to decode against. What a
//! missing descriptor does cost is knowing whether the method streams, so such a call is
//! treated as unary only when its request arrives as exactly one message.
//!
//! [`HybridServer::with_reflection`]: crate::HybridServer::with_reflection

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures_util::future::BoxFuture;
use http_body_util::BodyExt;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, MethodDescriptor};
use tonic::Status;
use tonic::body::Body;
use tonic::metadata::MetadataMap;

/// A unary call in flight, as an interceptor sees it.
#[derive(Debug)]
pub struct UnaryCall {
    path: String,
    /// The request metadata. An interceptor may add to it or rewrite it; what is here when
    /// the call reaches the service is what the service sees.
    pub metadata: MetadataMap,
    /// Values for the interceptors after this one, and for the service.
    pub extensions: http::Extensions,
    payload: Bytes,
    compressed: bool,
    descriptor: Option<MethodDescriptor>,
}

impl UnaryCall {
    /// The full method path, `/package.Service/Method` — Go's `info.FullMethod`.
    #[must_use]
    pub fn full_method(&self) -> &str {
        &self.path
    }

    /// The fully-qualified service name.
    #[must_use]
    pub fn service(&self) -> &str {
        split_path(&self.path).0
    }

    /// The bare method name.
    #[must_use]
    pub fn method(&self) -> &str {
        split_path(&self.path).1
    }

    /// The method's descriptor, when its descriptor set was registered.
    #[must_use]
    pub fn descriptor(&self) -> Option<&MethodDescriptor> {
        self.descriptor.as_ref()
    }

    /// The encoded request message.
    #[must_use]
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    /// The request, decoded against the method's input descriptor.
    ///
    /// # Errors
    ///
    /// `INTERNAL` when the method's descriptor was not registered or the message is
    /// compressed, `INVALID_ARGUMENT` when it does not decode.
    pub fn message(&self) -> Result<DynamicMessage, Status> {
        let descriptor = self
            .descriptor
            .as_ref()
            .map(MethodDescriptor::input)
            .ok_or_else(|| missing_descriptor(&self.path))?;
        decode_dynamic(descriptor, &self.payload, self.compressed)
    }

    /// The request, decoded into a concrete prost type.
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT` when it does not decode, `INTERNAL` when it is compressed.
    pub fn decode<M: prost::Message + Default>(&self) -> Result<M, Status> {
        decode_typed(&self.payload, self.compressed)
    }

    /// Replaces the request the service will receive.
    pub fn set_message(&mut self, message: &impl prost::Message) {
        self.payload = Bytes::from(message.encode_to_vec());
        self.compressed = false;
    }
}

/// A unary call's successful reply, as an interceptor sees it on the way back out.
#[derive(Debug)]
pub struct UnaryReply {
    /// The response headers.
    pub metadata: MetadataMap,
    /// The response trailers, including the `grpc-status` the service ended with.
    pub trailers: MetadataMap,
    payload: Bytes,
    compressed: bool,
    descriptor: Option<MessageDescriptor>,
}

impl UnaryReply {
    /// The encoded response message.
    #[must_use]
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    /// The response, decoded against the method's output descriptor.
    ///
    /// # Errors
    ///
    /// `INTERNAL` when the descriptor was not registered, the message is compressed, or it
    /// does not decode — a service's own reply failing to decode is the server's fault.
    pub fn message(&self) -> Result<DynamicMessage, Status> {
        let descriptor = self.descriptor.clone().ok_or_else(|| {
            Status::internal("the method's response descriptor was not registered")
        })?;
        decode_dynamic(descriptor, &self.payload, self.compressed)
            .map_err(|status| Status::internal(status.message().to_string()))
    }

    /// The response, decoded into a concrete prost type.
    ///
    /// # Errors
    ///
    /// `INTERNAL` when it does not decode.
    pub fn decode<M: prost::Message + Default>(&self) -> Result<M, Status> {
        decode_typed(&self.payload, self.compressed)
            .map_err(|status| Status::internal(status.message().to_string()))
    }

    /// Replaces the response the caller will receive.
    pub fn set_message(&mut self, message: &impl prost::Message) {
        self.payload = Bytes::from(message.encode_to_vec());
        self.compressed = false;
    }
}

/// Runs a unary call with access to its request and reply. Go's
/// `grpc.UnaryServerInterceptor`.
///
/// ```ignore
/// #[async_trait::async_trait]
/// impl Interceptor for Audit {
///     async fn intercept(&self, call: UnaryCall, next: Next<'_>) -> Result<UnaryReply, Status> {
///         let request = call.message()?;
///         tracing::info!(method = call.full_method(), fields = request.fields().count());
///         next.run(call).await
///     }
/// }
/// ```
#[async_trait::async_trait]
pub trait Interceptor: Send + Sync + 'static {
    /// Handles one call: inspect or rewrite it, then either answer it with an error or pass
    /// it on with [`Next::run`] and inspect or rewrite the reply.
    ///
    /// Returning `Err` is how an interceptor rejects; the status reaches a gRPC caller as
    /// is, and a REST caller as the gateway's rendering of it.
    async fn intercept(&self, call: UnaryCall, next: Next<'_>) -> Result<UnaryReply, Status>;
}

type Terminal = Box<dyn FnOnce(UnaryCall) -> BoxFuture<'static, Result<UnaryReply, Status>> + Send>;

/// The rest of the chain, ending at the service.
pub struct Next<'a> {
    rest: &'a [Arc<dyn Interceptor>],
    terminal: Terminal,
}

impl std::fmt::Debug for Next<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Next")
            .field("remaining", &self.rest.len())
            .finish()
    }
}

impl Next<'_> {
    /// Passes the call to the next interceptor, or to the service after the last.
    ///
    /// # Errors
    ///
    /// Whatever the rest of the chain or the service ends the call with.
    pub async fn run(self, call: UnaryCall) -> Result<UnaryReply, Status> {
        match self.rest.split_first() {
            Some((first, rest)) => {
                first
                    .intercept(
                        call,
                        Next {
                            rest,
                            terminal: self.terminal,
                        },
                    )
                    .await
            }
            None => (self.terminal)(call).await,
        }
    }
}

/// Installs [`Intercepted`] in front of whatever tonic serves.
#[derive(Clone)]
pub(crate) struct InterceptLayer {
    chain: Arc<[Arc<dyn Interceptor>]>,
    pool: DescriptorPool,
}

impl InterceptLayer {
    pub(crate) fn new(chain: Vec<Arc<dyn Interceptor>>, pool: DescriptorPool) -> Self {
        Self {
            chain: chain.into(),
            pool,
        }
    }
}

impl<S> tower::Layer<S> for InterceptLayer {
    type Service = Intercepted<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Intercepted {
            inner,
            chain: Arc::clone(&self.chain),
            pool: self.pool.clone(),
        }
    }
}

/// A service behind the message-level interceptor chain.
#[derive(Clone)]
pub(crate) struct Intercepted<S> {
    inner: S,
    chain: Arc<[Arc<dyn Interceptor>]>,
    pool: DescriptorPool,
}

impl<S> tower::Service<http::Request<Body>> for Intercepted<S>
where
    S: tower::Service<http::Request<Body>, Response = http::Response<Body>, Error = Infallible>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        if self.chain.is_empty() {
            return Box::pin(self.inner.call(request));
        }

        let path = request.uri().path().to_string();
        let descriptor = lookup(&self.pool, &path);
        // A streaming method passes through untouched: there is no single message to hand
        // an interceptor, and buffering a stream would break it. Stream interceptors are a
        // separate seam.
        if descriptor
            .as_ref()
            .is_some_and(|m| m.is_client_streaming() || m.is_server_streaming())
        {
            return Box::pin(self.inner.call(request));
        }

        let inner = self.inner.clone();
        let chain = Arc::clone(&self.chain);
        Box::pin(async move { Ok(intercept(inner, chain, path, descriptor, request).await) })
    }
}

/// Buffers a unary call, runs the chain over it, and renders what the chain decided.
async fn intercept<S>(
    inner: S,
    chain: Arc<[Arc<dyn Interceptor>]>,
    path: String,
    descriptor: Option<MethodDescriptor>,
    request: http::Request<Body>,
) -> http::Response<Body>
where
    S: tower::Service<http::Request<Body>, Response = http::Response<Body>, Error = Infallible>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    let (mut parts, body) = request.into_parts();
    let bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(status) => return status.into_http(),
    };

    // Not exactly one message: a client stream whose descriptor was not registered, or
    // something malformed. Either way it is not ours to interpret, so it goes on as sent.
    let Some((compressed, payload)) = unframe(&bytes) else {
        let mut inner = inner;
        let request = http::Request::from_parts(parts, Body::new(http_body_util::Full::new(bytes)));
        return match inner.call(request).await {
            Ok(response) => response,
            Err(never) => match never {},
        };
    };

    let output = descriptor.as_ref().map(MethodDescriptor::output);
    let call = UnaryCall {
        path,
        metadata: MetadataMap::from_headers(std::mem::take(&mut parts.headers)),
        extensions: std::mem::take(&mut parts.extensions),
        payload,
        compressed,
        descriptor,
    };

    let terminal: Terminal = Box::new(move |call: UnaryCall| {
        Box::pin(forward(inner, parts, call, output)) as BoxFuture<'static, _>
    });
    let outcome = Next {
        rest: &chain,
        terminal,
    }
    .run(call)
    .await;

    match outcome {
        Ok(reply) => render(reply),
        Err(status) => status.into_http(),
    }
}

/// Sends the call on to the service and reads its reply back into a [`UnaryReply`].
async fn forward<S>(
    mut inner: S,
    mut parts: http::request::Parts,
    call: UnaryCall,
    output: Option<MessageDescriptor>,
) -> Result<UnaryReply, Status>
where
    S: tower::Service<http::Request<Body>, Response = http::Response<Body>, Error = Infallible>,
{
    parts.headers = call.metadata.into_headers();
    parts.extensions = call.extensions;
    let body = Body::new(http_body_util::Full::new(frame(
        call.compressed,
        &call.payload,
    )));
    let response = match inner.call(http::Request::from_parts(parts, body)).await {
        Ok(response) => response,
        Err(never) => match never {},
    };

    let (head, body) = response.into_parts();
    // A trailers-only response carries its status in the headers.
    if let Some(status) = Status::from_header_map(&head.headers)
        && status.code() != tonic::Code::Ok
    {
        return Err(status);
    }

    let collected = body.collect().await?;
    let trailers = collected.trailers().cloned().unwrap_or_default();
    if let Some(status) = Status::from_header_map(&trailers)
        && status.code() != tonic::Code::Ok
    {
        return Err(status);
    }

    let bytes = collected.to_bytes();
    let (compressed, payload) = unframe(&bytes)
        .ok_or_else(|| Status::internal("the service's reply was not one gRPC message"))?;
    Ok(UnaryReply {
        metadata: MetadataMap::from_headers(head.headers),
        trailers: MetadataMap::from_headers(trailers),
        payload,
        compressed,
        descriptor: output,
    })
}

/// Renders a reply as the gRPC response it stands for: headers, one framed message, then
/// the trailers.
fn render(reply: UnaryReply) -> http::Response<Body> {
    let mut trailers = reply.trailers.into_headers();
    if !trailers.contains_key("grpc-status") {
        trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
    }
    let frames = futures_util::stream::iter([
        Ok::<_, Status>(http_body::Frame::data(frame(
            reply.compressed,
            &reply.payload,
        ))),
        Ok(http_body::Frame::trailers(trailers)),
    ]);
    let mut response = http::Response::new(Body::new(http_body_util::StreamBody::new(frames)));
    *response.headers_mut() = reply.metadata.into_headers();
    response
}

/// Prefixes a message with the gRPC length-prefixed framing.
fn frame(compressed: bool, payload: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(payload.len() + 5);
    out.put_u8(u8::from(compressed));
    out.put_u32(u32::try_from(payload.len()).unwrap_or(u32::MAX));
    out.put_slice(payload);
    out.freeze()
}

/// Splits exactly one framed message off a body; `None` for anything else.
fn unframe(bytes: &Bytes) -> Option<(bool, Bytes)> {
    let mut buf = bytes.clone();
    if buf.remaining() < 5 {
        return None;
    }
    let compressed = match buf.get_u8() {
        0 => false,
        1 => true,
        _ => return None,
    };
    let len = usize::try_from(buf.get_u32()).ok()?;
    (buf.remaining() == len).then_some((compressed, buf))
}

fn lookup(pool: &DescriptorPool, path: &str) -> Option<MethodDescriptor> {
    let (service, method) = split_path(path);
    pool.get_service_by_name(service)?
        .methods()
        .find(|m| m.name() == method)
}

fn split_path(path: &str) -> (&str, &str) {
    path.trim_start_matches('/')
        .split_once('/')
        .unwrap_or((path, ""))
}

fn missing_descriptor(path: &str) -> Status {
    Status::internal(format!(
        "no descriptor is registered for {path}; register its descriptor set with \
         HybridServer::with_reflection"
    ))
}

fn decode_dynamic(
    descriptor: MessageDescriptor,
    payload: &Bytes,
    compressed: bool,
) -> Result<DynamicMessage, Status> {
    if compressed {
        return Err(Status::internal("a compressed message cannot be inspected"));
    }
    DynamicMessage::decode(descriptor, payload.clone())
        .map_err(|err| Status::invalid_argument(format!("the message does not decode: {err}")))
}

fn decode_typed<M: prost::Message + Default>(
    payload: &Bytes,
    compressed: bool,
) -> Result<M, Status> {
    if compressed {
        return Err(Status::internal("a compressed message cannot be inspected"));
    }
    M::decode(payload.clone())
        .map_err(|err| Status::invalid_argument(format!("the message does not decode: {err}")))
}

/// Builds the pool interceptors decode against from the registered descriptor sets.
///
/// A set that does not decode is logged and skipped rather than failing the start: it is
/// reflection's input too, and a bad one should cost [`UnaryCall::message`], not the server.
pub(crate) fn descriptor_pool(sets: &[&'static [u8]]) -> DescriptorPool {
    let mut pool = DescriptorPool::new();
    for set in sets {
        if let Err(err) = pool.decode_file_descriptor_set(*set) {
            tracing::warn!(error = %err, "gRPC: a descriptor set did not decode; skipping it");
        }
    }
    pool
}
