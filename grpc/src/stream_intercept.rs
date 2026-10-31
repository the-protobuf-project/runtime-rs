//! Interceptors for streaming calls. Go's `WithStreamInterceptors`.
//!
//! Go's `grpc.StreamServerInterceptor` wraps the handler: it can refuse the call, and the
//! handler returning is the stream ending. [`StreamInterceptor`] keeps that shape —
//! [`StreamNext::run`] resolves once the stream is over, with the status it ended on, and
//! what the interceptor returns is the status the caller receives.
//!
//! # How that works without a blocking handler
//!
//! A tonic handler returns its response at once and the stream runs inside the body, so the
//! chain cannot simply be the response future. Each call's chain runs on a task of its own
//! instead. The end of the chain hands the response to the caller and then waits for the body
//! to finish, and the body holds its trailers back until the chain has decided.
//!
//! # Differences from Go
//!
//! An interceptor sees the call, not each message: Go's `ServerStream` wrapper has no
//! counterpart here, because a message is bytes the service has not asked for yet. Wrap the
//! service in a [`tower`] layer for per-message work.
//!
//! Without a registered descriptor set a method's shape is unknown, so the call is treated as
//! streaming and the chain runs: running a guard too often is the safe way to be wrong. Unary
//! methods with no descriptor therefore run this chain as well as the unary ones. Register
//! the descriptor sets with [`HybridServer::with_reflection`] to make the split exact.
//!
//! A span entered around [`StreamNext::run`] covers the call, but is not current while the
//! service produces messages, since the transport polls the body and not this task.
//!
//! As in Go, MCP tool dispatch is unary only and does not run these.
//!
//! [`HybridServer::with_reflection`]: crate::HybridServer::with_reflection

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, ready};

use bytes::Bytes;
use futures_util::future::BoxFuture;
use http::HeaderMap;
use http_body::Frame;
use opentelemetry::context::FutureExt as _;
use prost_reflect::{DescriptorPool, MethodDescriptor};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tonic::body::Body;
use tonic::metadata::MetadataMap;
use tracing::Instrument as _;

use crate::hybrid::HybridServer;
use crate::intercept::{lookup, split_path};

/// How a stream ended, or how an interceptor says it did.
type Outcome = Result<(), Status>;
type Reply = http::Response<Body>;
type ReplySlot = Arc<Mutex<Option<oneshot::Sender<Reply>>>>;

/// A streaming call in flight, as a [`StreamInterceptor`] sees it.
#[derive(Debug)]
pub struct StreamCall {
    path: String,
    /// The request metadata. An interceptor may add to it or rewrite it; what is here when
    /// the call reaches the service is what the service sees.
    pub metadata: MetadataMap,
    /// Values for the interceptors after this one, and for the service.
    pub extensions: http::Extensions,
    descriptor: Option<MethodDescriptor>,
}

impl StreamCall {
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

    /// The method's descriptor, when its descriptor set was registered. It says which sides
    /// stream, which is Go's `info.IsClientStream` and `info.IsServerStream`.
    #[must_use]
    pub fn descriptor(&self) -> Option<&MethodDescriptor> {
        self.descriptor.as_ref()
    }
}

/// Runs around a streaming call. Go's `grpc.StreamServerInterceptor`.
///
/// ```ignore
/// #[async_trait::async_trait]
/// impl StreamInterceptor for Audit {
///     async fn intercept(&self, call: StreamCall, next: StreamNext<'_>) -> Result<(), Status> {
///         let method = call.full_method().to_string();
///         let ended = next.run(call).await;
///         tracing::info!(%method, ok = ended.is_ok(), "stream over");
///         ended
///     }
/// }
/// ```
#[async_trait::async_trait]
pub trait StreamInterceptor: Send + Sync + 'static {
    /// Handles one call: inspect or rewrite it, then either refuse it by returning `Err`, or
    /// pass it on with [`StreamNext::run`], which resolves once the stream has ended.
    ///
    /// The result is the status the caller receives. Returning something other than what
    /// [`StreamNext::run`] gave rewrites it; the caller's trailers wait until this returns,
    /// so work after the stream should be short. Returning `Ok` without calling
    /// [`StreamNext::run`] ends the call successfully without the service seeing it.
    async fn intercept(&self, call: StreamCall, next: StreamNext<'_>) -> Result<(), Status>;
}

type Terminal = Box<dyn FnOnce(StreamCall) -> BoxFuture<'static, Outcome> + Send>;

/// The rest of the stream chain, ending at the service.
pub struct StreamNext<'a> {
    rest: &'a [Arc<dyn StreamInterceptor>],
    terminal: Terminal,
}

impl std::fmt::Debug for StreamNext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamNext")
            .field("remaining", &self.rest.len())
            .finish()
    }
}

impl StreamNext<'_> {
    /// Passes the call on, resolving when the stream has ended.
    ///
    /// # Errors
    ///
    /// The status the stream ended on: whatever the rest of the chain or the service
    /// returned, or `CANCELLED` when the caller went away.
    pub async fn run(self, call: StreamCall) -> Result<(), Status> {
        match self.rest.split_first() {
            Some((first, rest)) => {
                first
                    .intercept(
                        call,
                        StreamNext {
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

impl HybridServer {
    /// Installs a stream interceptor, applied in the order the calls are chained.
    ///
    /// Go's `WithStreamInterceptors`. It guards every streaming RPC the server answers — the
    /// caller's own streaming methods and the built-in ones such as reflection — which unary
    /// interceptors never see, on the TCP listener and the [`crate::Loopback`] alike. It runs
    /// after every [`HybridServer::with_unary_interceptor`], which sees streaming calls'
    /// metadata too.
    #[must_use]
    pub fn with_stream_interceptor(mut self, interceptor: impl StreamInterceptor) -> Self {
        tracing::debug!("with_stream_interceptor: appending stream interceptor");
        self.stream_interceptors.push(Arc::new(interceptor));
        self
    }
}

/// Installs [`StreamIntercepted`] in front of whatever tonic serves.
#[derive(Clone)]
pub(crate) struct StreamInterceptLayer {
    chain: Arc<[Arc<dyn StreamInterceptor>]>,
    pool: DescriptorPool,
}

impl StreamInterceptLayer {
    pub(crate) fn new(chain: Vec<Arc<dyn StreamInterceptor>>, pool: DescriptorPool) -> Self {
        if !chain.is_empty() {
            tracing::debug!(count = chain.len(), "gRPC: chaining stream interceptors");
        }
        Self {
            chain: chain.into(),
            pool,
        }
    }
}

impl<S> tower::Layer<S> for StreamInterceptLayer {
    type Service = StreamIntercepted<S>;

    fn layer(&self, inner: S) -> Self::Service {
        StreamIntercepted {
            inner,
            chain: Arc::clone(&self.chain),
            pool: self.pool.clone(),
        }
    }
}

/// A service behind the stream interceptor chain.
#[derive(Clone)]
pub(crate) struct StreamIntercepted<S> {
    inner: S,
    chain: Arc<[Arc<dyn StreamInterceptor>]>,
    pool: DescriptorPool,
}

impl<S> tower::Service<http::Request<Body>> for StreamIntercepted<S>
where
    S: tower::Service<http::Request<Body>, Response = Reply, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = Reply;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Reply, Infallible>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        if self.chain.is_empty() {
            return Box::pin(self.inner.call(request));
        }

        let path = request.uri().path().to_string();
        let descriptor = lookup(&self.pool, &path);
        if descriptor
            .as_ref()
            .is_some_and(|m| !m.is_client_streaming() && !m.is_server_streaming())
        {
            return Box::pin(self.inner.call(request));
        }

        // The ready service goes to the call; the fresh clone stays for the next poll_ready.
        let clone = self.inner.clone();
        let inner = std::mem::replace(&mut self.inner, clone);
        let chain = Arc::clone(&self.chain);
        let (reply, replied) = oneshot::channel();
        let caller_gone = CancellationToken::new();
        let gone = caller_gone.clone();

        tracing::debug!(%path, "gRPC: running stream interceptors");
        // The chain's task inherits what is current here, so spans it opens have a parent.
        tokio::spawn(
            drive(inner, chain, path, descriptor, request, reply, gone)
                .in_current_span()
                .with_current_context(),
        );

        Box::pin(async move {
            let guard = caller_gone.drop_guard();
            let reply = replied.await.unwrap_or_else(|_| {
                Status::internal("a stream interceptor stopped before answering").into_http()
            });
            guard.disarm();
            Ok(reply)
        })
    }
}

/// Runs the chain for one call, then delivers what it decided to whichever side is waiting:
/// the response future if the service was never reached, the body if it was.
async fn drive<S>(
    inner: S,
    chain: Arc<[Arc<dyn StreamInterceptor>]>,
    path: String,
    descriptor: Option<MethodDescriptor>,
    request: http::Request<Body>,
    reply: oneshot::Sender<Reply>,
    caller_gone: CancellationToken,
) where
    S: tower::Service<http::Request<Body>, Response = Reply, Error = Infallible> + Send + 'static,
    S::Future: Send + 'static,
{
    let (mut parts, body) = request.into_parts();
    let call = StreamCall {
        path,
        metadata: MetadataMap::from_headers(std::mem::take(&mut parts.headers)),
        extensions: std::mem::take(&mut parts.extensions),
        descriptor,
    };

    // Shared because the terminal may never run, and then this function must answer.
    let reply: ReplySlot = Arc::new(Mutex::new(Some(reply)));
    let (verdict, verdict_received) = oneshot::channel();
    let forward_reply = Arc::clone(&reply);
    let terminal: Terminal = Box::new(move |call| {
        Box::pin(forward(
            inner,
            parts,
            body,
            call,
            forward_reply,
            verdict_received,
            caller_gone,
        )) as BoxFuture<'static, Outcome>
    });

    let outcome = StreamNext {
        rest: &chain,
        terminal,
    }
    .run(call)
    .await;

    let unanswered = reply.lock().expect("grpc: stream reply poisoned").take();
    match unanswered {
        Some(reply) => {
            let _ = reply.send(rendered(outcome));
        }
        None => {
            let _ = verdict.send(outcome);
        }
    }
}

/// A status as a whole response, for a call the service never answered.
fn rendered(outcome: Outcome) -> Reply {
    match outcome {
        Ok(()) => Status::ok("").into_http(),
        Err(status) => status.into_http(),
    }
}

/// Calls the service, hands its response to the caller, and waits for the stream to end.
async fn forward<S>(
    mut inner: S,
    mut parts: http::request::Parts,
    body: Body,
    call: StreamCall,
    reply: ReplySlot,
    verdict: oneshot::Receiver<Outcome>,
    caller_gone: CancellationToken,
) -> Outcome
where
    S: tower::Service<http::Request<Body>, Response = Reply, Error = Infallible>,
{
    parts.headers = call.metadata.into_headers();
    parts.extensions = call.extensions;
    let request = http::Request::from_parts(parts, body);
    let response = tokio::select! {
        response = inner.call(request) => match response {
            Ok(response) => response,
            Err(never) => match never {},
        },
        () = caller_gone.cancelled() => return Err(cancelled()),
    };

    let (head, body) = response.into_parts();
    // A trailers-only response has already said how it ended and nothing has been sent, so
    // the chain can still decide what the caller hears.
    if let Some(status) = Status::from_header_map(&head.headers)
        && status.code() != tonic::Code::Ok
    {
        return Err(status);
    }

    let (ended, end) = oneshot::channel();
    let watched = Watched {
        inner: body,
        ended: Some(ended),
        verdict: Some(verdict),
        held: None,
        finished: false,
    };
    let response = http::Response::from_parts(head, Body::new(watched));
    let waiting = reply.lock().expect("grpc: stream reply poisoned").take();
    let delivered = waiting.is_some_and(|reply| reply.send(response).is_ok());
    if !delivered {
        return Err(cancelled());
    }
    end.await.unwrap_or_else(|_| Err(cancelled()))
}

fn cancelled() -> Status {
    Status::cancelled("the caller went away")
}

/// A response body that tells the chain when it ends, and takes the chain's word on the
/// trailers.
struct Watched {
    inner: Body,
    /// Told how the stream ended. Dropping the body with this still here means the caller
    /// went away.
    ended: Option<oneshot::Sender<Outcome>>,
    verdict: Option<oneshot::Receiver<Outcome>>,
    /// The service's trailers and what they said, waiting for the verdict.
    held: Option<(HeaderMap, Outcome)>,
    finished: bool,
}

impl Watched {
    fn end(&mut self, trailers: HeaderMap) {
        let outcome = trailer_outcome(&trailers);
        if let Some(ended) = self.ended.take() {
            let _ = ended.send(outcome.clone());
        }
        self.held = Some((trailers, outcome));
    }
}

impl http_body::Body for Watched {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let this = &mut *self;
        loop {
            if this.finished {
                return Poll::Ready(None);
            }
            if this.held.is_some() {
                let verdict = match this.verdict.as_mut() {
                    Some(verdict) => ready!(Pin::new(verdict).poll(cx)).ok(),
                    None => None,
                };
                this.finished = true;
                let Some((trailers, service)) = this.held.take() else {
                    continue;
                };
                let trailers = settle(trailers, &service, verdict);
                if trailers.is_empty() {
                    continue;
                }
                return Poll::Ready(Some(Ok(Frame::trailers(trailers))));
            }

            match ready!(Pin::new(&mut this.inner).poll_frame(cx)) {
                Some(Ok(frame)) => match frame.into_trailers() {
                    Ok(trailers) => this.end(trailers),
                    Err(frame) => return Poll::Ready(Some(Ok(frame))),
                },
                Some(Err(status)) => {
                    if let Some(ended) = this.ended.take() {
                        let _ = ended.send(Err(status.clone()));
                    }
                    this.finished = true;
                    return Poll::Ready(Some(Err(status)));
                }
                None => this.end(HeaderMap::new()),
            }
        }
    }
}

impl Drop for Watched {
    fn drop(&mut self) {
        if let Some(ended) = self.ended.take() {
            let _ = ended.send(Err(cancelled()));
        }
    }
}

/// What a stream's trailers say about how it ended.
fn trailer_outcome(trailers: &HeaderMap) -> Outcome {
    match Status::from_header_map(trailers) {
        Some(status) if status.code() != tonic::Code::Ok => Err(status),
        _ => Ok(()),
    }
}

/// The trailers the caller receives: the service's own unless the chain changed the outcome.
/// No verdict means the chain's task died, and then the service's word stands.
fn settle(trailers: HeaderMap, service: &Outcome, verdict: Option<Outcome>) -> HeaderMap {
    let same = match (service, &verdict) {
        (_, None) | (Ok(()), Some(Ok(()))) => true,
        (Err(service), Some(Err(chain))) => service.code() == chain.code(),
        _ => false,
    };
    if same {
        return trailers;
    }
    let (head, _) = rendered(verdict.unwrap_or(Ok(()))).into_parts();
    let mut headers = head.headers;
    headers.remove(http::header::CONTENT_TYPE);
    headers
}

#[cfg(test)]
mod tests {
    use super::*;

    fn status_of(headers: &HeaderMap) -> tonic::Code {
        Status::from_header_map(headers).map_or(tonic::Code::Ok, |s| s.code())
    }

    #[test]
    fn settle_keeps_the_services_trailers_unless_the_chain_changed_the_outcome() {
        let mut trailers = HeaderMap::new();
        trailers.insert("grpc-status", http::HeaderValue::from_static("5"));
        trailers.insert("x-kept", http::HeaderValue::from_static("yes"));
        let service = Err(Status::not_found("gone"));

        let same = settle(
            trailers.clone(),
            &service,
            Some(Err(Status::not_found("other"))),
        );
        assert!(same.contains_key("x-kept"));

        let died = settle(trailers.clone(), &service, None);
        assert!(died.contains_key("x-kept"));

        let rewritten = settle(trailers.clone(), &service, Some(Err(Status::aborted("no"))));
        assert_eq!(status_of(&rewritten), tonic::Code::Aborted);
        assert!(!rewritten.contains_key("x-kept"));
        assert!(!rewritten.contains_key("content-type"));

        let swallowed = settle(trailers, &service, Some(Ok(())));
        assert_eq!(status_of(&swallowed), tonic::Code::Ok);
        assert!(swallowed.contains_key("grpc-status"));
    }

    #[test]
    fn missing_trailers_read_as_success() {
        assert!(trailer_outcome(&HeaderMap::new()).is_ok());
    }
}
