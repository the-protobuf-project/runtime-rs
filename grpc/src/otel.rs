//! OpenTelemetry on every gRPC call, always on. Go's `opentelemetry.go`.
//!
//! Go installs `otelgrpc`'s stats handler and gRPC's own metrics on every server, through
//! OpenTelemetry's global providers, which the application sets up. This layer does the same
//! job: a server span per call, and the four `grpc.server.call.*` metrics named in
//! [`crate::options::opentelemetry::server`]. It sits outermost on the TCP listener and the
//! [`crate::Loopback`], so it sees a call an interceptor refuses, and a REST call as the gRPC
//! call it becomes, just as Go's stats handler sees the gateway's dial.
//!
//! With no provider installed the instruments are no-ops, which is the cost argument Go makes
//! for leaving this on unconditionally. No exporter is added: that is the application's job,
//! as Go's comment on the exporter it removed explains. This is also what remains of Go's
//! `grpc/shared/telemetry.go`: logs go through `tracing`, the scope below is its `Meter`, and
//! its `Close` has nothing to release here because the application owns the providers.
//!
//! # Differences from Go
//!
//! Rust's global providers do not delegate, so the instruments resolve on the first call
//! rather than when the server is built — by which time the application has set them up.
//!
//! The size metrics count bytes on the wire, each message's five-byte gRPC prefix included,
//! because only the transport's body is visible here. `grpc.method` is the request path
//! without its leading slash; Go's `other` bucket for unregistered methods needs a method
//! registry this crate does not keep, so OpenTelemetry's cardinality limit bounds it instead.
//!
//! The client, load balancer and xDS metrics Go also enables are named in the options but not
//! emitted: a server has none of those parts.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context as TaskContext, Poll, ready};
use std::time::Instant;

use bytes::Bytes;
use http_body::Frame;
use opentelemetry::context::{Context, FutureExt as _};
use opentelemetry::global::{self, BoxedTracer};
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::trace::{SpanKind, Status as SpanStatus, TraceContextExt as _, Tracer as _};
use opentelemetry::{InstrumentationScope, KeyValue};
use opentelemetry_http::HeaderExtractor;
use tonic::body::Body;
use tonic::{Code, Status};

use crate::options::opentelemetry::server;

/// The instrumentation scope this crate reports under; Go's `runtime-go-grpc`.
const SCOPE: &str = "runtime-rs-grpc";

/// gRFC A66's bucket boundaries for call latency, in seconds.
const DURATION_BOUNDARIES: [f64; 41] = [
    0.0, 0.00001, 0.00005, 0.0001, 0.0003, 0.0006, 0.0008, 0.001, 0.002, 0.003, 0.004, 0.005,
    0.006, 0.008, 0.01, 0.013, 0.016, 0.02, 0.025, 0.03, 0.04, 0.05, 0.065, 0.08, 0.1, 0.13, 0.16,
    0.2, 0.25, 0.3, 0.4, 0.5, 0.65, 0.8, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0,
];

/// gRFC A66's bucket boundaries for message sizes, in bytes.
const SIZE_BOUNDARIES: [f64; 14] = [
    0.0,
    1024.0,
    2048.0,
    4096.0,
    16384.0,
    65536.0,
    262_144.0,
    1_048_576.0,
    4_194_304.0,
    16_777_216.0,
    67_108_864.0,
    268_435_456.0,
    1_073_741_824.0,
    4_294_967_296.0,
];

struct Instruments {
    tracer: BoxedTracer,
    started: Counter<u64>,
    duration: Histogram<f64>,
    sent: Histogram<u64>,
    received: Histogram<u64>,
}

impl Instruments {
    fn new() -> Self {
        tracing::debug!(scope = SCOPE, "gRPC: resolving OpenTelemetry instruments");
        let scope = InstrumentationScope::builder(SCOPE)
            .with_version(env!("CARGO_PKG_VERSION"))
            .build();
        let meter = global::meter_with_scope(scope.clone());
        Self {
            tracer: global::tracer_with_scope(scope),
            started: meter
                .u64_counter(server::CALL_STARTED)
                .with_unit("{call}")
                .build(),
            duration: meter
                .f64_histogram(server::CALL_DURATION)
                .with_unit("s")
                .with_boundaries(DURATION_BOUNDARIES.to_vec())
                .build(),
            sent: meter
                .u64_histogram(server::CALL_SENT_TOTAL_COMPRESSED_MESSAGE_SIZE)
                .with_unit("By")
                .with_boundaries(SIZE_BOUNDARIES.to_vec())
                .build(),
            received: meter
                .u64_histogram(server::CALL_RCVD_TOTAL_COMPRESSED_MESSAGE_SIZE)
                .with_unit("By")
                .with_boundaries(SIZE_BOUNDARIES.to_vec())
                .build(),
        }
    }
}

/// Installs [`Otel`] in front of whatever tonic serves.
#[derive(Clone)]
pub(crate) struct OtelLayer {
    instruments: Arc<OnceLock<Arc<Instruments>>>,
}

impl OtelLayer {
    pub(crate) fn new() -> Self {
        Self {
            instruments: Arc::new(OnceLock::new()),
        }
    }
}

impl<S> tower::Layer<S> for OtelLayer {
    type Service = Otel<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Otel {
            inner,
            instruments: Arc::clone(&self.instruments),
        }
    }
}

/// A service whose calls are spanned and measured.
#[derive(Clone)]
pub(crate) struct Otel<S> {
    inner: S,
    instruments: Arc<OnceLock<Arc<Instruments>>>,
}

impl<S> tower::Service<http::Request<Body>> for Otel<S>
where
    S: tower::Service<http::Request<Body>, Response = http::Response<Body>, Error = Infallible>,
    S::Future: Send + 'static,
{
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let instruments = Arc::clone(
            self.instruments
                .get_or_init(|| Arc::new(Instruments::new())),
        );
        let full_method = request.uri().path().trim_start_matches('/').to_string();
        let (service, method) = full_method
            .split_once('/')
            .unwrap_or((full_method.as_str(), ""));

        // The caller's trace, when it sent one, as `otelgrpc` reads it.
        let parent = global::get_text_map_propagator(|propagator| {
            propagator.extract(&HeaderExtractor(request.headers()))
        });
        let span = instruments
            .tracer
            .span_builder(full_method.clone())
            .with_kind(SpanKind::Server)
            .with_attributes([
                KeyValue::new("rpc.system", "grpc"),
                KeyValue::new("rpc.service", service.to_string()),
                KeyValue::new("rpc.method", method.to_string()),
            ])
            .start_with_context(&instruments.tracer, &parent);
        let context = parent.with_span(span);

        let attribute = KeyValue::new("grpc.method", full_method);
        instruments.started.add(1, std::slice::from_ref(&attribute));

        let received = Arc::new(AtomicU64::new(0));
        let (parts, body) = request.into_parts();
        let body = Body::new(CountedRequest {
            inner: body,
            bytes: Arc::clone(&received),
        });
        let request = http::Request::from_parts(parts, body);

        // Attached across `call` as well as the await, because an inner layer may capture
        // the current context at call time to hand to a task of its own.
        let future = {
            let _attached = context.clone().attach();
            self.inner.call(request)
        };

        let mut call = Call {
            instruments,
            context: context.clone(),
            attribute,
            started: Instant::now(),
            received,
            sent: 0,
            finished: false,
        };
        Box::pin(async move {
            let response = match future.with_context(context).await {
                Ok(response) => response,
                Err(never) => match never {},
            };
            let (head, body) = response.into_parts();
            // A trailers-only response has already ended, and its body says nothing more.
            if let Some(status) = Status::from_header_map(&head.headers) {
                call.finish(status.code());
                return Ok(http::Response::from_parts(head, body));
            }
            let body = Body::new(MeasuredResponse { inner: body, call });
            Ok(http::Response::from_parts(head, body))
        })
    }
}

/// A call in flight. Dropped unfinished means the caller went away.
struct Call {
    instruments: Arc<Instruments>,
    context: Context,
    attribute: KeyValue,
    started: Instant,
    received: Arc<AtomicU64>,
    sent: u64,
    finished: bool,
}

impl Call {
    fn finish(&mut self, code: Code) {
        if std::mem::replace(&mut self.finished, true) {
            return;
        }
        let attributes = [
            self.attribute.clone(),
            KeyValue::new("grpc.status", code_name(code)),
        ];
        let instruments = &self.instruments;
        instruments
            .duration
            .record(self.started.elapsed().as_secs_f64(), &attributes);
        instruments.sent.record(self.sent, &attributes);
        instruments
            .received
            .record(self.received.load(Ordering::Relaxed), &attributes);

        let span = self.context.span();
        span.set_attribute(KeyValue::new(
            "rpc.grpc.status_code",
            i64::from(code as i32),
        ));
        if is_span_error(code) {
            span.set_status(SpanStatus::error(code_name(code)));
        }
        span.end();
    }
}

impl Drop for Call {
    fn drop(&mut self) {
        self.finish(Code::Cancelled);
    }
}

/// Counts what the caller sent.
struct CountedRequest {
    inner: Body,
    bytes: Arc<AtomicU64>,
}

impl http_body::Body for CountedRequest {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let frame = ready!(Pin::new(&mut self.inner).poll_frame(cx));
        if let Some(data) = frame
            .as_ref()
            .and_then(|frame| frame.as_ref().ok())
            .and_then(Frame::data_ref)
        {
            self.bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
        }
        Poll::Ready(frame)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

/// Counts what the server sent, and ends the call with the status the stream ended on.
struct MeasuredResponse {
    inner: Body,
    call: Call,
}

impl http_body::Body for MeasuredResponse {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let this = &mut *self;
        let frame = ready!(Pin::new(&mut this.inner).poll_frame(cx));
        match &frame {
            Some(Ok(frame)) => {
                if let Some(data) = frame.data_ref() {
                    this.call.sent += data.len() as u64;
                } else if let Some(trailers) = frame.trailers_ref() {
                    let code =
                        Status::from_header_map(trailers).map_or(Code::Unknown, |s| s.code());
                    this.call.finish(code);
                }
            }
            Some(Err(status)) => this.call.finish(status.code()),
            // A body that ends with no trailers never said how it ended.
            None => this.call.finish(Code::Unknown),
        }
        Poll::Ready(frame)
    }
}

/// Whether `otelgrpc` marks a server span ending with this code as failed.
fn is_span_error(code: Code) -> bool {
    matches!(
        code,
        Code::Unknown
            | Code::DeadlineExceeded
            | Code::Unimplemented
            | Code::Internal
            | Code::Unavailable
            | Code::DataLoss
    )
}

/// The canonical name of a status code, which is what `grpc.status` carries.
fn code_name(code: Code) -> &'static str {
    match code {
        Code::Ok => "OK",
        Code::Cancelled => "CANCELLED",
        Code::Unknown => "UNKNOWN",
        Code::InvalidArgument => "INVALID_ARGUMENT",
        Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        Code::NotFound => "NOT_FOUND",
        Code::AlreadyExists => "ALREADY_EXISTS",
        Code::PermissionDenied => "PERMISSION_DENIED",
        Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        Code::FailedPrecondition => "FAILED_PRECONDITION",
        Code::Aborted => "ABORTED",
        Code::OutOfRange => "OUT_OF_RANGE",
        Code::Unimplemented => "UNIMPLEMENTED",
        Code::Internal => "INTERNAL",
        Code::Unavailable => "UNAVAILABLE",
        Code::DataLoss => "DATA_LOSS",
        Code::Unauthenticated => "UNAUTHENTICATED",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn span_errors_are_otelgrpcs_server_faults() {
        assert!(is_span_error(Code::Internal));
        assert!(is_span_error(Code::Unimplemented));
        assert!(!is_span_error(Code::NotFound));
        assert!(!is_span_error(Code::Ok));
    }

    #[test]
    fn code_names_are_canonical() {
        assert_eq!(code_name(Code::DeadlineExceeded), "DEADLINE_EXCEEDED");
        assert_eq!(code_name(Code::Ok), "OK");
    }
}
