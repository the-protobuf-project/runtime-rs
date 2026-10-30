//! Per-call spans, counters and outcome logging. Go's `observe.go`.
//!
//! An [`Observer`] puts a span named `<service>/<method>` around a call, counts it on
//! `rpc.requests`, counts server faults on `rpc.errors` (see [`is_server_error`]), and logs
//! the outcome. The counters carry `service` and `method` attributes, so they slice per
//! endpoint.
//!
//! # Differences from Go
//!
//! Go's handlers call `Traced` themselves. Here the same work is also an [`Interceptor`] and
//! a [`StreamInterceptor`], so [`HybridServer::with_observer`] covers every call without
//! touching a handler; [`Observer::traced`] is the Go shape for code that wants it. Register
//! the observer last to measure what Go measures, which is the handler alone.
//!
//! Go takes the application's telemetry client. The equivalent here is the OpenTelemetry
//! providers: [`Observer::new`] uses the global ones and [`Observer::with_providers`] takes
//! them explicitly. Logging goes through `tracing`. No exporter is added — exporting is the
//! application's job. Rust's global providers do not delegate as Go's do, so an instrument
//! built before the application installs its provider would stay a no-op; [`Observer::new`]
//! therefore resolves the globals on the first call rather than when it is built.
//!
//! A call dropped before finishing, because its caller went away, is still counted as a
//! request, where Go's handler would run on and return. On a stream, the span is current for
//! the chain but not while the service produces messages.
//!
//! [`HybridServer::with_observer`]: crate::HybridServer::with_observer

use std::borrow::Cow;
use std::future::Future;
use std::sync::{Arc, OnceLock};

use opentelemetry::context::{Context, FutureExt as _};
use opentelemetry::global::{self, BoxedTracer};
use opentelemetry::metrics::{Counter, Meter};
use opentelemetry::trace::{Status as SpanStatus, TraceContextExt as _, Tracer as _};
use opentelemetry::{InstrumentationScope, KeyValue};
use tonic::{Code, Status};
use tracing::Instrument as _;

use crate::hybrid::HybridServer;
use crate::intercept::{Interceptor, Next, UnaryCall, UnaryReply};
use crate::stream_intercept::{StreamCall, StreamInterceptor, StreamNext};

/// Reports whether `error` is a server-side fault — one of the codes that mean the service
/// itself misbehaved — as opposed to an expected client or business outcome such as
/// `NOT_FOUND`, `INVALID_ARGUMENT` or `FAILED_PRECONDITION`. Go's `IsServerError`.
#[must_use]
pub fn is_server_error(error: &Status) -> bool {
    matches!(
        error.code(),
        Code::Internal
            | Code::Unknown
            | Code::DataLoss
            | Code::Unavailable
            | Code::DeadlineExceeded
    )
}

struct Instruments {
    tracer: BoxedTracer,
    requests: Counter<u64>,
    errors: Counter<u64>,
}

impl Instruments {
    fn new(tracer: BoxedTracer, meter: &Meter) -> Self {
        Self {
            tracer,
            requests: meter.u64_counter("rpc.requests").build(),
            errors: meter.u64_counter("rpc.errors").build(),
        }
    }
}

struct Inner {
    /// The instrumentation scope the globals are resolved under, on the first call.
    scope: Cow<'static, str>,
    instruments: OnceLock<Instruments>,
}

/// Wraps RPC handling in a trace span, request and error counters, and outcome logging.
///
/// Construct one per application and share it: it is cheap to clone, and stateless beyond
/// its handles.
#[derive(Clone)]
pub struct Observer {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Observer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Observer")
            .field("scope", &self.inner.scope)
            .finish_non_exhaustive()
    }
}

impl Observer {
    /// An observer emitting through OpenTelemetry's global providers, under the
    /// instrumentation scope `name` — usually the application's name. It may be built before
    /// the application installs the providers. Go's `NewObserver`.
    #[must_use]
    pub fn new(name: impl Into<Cow<'static, str>>) -> Self {
        Self {
            inner: Arc::new(Inner {
                scope: name.into(),
                instruments: OnceLock::new(),
            }),
        }
    }

    /// An observer emitting through the given tracer and meter, for an application that does
    /// not install global providers.
    #[must_use]
    pub fn with_providers(tracer: BoxedTracer, meter: &Meter) -> Self {
        Self {
            inner: Arc::new(Inner {
                scope: Cow::Borrowed("explicit"),
                instruments: OnceLock::from(Instruments::new(tracer, meter)),
            }),
        }
    }

    fn instruments(&self) -> &Instruments {
        self.inner.instruments.get_or_init(|| {
            let scope = InstrumentationScope::builder(self.inner.scope.clone()).build();
            Instruments::new(
                global::tracer_with_scope(scope.clone()),
                &global::meter_with_scope(scope),
            )
        })
    }

    /// Runs `body` inside a span named `<service>/<method>`, records the counters, and logs
    /// the outcome. Go's `Traced`.
    ///
    /// Expected client or business rejections are logged at debug rather than error and left
    /// out of the error counter. The span's status is an error for either, as the tracing
    /// layer under Go's `Traced` sets it.
    ///
    /// # Errors
    ///
    /// Whatever `body` returned.
    pub async fn traced<T, F, Fut>(&self, service: &str, method: &str, body: F) -> Result<T, Status>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        let span = self
            .instruments()
            .tracer
            .start(format!("{service}/{method}"));
        let context = Context::current_with_span(span);
        let mut pending = Pending {
            observer: self,
            service,
            method,
            armed: true,
        };

        let result = body()
            .with_context(context.clone())
            .instrument(tracing::debug_span!("rpc", %service, %method))
            .await;

        pending.armed = false;
        self.record(service, method, result.as_ref().err());
        match &result {
            Ok(_) => tracing::debug!("{method} ok"),
            Err(status) => {
                context
                    .span()
                    .set_status(SpanStatus::error(status.message().to_string()));
                if is_server_error(status) {
                    tracing::error!(error = %status, "{method} failed");
                } else {
                    // An expected client or business outcome — returned to the caller, but
                    // not a fault of the service.
                    tracing::debug!(error = %status, "{method} rejected");
                }
            }
        }
        context.span().end();
        result
    }

    /// Emits the per-call counters. Only server faults reach `rpc.errors`.
    fn record(&self, service: &str, method: &str, error: Option<&Status>) {
        let instruments = self.instruments();
        let attributes = [
            KeyValue::new("service", service.to_string()),
            KeyValue::new("method", method.to_string()),
        ];
        instruments.requests.add(1, &attributes);
        if error.is_some_and(is_server_error) {
            instruments.errors.add(1, &attributes);
        }
    }
}

/// Counts a call dropped before it finished, so a caller hanging up is still a request.
struct Pending<'a> {
    observer: &'a Observer,
    service: &'a str,
    method: &'a str,
    armed: bool,
}

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        if self.armed {
            tracing::debug!(method = self.method, "dropped before finishing");
            self.observer.record(self.service, self.method, None);
        }
    }
}

#[async_trait::async_trait]
impl Interceptor for Observer {
    async fn intercept(&self, call: UnaryCall, next: Next<'_>) -> Result<UnaryReply, Status> {
        let (service, method) = (call.service().to_string(), call.method().to_string());
        self.traced(&service, &method, move || next.run(call)).await
    }
}

#[async_trait::async_trait]
impl StreamInterceptor for Observer {
    async fn intercept(&self, call: StreamCall, next: StreamNext<'_>) -> Result<(), Status> {
        let (service, method) = (call.service().to_string(), call.method().to_string());
        self.traced(&service, &method, move || next.run(call)).await
    }
}

impl HybridServer {
    /// Observes every unary and streaming call with `observer`, at this point in both the
    /// [`HybridServer::with_interceptor`] and [`HybridServer::with_stream_interceptor`]
    /// chains.
    ///
    /// Go's handlers call `Traced` themselves; this is the same work as an interceptor. See
    /// [`crate::observe`] for why it should usually be registered last.
    #[must_use]
    pub fn with_observer(mut self, observer: Observer) -> Self {
        tracing::debug!(?observer, "with_observer: installing observer");
        self.message_interceptors.push(Arc::new(observer.clone()));
        self.stream_interceptors.push(Arc::new(observer));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};

    struct Harness {
        spans: InMemorySpanExporter,
        metrics: InMemoryMetricExporter,
        tracers: SdkTracerProvider,
        meters: SdkMeterProvider,
        observer: Observer,
    }

    fn harness() -> Harness {
        let spans = InMemorySpanExporter::default();
        let metrics = InMemoryMetricExporter::default();
        let tracers = SdkTracerProvider::builder()
            .with_simple_exporter(spans.clone())
            .build();
        let meters = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(metrics.clone()).build())
            .build();
        let observer = Observer::with_providers(
            BoxedTracer::new(Box::new(tracers.tracer("test"))),
            &meters.meter("test"),
        );
        Harness {
            spans,
            metrics,
            tracers,
            meters,
            observer,
        }
    }

    impl Harness {
        /// The sum of every data point of the counter `name`.
        fn count(&self, name: &str) -> u64 {
            self.meters.force_flush().expect("flush");
            let finished = self.metrics.get_finished_metrics().expect("metrics");
            let Some(last) = finished.last() else {
                return 0;
            };
            last.scope_metrics()
                .flat_map(|scope| scope.metrics())
                .filter(|metric| metric.name() == name)
                .map(|metric| match metric.data() {
                    AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                        sum.data_points().map(|point| point.value()).sum()
                    }
                    _ => 0,
                })
                .sum()
        }
    }

    #[test]
    fn only_server_faults_count_as_server_errors() {
        for code in [
            Code::Internal,
            Code::Unknown,
            Code::DataLoss,
            Code::Unavailable,
            Code::DeadlineExceeded,
        ] {
            assert!(is_server_error(&Status::new(code, "")), "{code:?}");
        }
        for code in [
            Code::Ok,
            Code::NotFound,
            Code::InvalidArgument,
            Code::FailedPrecondition,
            Code::ResourceExhausted,
            Code::Aborted,
            Code::AlreadyExists,
            Code::PermissionDenied,
            Code::Unauthenticated,
            Code::Cancelled,
        ] {
            assert!(!is_server_error(&Status::new(code, "")), "{code:?}");
        }
    }

    #[tokio::test]
    async fn traced_records_a_named_span_and_the_counters() {
        let harness = harness();
        let ok = harness
            .observer
            .traced("pkg.Svc", "Get", || async { Ok::<_, Status>(7) })
            .await;
        assert_eq!(ok.expect("the body's value"), 7);

        let rejected = harness
            .observer
            .traced("pkg.Svc", "Get", || async {
                Err::<(), _>(Status::not_found("nope"))
            })
            .await;
        assert_eq!(rejected.expect_err("rejected").code(), Code::NotFound);

        let failed = harness
            .observer
            .traced("pkg.Svc", "Get", || async {
                Err::<(), _>(Status::internal("broke"))
            })
            .await;
        assert_eq!(failed.expect_err("failed").code(), Code::Internal);

        harness.tracers.force_flush().expect("flush");
        let spans = harness.spans.get_finished_spans().expect("spans");
        assert_eq!(spans.len(), 3);
        assert!(spans.iter().all(|span| span.name == "pkg.Svc/Get"));
        assert_eq!(spans[0].status, SpanStatus::Unset);
        assert_eq!(spans[2].status, SpanStatus::error("broke"));

        assert_eq!(harness.count("rpc.requests"), 3);
        assert_eq!(harness.count("rpc.errors"), 1);
    }

    #[tokio::test]
    async fn the_span_is_current_inside_the_body() {
        let harness = harness();
        let valid = harness
            .observer
            .traced("s", "m", || async {
                Ok::<_, Status>(Context::current().span().span_context().is_valid())
            })
            .await;
        assert!(valid.expect("ok"));
    }

    #[tokio::test]
    async fn a_dropped_call_still_counts_as_a_request() {
        let harness = harness();
        {
            let call =
                harness
                    .observer
                    .traced("s", "m", std::future::pending::<Result<(), Status>>);
            tokio::pin!(call);
            assert!(futures_util::poll!(&mut call).is_pending());
        }
        assert_eq!(harness.count("rpc.requests"), 1);
        assert_eq!(harness.count("rpc.errors"), 0);
    }
}
