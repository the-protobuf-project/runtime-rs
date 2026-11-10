//! An embedder can keep the runtime's spans and metrics out of its own export.
//!
//! One test, because the global providers it proves untouched are process-global.

use grpc::HybridServer;
use grpc::options::{Endpoint, Options};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_client::HealthClient;

const SPAN: &str = "grpc.health.v1.Health/Check";
const METRIC: &str = "grpc.server.call.started";

struct Signals {
    spans: InMemorySpanExporter,
    metrics: InMemoryMetricExporter,
    tracers: SdkTracerProvider,
    meters: SdkMeterProvider,
}

impl Signals {
    /// These providers as the runtime's `Telemetry`.
    fn telemetry(&self) -> telemetry::Telemetry {
        use telemetry::opentelemetry::{metrics::MeterProvider as _, trace::TracerProvider as _};
        telemetry::Telemetry::default()
            .with_tracer(self.tracers.tracer("runtime-rs-grpc"))
            .with_meter(self.meters.meter("runtime-rs-grpc"))
    }

    fn new() -> Self {
        let spans = InMemorySpanExporter::default();
        let metrics = InMemoryMetricExporter::default();
        Self {
            tracers: SdkTracerProvider::builder()
                .with_simple_exporter(spans.clone())
                .build(),
            meters: SdkMeterProvider::builder()
                .with_reader(PeriodicReader::builder(metrics.clone()).build())
                .build(),
            spans,
            metrics,
        }
    }

    fn span_count(&self) -> usize {
        self.tracers.force_flush().expect("flush spans");
        let spans = self.spans.get_finished_spans().expect("spans");
        spans.iter().filter(|span| span.name == SPAN).count()
    }

    fn has_metric(&self) -> bool {
        self.meters.force_flush().expect("flush metrics");
        let exported = self.metrics.get_finished_metrics().expect("metrics");
        exported
            .iter()
            .flat_map(|resource| resource.scope_metrics())
            .flat_map(|scope| scope.metrics())
            .any(|metric| metric.name() == METRIC)
    }
}

async fn one_call(server: HybridServer) {
    let mut server = server;
    server.start().await.expect("the server starts");
    let addr = server.grpc_addr().expect("gRPC bound");
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .expect("a valid endpoint")
        .connect()
        .await
        .expect("connects");
    HealthClient::new(channel)
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("served");
    server.stop().await.expect("the server stops");
}

fn options() -> Options {
    Options {
        service_name: "telemetry-test".into(),
        grpc: Endpoint::new("127.0.0.1", 0),
        enable_health: true,
        ..Default::default()
    }
}

#[tokio::test]
async fn the_embedders_global_providers_see_only_what_it_allows() {
    let global = Signals::new();
    telemetry::opentelemetry::global::set_tracer_provider(global.tracers.clone());
    telemetry::opentelemetry::global::set_meter_provider(global.meters.clone());

    let passed = Signals::new();
    one_call(HybridServer::new(options()).with_telemetry(&passed.telemetry())).await;
    assert_eq!(passed.span_count(), 1, "the passed tracer got the span");
    assert!(passed.has_metric(), "the passed meter got the metric");

    one_call(HybridServer::new(options()).without_telemetry()).await;

    assert_eq!(global.span_count(), 0, "nothing reached the global tracer");
    assert!(!global.has_metric(), "nothing reached the global meter");

    // The control: the default still records globally, so the zero above means something.
    one_call(HybridServer::new(options())).await;
    assert_eq!(global.span_count(), 1, "the default is the global tracer");
    assert!(global.has_metric(), "the default is the global meter");
}
