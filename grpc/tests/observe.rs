//! A running server reports every call through OpenTelemetry's global providers: the
//! always-on server span and `grpc.server.call.*` metrics, and an [`Observer`]'s
//! `<service>/<method>` span and `rpc.*` counters inside it.
//!
//! One test, because the providers are process-global.

use std::net::TcpListener;

use grpc::options::{Endpoint, Options};
use grpc::{HybridServer, Observer};
use opentelemetry::trace::SpanKind;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_client::HealthClient;

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
        .expect("a free port")
}

/// The sum of a counter's points, or the count of a histogram's.
fn total(metrics: &ResourceMetrics, name: &str) -> u64 {
    metrics
        .scope_metrics()
        .flat_map(|scope| scope.metrics())
        .filter(|metric| metric.name() == name)
        .map(|metric| match metric.data() {
            AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                sum.data_points().map(|point| point.value()).sum()
            }
            AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                histogram.data_points().map(|point| point.count()).sum()
            }
            AggregatedMetrics::U64(MetricData::Histogram(histogram)) => {
                histogram.data_points().map(|point| point.count()).sum()
            }
            _ => 0,
        })
        .sum()
}

#[tokio::test]
async fn calls_are_spanned_and_counted_through_the_global_providers() {
    let spans = InMemorySpanExporter::default();
    let metrics = InMemoryMetricExporter::default();
    let tracers = SdkTracerProvider::builder()
        .with_simple_exporter(spans.clone())
        .build();
    let meters = SdkMeterProvider::builder()
        .with_reader(PeriodicReader::builder(metrics.clone()).build())
        .build();

    // Built before the providers are installed, as an application's would be.
    let grpc_port = free_port();
    let mut server = HybridServer::new(Options {
        service_name: "observe-test".into(),
        grpc: Endpoint::new("127.0.0.1", grpc_port),
        enable_health: true,
        ..Default::default()
    })
    .with_reflection(tonic_health::pb::FILE_DESCRIPTOR_SET)
    .with_observer(Observer::new("observe-test"));

    opentelemetry::global::set_tracer_provider(tracers.clone());
    opentelemetry::global::set_meter_provider(meters.clone());
    server.start().await.expect("the server starts");

    let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{grpc_port}"))
        .expect("a valid endpoint")
        .connect()
        .await
        .expect("connects");
    let mut client = HealthClient::new(channel);
    client
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("served");
    let missing = client
        .check(HealthCheckRequest {
            service: "nope".into(),
        })
        .await
        .expect_err("unknown service");
    assert_eq!(missing.code(), tonic::Code::NotFound);
    server.stop().await.expect("the server stops");

    tracers.force_flush().expect("flush spans");
    let finished = spans.get_finished_spans().expect("spans");
    let named: Vec<_> = finished
        .iter()
        .filter(|span| span.name == "grpc.health.v1.Health/Check")
        .collect();
    let (server_spans, observer_spans): (Vec<_>, Vec<_>) = named
        .into_iter()
        .partition(|span| span.span_kind == SpanKind::Server);
    assert_eq!(server_spans.len(), 2, "{finished:#?}");
    assert_eq!(observer_spans.len(), 2, "{finished:#?}");
    for observed in &observer_spans {
        assert!(
            server_spans
                .iter()
                .any(|server| server.span_context.span_id() == observed.parent_span_id),
            "the observer's span is a child of the server span"
        );
    }

    meters.force_flush().expect("flush metrics");
    let exported = metrics.get_finished_metrics().expect("metrics");
    let last = exported.last().expect("one export at least");
    assert_eq!(total(last, "rpc.requests"), 2);
    assert_eq!(
        total(last, "rpc.errors"),
        0,
        "NOT_FOUND is not a server fault"
    );
    assert_eq!(total(last, "grpc.server.call.started"), 2);
    assert_eq!(total(last, "grpc.server.call.duration"), 2);
    assert_eq!(
        total(last, "grpc.server.call.rcvd_total_compressed_message_size"),
        2
    );
}
