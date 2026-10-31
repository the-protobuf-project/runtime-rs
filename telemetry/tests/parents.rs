//! Parent bridging and unwinding use real SDK exporters, never globals.
mod support;
use opentelemetry::trace::{TraceContextExt, TracerProvider};
use support::Harness;
use telemetry::{Outcome, SpanKind};
use tracing::{Instrument, instrument::WithSubscriber};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::prelude::*;

#[tokio::test]
async fn tracing_application_spans_parent_runtime_otel_spans() {
    let h = Harness::new();
    let subscriber = tracing_subscriber::registry()
        .with(tracing_opentelemetry::layer().with_tracer(h.trace_provider.tracer("application")));
    let dispatch = tracing::Dispatch::new(subscriber);
    let parent = tracing::dispatcher::with_default(&dispatch, || tracing::info_span!("request"));
    let parent_id = parent.context().span().span_context().span_id();
    async {
        h.signals
            .observer("database")
            .start("influxdb3", "range", SpanKind::Client)
            .finish(Outcome::Ok);
    }
    .instrument(parent.clone())
    .with_subscriber(dispatch)
    .await;
    drop(parent);
    let spans = h.traces.get_finished_spans().unwrap();
    let child = spans
        .iter()
        .find(|span| span.name == "database.range")
        .unwrap();
    assert_eq!(child.parent_span_id, parent_id);
    assert_eq!(h.logs.get_emitted_logs().unwrap().len(), 1);
    for point in h.measurements() {
        if point.name == "runtime.operation.active" {
            assert_eq!(point.value, 0.0);
        }
        assert!(point.attributes.contains_key("component"));
    }
}

#[test]
fn unwinding_is_recorded_once_as_panic_and_balances_active_operations() {
    let h = Harness::new();
    let observer = h.signals.observer("database");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _operation = observer.start("influxdb3", "write", SpanKind::Client);
        panic!("not exported: confidential panic text");
    }));
    assert!(result.is_err());
    let spans = h.traces.get_finished_spans().unwrap();
    assert_eq!(spans.len(), 1);
    assert!(
        spans[0]
            .attributes
            .iter()
            .any(|attr| attr.key.as_str() == "outcome" && attr.value.to_string() == "panic")
    );
    assert!(!format!("{spans:?}").contains("confidential"));
    assert_eq!(h.logs.get_emitted_logs().unwrap().len(), 1);
    assert!(
        h.measurements()
            .iter()
            .filter(|m| m.name == "runtime.operation.active")
            .all(|m| m.value == 0.0)
    );
}
