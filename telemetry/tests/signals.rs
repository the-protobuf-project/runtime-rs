//! All three signals, correlation, safe attributes and balanced cancellation.
mod support;
use opentelemetry::{
    Context,
    trace::{FutureExt, TraceContextExt, Tracer, TracerProvider},
};
use support::Harness;
use telemetry::{Outcome, SpanKind, Telemetry};

#[tokio::test]
async fn completion_exports_metrics_logs_and_a_correlated_child_span() {
    let h = Harness::new();
    let observer = h.signals.observer("database");
    let parent =
        Context::current_with_span(h.trace_provider.tracer("application").start("request"));
    let parent_id = parent.span().span_context().span_id();
    async {
        let mut operation = observer.start("influxdb3", "write", SpanKind::Client);
        operation.written_rows(3);
        operation.finish(Outcome::Ok);
    }
    .with_context(parent.clone())
    .await;
    let spans = h.traces.get_finished_spans().unwrap();
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0].parent_span_id, parent_id);
    let logs = h.logs.get_emitted_logs().unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(
        logs[0].record.trace_context().unwrap().span_id,
        spans[0].span_context.span_id()
    );
    let measurements = h.measurements();
    assert!(
        measurements
            .iter()
            .any(|m| m.name == "runtime.records.written" && m.value == 3.0)
    );
    assert!(
        measurements
            .iter()
            .any(|m| m.name == "runtime.operation.duration" && m.value == 1.0)
    );
    assert!(
        measurements
            .iter()
            .filter(|m| m.name == "runtime.operation.active")
            .all(|m| m.value == 0.0)
    );
    for metric in &measurements {
        assert!(metric.attributes.keys().all(|key| ["component", "backend", "operation", "outcome"].contains(&key.as_str())));
    }
    parent.span().end();
}

#[tokio::test]
async fn dropping_an_inflight_future_finishes_once_as_cancelled() {
    let h = Harness::new();
    let observer = h.signals.observer("database");
    let result = tokio::time::timeout(std::time::Duration::from_millis(10), async {
        let _operation = observer.start("influxdb3", "range", SpanKind::Client);
        std::future::pending::<()>().await;
    })
    .await;
    assert!(result.is_err());
    let spans = h.traces.get_finished_spans().unwrap();
    assert_eq!(spans.len(), 1);
    assert!(
        spans[0]
            .attributes
            .iter()
            .any(|kv| kv.key.as_str() == "outcome" && kv.value.to_string() == "cancelled")
    );
    assert_eq!(h.logs.get_emitted_logs().unwrap().len(), 1);
    assert!(
        h.measurements()
            .iter()
            .filter(|m| m.name == "runtime.operation.active")
            .all(|m| m.value == 0.0)
    );
}

#[test]
fn noop_or_meter_only_observers_never_end_the_callers_span() {
    let h = Harness::new();
    let parent = Context::current_with_span(h.trace_provider.tracer("app").start("parent"));
    let _attached = parent.clone().attach();
    use opentelemetry::metrics::{MeterProvider, NoopMeterProvider};
    let observer = Telemetry::default().observer("database");
    observer
        .start("influxdb3", "write", SpanKind::Client)
        .finish(Outcome::Ok);
    let meter_only = Telemetry::default().with_meter(NoopMeterProvider::new().meter("test"));
    meter_only
        .observer("database")
        .start("influxdb3", "write", SpanKind::Client)
        .finish(Outcome::Ok);
    assert!(h.traces.get_finished_spans().unwrap().is_empty());
    parent.span().end();
    assert_eq!(h.traces.get_finished_spans().unwrap().len(), 1);
}

#[tokio::test]
async fn concurrent_future_contexts_do_not_cross_parent_boundaries() {
    let h = Harness::new();
    let tracer = h.trace_provider.tracer("application");
    let first = Context::current_with_span(tracer.start("first"));
    let second = Context::current_with_span(tracer.start("second"));
    let observer = h.signals.observer("database");
    let work = || async {
        let operation = observer.start("influxdb3", "range", SpanKind::Client);
        tokio::task::yield_now()
            .with_context(operation.context())
            .await;
        operation.finish(Outcome::Ok);
    };
    futures::join!(
        work().with_context(first.clone()),
        work().with_context(second.clone())
    );
    let spans = h.traces.get_finished_spans().unwrap();
    assert_eq!(spans.len(), 2);
    assert!(
        spans
            .iter()
            .any(|s| s.parent_span_id == first.span().span_context().span_id())
    );
    assert!(
        spans
            .iter()
            .any(|s| s.parent_span_id == second.span().span_context().span_id())
    );
    first.span().end();
    second.span().end();
}
