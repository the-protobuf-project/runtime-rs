//! A span started from headers is the remote span's child, at the given time.
mod support;
use opentelemetry::{KeyValue, trace::TraceContextExt};
use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};
use support::Harness;
use telemetry::{Outcome, SpanKind, Start, extract, inject, trace_id};

#[test]
fn a_trace_crosses_headers_into_a_child_span() {
    let h = Harness::new();
    let observer = h.signals.observer("streams");

    let send = observer.start("nats", "publish", SpanKind::Producer);
    let mut headers = HashMap::new();
    inject(&send.context(), &mut headers);
    let send_trace = trace_id(&send.context()).unwrap();
    let send_span = send.context().span().span_context().span_id();
    send.finish(Outcome::Ok);

    let received = SystemTime::now() - Duration::from_millis(5);
    let deliver = observer.start_with(
        "nats",
        "deliver",
        SpanKind::Consumer,
        Start {
            parent: Some(extract(&headers)),
            time: Some(received),
            attributes: vec![KeyValue::new("messaging.destination.name", "order.placed")],
            ..Start::default()
        },
    );
    deliver.set_attribute(KeyValue::new("streams.settled", "ack"));
    deliver.finish(Outcome::Ok);

    let spans = h.traces.get_finished_spans().unwrap();
    let child = spans.iter().find(|s| s.name == "streams.deliver").unwrap();
    assert_eq!(child.span_context.trace_id().to_string(), send_trace);
    assert_eq!(child.parent_span_id, send_span);
    assert_eq!(child.start_time, received);
    let attrs: HashMap<_, _> = child
        .attributes
        .iter()
        .map(|kv| (kv.key.to_string(), kv.value.to_string()))
        .collect();
    assert_eq!(attrs["messaging.destination.name"], "order.placed");
    assert_eq!(attrs["streams.settled"], "ack");
    // The span-only attribute never becomes a metric label.
    for m in h.measurements() {
        assert!(!m.attributes.contains_key("messaging.destination.name"));
        if m.name == "runtime.operation.active" {
            assert_eq!(m.value, 0.0);
        }
    }
    assert_eq!(h.logs.get_emitted_logs().unwrap().len(), 2);
}
