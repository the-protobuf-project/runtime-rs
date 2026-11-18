//! W3C baggage crosses headers and lands on an operation's span and log only.
mod support;
use opentelemetry::Context;
use std::collections::HashMap;
use support::Harness;
use telemetry::{BAGGAGE, Outcome, SpanKind, Start, baggage, extract, inject, set_baggage};

#[test]
fn baggage_round_trips_through_headers_escaped() {
    let cx = set_baggage(&Context::new(), "tenant", "acme corp");
    let cx = set_baggage(&cx, "class", "a,b=c");
    let cx = set_baggage(&cx, "tenant", "acme corp, ltd");

    let mut headers = HashMap::new();
    inject(&cx, &mut headers);
    // Written even without a span: baggage is not the trace.
    let header = &headers[BAGGAGE];
    assert!(!header.contains(' '), "unescaped space in {header}");

    let received = baggage(&extract(&headers));
    assert_eq!(
        received,
        vec![
            ("class".to_string(), "a,b=c".to_string()),
            ("tenant".to_string(), "acme corp, ltd".to_string()),
        ]
    );
}

#[test]
fn a_header_from_another_runtime_is_read() {
    // As runtime-go's SDK propagator writes it, with a property to ignore.
    let headers = HashMap::from([(
        BAGGAGE.to_string(),
        "tenant=acme%20corp;ttl=3, region = eu ,=nokey,bad=%zz".to_string(),
    )]);
    let received = baggage(&extract(&headers));
    assert_eq!(
        received,
        vec![
            ("region".to_string(), "eu".to_string()),
            ("tenant".to_string(), "acme corp".to_string()),
        ]
    );
}

#[test]
fn an_operation_carries_its_baggage_on_span_and_log_never_metrics() {
    let h = Harness::new();
    let observer = h.signals.observer("database");
    let parent = set_baggage(&Context::new(), "tenant", "acme");

    let op = observer.start_with(
        "postgres",
        "get",
        SpanKind::Client,
        Start {
            parent: Some(parent),
            ..Start::default()
        },
    );
    op.finish(Outcome::Ok);

    let spans = h.traces.get_finished_spans().unwrap();
    let span = spans.iter().find(|s| s.name == "database.get").unwrap();
    assert!(
        span.attributes
            .iter()
            .any(|kv| kv.key.as_str() == "baggage.tenant" && kv.value.to_string() == "acme")
    );
    // Started and completed, both carrying the baggage.
    let logs = h.logs.get_emitted_logs().unwrap();
    assert_eq!(logs.len(), 2);
    assert!(
        logs[1]
            .record
            .attributes_iter()
            .any(|(k, v)| k.as_str() == "baggage.tenant" && format!("{v:?}").contains("acme"))
    );
    let measurements = h.measurements();
    let count = measurements
        .iter()
        .find(|m| m.name == "runtime.operation.count")
        .unwrap();
    assert_eq!(count.value, 1.0);
    for m in &measurements {
        assert!(
            !m.attributes.contains_key("baggage.tenant"),
            "{} has baggage",
            m.name
        );
    }
}
