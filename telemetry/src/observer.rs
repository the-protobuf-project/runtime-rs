//! Starts operation guards without entering thread-local context across awaits.
use crate::{
    Operation, Telemetry,
    metrics::{Metrics, labels},
};
use opentelemetry::{
    Context, KeyValue,
    trace::{Link, SpanContext, SpanKind, TraceContextExt, Tracer},
};
use std::{sync::Arc, time::Instant};
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub(crate) struct State {
    pub telemetry: Telemetry,
    pub component: &'static str,
    pub metrics: Option<Metrics>,
}

/// Cheap, shareable component observer; no tasks or lifecycle ownership.
#[derive(Clone)]
pub struct Observer(pub(crate) Arc<State>);
impl Observer {
    pub(crate) fn new(telemetry: Telemetry, component: &'static str) -> Self {
        let metrics = telemetry.meter.as_ref().map(Metrics::new);
        Self(Arc::new(State {
            telemetry,
            component,
            metrics,
        }))
    }

    /// Whether this observer has any configured signal.
    pub fn is_enabled(&self) -> bool {
        self.0.telemetry.is_enabled()
    }

    /// Starts one operation under the active OTel/tracing parent context.
    pub fn start(
        &self,
        backend: &'static str,
        operation: &'static str,
        kind: SpanKind,
    ) -> Operation {
        self.start_linked(backend, operation, kind, &[])
    }

    /// Links at most 32 producer contexts to a background batch operation.
    pub fn start_linked(
        &self,
        backend: &'static str,
        operation: &'static str,
        kind: SpanKind,
        links: &[SpanContext],
    ) -> Operation {
        let mut parent = Context::current();
        if !parent.span().span_context().is_valid() && self.0.telemetry.tracer.is_some() {
            parent = tracing::Span::current().context();
        }
        let attributes = labels(self.0.component, backend, operation);
        let context = if let Some(tracer) = &self.0.telemetry.tracer {
            let links = links
                .iter()
                .filter(|sc| sc.is_valid())
                .take(32)
                .cloned()
                .map(|sc| Link::new(sc, Vec::new(), 0))
                .collect();
            let span = tracer
                .span_builder(format!("{}.{}", self.0.component, operation))
                .with_kind(kind)
                .with_attributes(attributes.clone())
                .with_links(links)
                .start_with_context(tracer.as_ref(), &parent);
            parent.with_span(span)
        } else {
            parent
        };
        if let Some(metrics) = &self.0.metrics {
            metrics.active.add(1, &attributes);
        }
        Operation {
            observer: self.clone(),
            context,
            attributes,
            started: Instant::now(),
            finished: false,
            read: 0,
            written: 0,
        }
    }

    /// Changes the number of admitted points still waiting in an ingest queue.
    /// Queue lease guards must balance every increment, including failure drops.
    pub fn queue_delta(&self, backend: &'static str, delta: i64) {
        if let Some(metrics) = &self.0.metrics {
            metrics.queue.add(
                delta,
                &[
                    KeyValue::new("component", self.0.component),
                    KeyValue::new("backend", backend),
                ],
            );
        }
    }
}
