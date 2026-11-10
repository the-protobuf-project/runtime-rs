//! Starts operation guards without entering thread-local context across awaits.
use crate::{
    Operation, Telemetry,
    metrics::{Metrics, labels},
};
use opentelemetry::{
    Context, KeyValue,
    trace::{Link, SpanContext, SpanKind, TraceContextExt, Tracer},
};
use std::{
    sync::Arc,
    time::{Instant, SystemTime},
};
use tracing_opentelemetry::OpenTelemetrySpanExt;

pub(crate) struct State {
    pub telemetry: Telemetry,
    pub component: &'static str,
    pub metrics: Option<Metrics>,
}

/// How [`Observer::start_with`] begins an operation; the default is
/// [`Observer::start`]'s: the current context, now, no extra attributes.
#[derive(Clone, Debug, Default)]
pub struct Start {
    /// The parent context; the current one when unset.
    pub parent: Option<Context>,
    /// When the work began, for a span opened after it did.
    pub time: Option<SystemTime>,
    /// Span-only attributes. Metric labels stay the bounded four.
    pub attributes: Vec<KeyValue>,
    /// Producer spans this one follows from; at most 32 are kept.
    pub links: Vec<SpanContext>,
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

    /// The meter, for instruments a component needs beyond the shared ones.
    pub fn meter(&self) -> Option<&opentelemetry::metrics::Meter> {
        self.0.telemetry.meter.as_ref()
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
        self.start_with(
            backend,
            operation,
            kind,
            Start {
                links: links.to_vec(),
                ..Start::default()
            },
        )
    }

    /// Starts one operation as `start` says: under an explicit parent (a remote
    /// one [`extract`](crate::extract)ed from a message or request), at a time
    /// that has already passed, with span-only attributes and links.
    pub fn start_with(
        &self,
        backend: &'static str,
        operation: &'static str,
        kind: SpanKind,
        start: Start,
    ) -> Operation {
        let parent = match start.parent {
            Some(parent) => parent,
            None => {
                let mut parent = Context::current();
                if !parent.span().span_context().is_valid() && self.0.telemetry.tracer.is_some() {
                    parent = tracing::Span::current().context();
                }
                parent
            }
        };
        let attributes = labels(self.0.component, backend, operation);
        let context = if let Some(tracer) = &self.0.telemetry.tracer {
            let links = start
                .links
                .into_iter()
                .filter(|sc| sc.is_valid())
                .take(32)
                .map(|sc| Link::new(sc, Vec::new(), 0))
                .collect();
            let mut builder = tracer
                .span_builder(format!("{}.{}", self.0.component, operation))
                .with_kind(kind)
                .with_attributes(attributes.iter().cloned().chain(start.attributes))
                .with_links(links);
            if let Some(time) = start.time {
                builder = builder.with_start_time(time);
            }
            let span = builder.start_with_context(tracer.as_ref(), &parent);
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
