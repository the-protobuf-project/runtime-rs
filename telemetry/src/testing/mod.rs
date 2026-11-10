//! In-memory signals for other crates' tests, behind the `testing` feature.
//!
//! A [`Recorder`] is a [`Telemetry`] whose spans, metrics and logs land in
//! memory, so a crate checks what its instrumentation reported without an
//! exporter, a collector or a global provider, and without depending on the
//! OpenTelemetry SDK itself.
mod metrics;

use crate::Telemetry;
use opentelemetry::{
    logs::LoggerProvider,
    metrics::MeterProvider,
    trace::{SpanKind, Status, TracerProvider},
};
use opentelemetry_sdk::{
    logs::{InMemoryLogExporter, SdkLoggerProvider},
    metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider},
    trace::{InMemorySpanExporter, SdkTracerProvider, SpanData},
};
use std::collections::{BTreeMap, HashMap};

/// Records everything reported through [`Recorder::signals`].
pub struct Recorder {
    signals: Telemetry,
    traces: InMemorySpanExporter,
    logs: InMemoryLogExporter,
    metrics: InMemoryMetricExporter,
    trace_provider: SdkTracerProvider,
    log_provider: SdkLoggerProvider,
    meter_provider: SdkMeterProvider,
}

/// One ended span, in plain types.
#[derive(Clone, Debug)]
pub struct Span {
    /// `component.operation`.
    pub name: String,
    /// Client, server, producer, consumer or internal.
    pub kind: SpanKind,
    /// 32 hex digits.
    pub trace_id: String,
    /// 16 hex digits.
    pub span_id: String,
    /// The parent's span id; empty for a root span.
    pub parent_span_id: String,
    /// Whether the status is an error.
    pub failed: bool,
    /// Every attribute, values as strings.
    pub attributes: HashMap<String, String>,
    /// Event names, in order.
    pub events: Vec<String>,
    /// How many spans it links to.
    pub links: usize,
}

/// One recorded data point: a sum's value, or a histogram's count.
#[derive(Clone, Debug)]
pub struct Measurement {
    /// The instrument's name.
    pub name: String,
    /// A sum's value, or a histogram's count.
    pub value: f64,
    /// The data point's labels.
    pub attributes: BTreeMap<String, String>,
}

impl Default for Recorder {
    fn default() -> Self {
        Self::new()
    }
}

impl Recorder {
    /// A recorder with all three signals wired.
    pub fn new() -> Self {
        let traces = InMemorySpanExporter::default();
        let logs = InMemoryLogExporter::default();
        let metrics = InMemoryMetricExporter::default();
        let trace_provider = SdkTracerProvider::builder()
            .with_simple_exporter(traces.clone())
            .build();
        let log_provider = SdkLoggerProvider::builder()
            .with_simple_exporter(logs.clone())
            .build();
        let meter_provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(metrics.clone()).build())
            .build();
        let signals = Telemetry::default()
            .with_meter(meter_provider.meter("runtime-test"))
            .with_tracer(trace_provider.tracer("runtime-test"))
            .with_logger(log_provider.logger("runtime-test"));
        Self {
            signals,
            traces,
            logs,
            metrics,
            trace_provider,
            log_provider,
            meter_provider,
        }
    }

    /// The signals to hand the code under test.
    pub fn signals(&self) -> Telemetry {
        self.signals.clone()
    }

    /// Every span ended so far, in the order they ended.
    pub fn spans(&self) -> Vec<Span> {
        let _ = self.trace_provider.force_flush();
        self.traces
            .get_finished_spans()
            .unwrap_or_default()
            .into_iter()
            .map(plain)
            .collect()
    }

    /// The first ended span named `name`.
    pub fn span(&self, name: &str) -> Option<Span> {
        self.spans().into_iter().find(|s| s.name == name)
    }

    /// How many log records were emitted.
    pub fn log_count(&self) -> usize {
        let _ = self.log_provider.force_flush();
        self.logs.get_emitted_logs().map(|l| l.len()).unwrap_or(0)
    }

    /// Every data point recorded so far.
    pub fn measurements(&self) -> Vec<Measurement> {
        let _ = self.meter_provider.force_flush();
        metrics::collect(&self.metrics)
    }

    /// Maps each operation counted in `runtime.operation.count` to its outcome.
    pub fn outcomes(&self) -> HashMap<String, String> {
        self.measurements()
            .into_iter()
            .filter(|m| m.name == "runtime.operation.count")
            .filter_map(|m| {
                Some((
                    m.attributes.get("operation")?.clone(),
                    m.attributes.get("outcome")?.clone(),
                ))
            })
            .collect()
    }
}

fn plain(s: SpanData) -> Span {
    let parent = s.parent_span_id;
    Span {
        name: s.name.to_string(),
        kind: s.span_kind,
        trace_id: s.span_context.trace_id().to_string(),
        span_id: s.span_context.span_id().to_string(),
        parent_span_id: if parent == opentelemetry::trace::SpanId::INVALID {
            String::new()
        } else {
            parent.to_string()
        },
        failed: matches!(s.status, Status::Error { .. }),
        attributes: s
            .attributes
            .iter()
            .map(|kv| (kv.key.to_string(), kv.value.to_string()))
            .collect(),
        events: s.events.iter().map(|e| e.name.to_string()).collect(),
        links: s.links.len(),
    }
}
