//! Cloneable signal handles; the application retains provider ownership.
use crate::{LogSink, Observer, logging::OtelLog};
use opentelemetry::{
    global::BoxedTracer,
    logs::Logger,
    metrics::Meter,
    trace::{Span, Tracer},
};
use std::sync::Arc;

/// Shared signal bindings. Default is a true no-op; nothing starts exporters,
/// reads environment configuration, installs globals or shuts down providers.
#[derive(Clone, Default)]
pub struct Telemetry {
    pub(crate) meter: Option<Meter>,
    pub(crate) tracer: Option<Arc<BoxedTracer>>,
    pub(crate) logs: Option<Arc<dyn LogSink>>,
}

impl Telemetry {
    /// Uses an application-owned meter; instruments are created per Observer.
    pub fn with_meter(mut self, meter: Meter) -> Self {
        self.meter = Some(meter);
        self
    }

    /// Uses an application-owned tracer without changing the global provider.
    pub fn with_tracer<T>(mut self, tracer: T) -> Self
    where
        T: Tracer + Send + Sync + 'static,
        T::Span: Span + Send + Sync + 'static,
    {
        self.tracer = Some(Arc::new(BoxedTracer::new(Box::new(tracer))));
        self
    }

    /// Uses an OpenTelemetry logger with explicit trace/span ID correlation.
    pub fn with_logger<L: Logger + Send + Sync + 'static>(mut self, logger: L) -> Self {
        self.logs = Some(Arc::new(OtelLog(logger)));
        self
    }

    /// Supplies a custom sink, e.g. the linked telemetry SDK's logging adapter.
    pub fn with_log_sink(mut self, sink: Arc<dyn LogSink>) -> Self {
        self.logs = Some(sink);
        self
    }

    /// Whether any signal has explicitly been supplied.
    pub fn is_enabled(&self) -> bool {
        self.meter.is_some() || self.tracer.is_some() || self.logs.is_some()
    }

    /// Creates reusable instruments for a fixed runtime component name.
    pub fn observer(&self, component: &'static str) -> Observer {
        Observer::new(self.clone(), component)
    }
}
