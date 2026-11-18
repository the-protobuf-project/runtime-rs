//! Cloneable signal handles; the application retains provider ownership.
use crate::{LogSink, Observer, logging::OtelLog};
use opentelemetry::{
    global::BoxedTracer,
    logs::Logger,
    metrics::Meter,
    trace::{Span, Tracer},
};
use std::sync::{Arc, RwLock};

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

    /// The tracer, for a component whose spans are named by the call they cover
    /// (a gRPC method) rather than by a bounded operation.
    pub fn tracer(&self) -> Option<Arc<BoxedTracer>> {
        self.tracer.clone()
    }

    /// Snapshots OpenTelemetry's global meter and tracer under `scope`, for a
    /// component that has always reported through whatever the application
    /// installed globally. An explicit opt-in: no other constructor here reads
    /// a global, and this one installs none.
    pub fn global(scope: impl Into<std::borrow::Cow<'static, str>>) -> Self {
        let scope = opentelemetry::InstrumentationScope::builder(scope).build();
        Self::default()
            .with_meter(opentelemetry::global::meter_with_scope(scope.clone()))
            .with_tracer(opentelemetry::global::tracer_with_scope(scope))
    }

    /// The meter, for instruments a component needs beyond the shared ones.
    pub fn meter(&self) -> Option<&Meter> {
        self.meter.as_ref()
    }

    /// Whether any signal has explicitly been supplied.
    pub fn is_enabled(&self) -> bool {
        self.meter.is_some() || self.tracer.is_some() || self.logs.is_some()
    }

    /// Creates reusable instruments for a fixed runtime component name.
    pub fn observer(&self, component: &'static str) -> Observer {
        Observer::new(self.clone(), component)
    }

    /// The process's default signals ([`set_process_default`]), or the no-op
    /// when none were set: what every runtime crate reports through when it
    /// was handed no signals of its own.
    pub fn process() -> Self {
        PROCESS
            .read()
            .map(|t| t.clone().unwrap_or_default())
            .unwrap_or_default()
    }

    /// These signals when any was supplied, and [`Telemetry::process`]
    /// otherwise: what a crate reports through, given what its caller passed.
    pub fn or_process(self) -> Self {
        if self.is_enabled() {
            self
        } else {
            Self::process()
        }
    }
}

static PROCESS: RwLock<Option<Telemetry>> = RwLock::new(None);

/// Makes `signals` the process's default: every runtime crate built
/// afterwards without signals of its own (a provider, a manager, a client, a
/// server) reports through them. The application calls it once, in main,
/// before building anything; signals passed explicitly always win, and
/// `Telemetry::default()` turns the default off. No runtime crate starts an
/// exporter or a pipeline of its own without it.
pub fn set_process_default(signals: Telemetry) {
    if let Ok(mut slot) = PROCESS.write() {
        *slot = signals.is_enabled().then_some(signals);
    }
}
