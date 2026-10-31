//! Adapter to the application's telemetry-rs SDK owner; never initializes it.
use crate::{LogSink, Severity, Telemetry};
use opentelemetry::{Context, KeyValue, metrics::MeterProvider};
use std::sync::Arc;
use telemetry_sdk::{Telemetry as Sdk, logging::OtelLogger, telemetry::TelemetryProvider};

struct TppLogs(Arc<OtelLogger>);
impl LogSink for TppLogs {
    fn emit(&self, context: &Context, level: Severity, event: &'static str, attrs: &[KeyValue]) {
        let _attached = context.clone().attach();
        self.0.log(level, event, attrs.to_vec());
    }
}

impl Telemetry {
    /// Adapts explicit SDK provider handles without consulting global providers.
    /// Caller owns this provider's lifetime, flushing and shutdown.
    pub fn from_tpp_provider(provider: &TelemetryProvider, scope: &'static str) -> Self {
        let mut signals = Self::default();
        if let Some(meter) = provider.meter_provider() {
            signals = signals.with_meter(meter.meter(scope));
        }
        if let Some(tracer) = provider.get_tracer(scope) {
            signals = signals.with_tracer(tracer);
        }
        if let Some(logger) = provider.get_logger(scope) {
            signals = signals.with_log_sink(Arc::new(TppLogs(Arc::new(logger))));
        }
        signals
    }

    /// Adapts the SDK's high-level Telemetry owner. Its tracing builder installs
    /// a global tracer; this explicit opt-in snapshots that tracer. Applications
    /// with multiple tracer providers should use from_tpp_provider instead.
    pub fn from_tpp(owner: &Sdk, scope: &'static str) -> Self {
        let mut signals = Self::default();
        if let Some(meter) = owner.meter_provider() {
            signals = signals.with_meter(meter.meter(scope));
        }
        if owner
            .tracing
            .as_ref()
            .is_some_and(|tracing| tracing.is_enabled())
        {
            signals = signals.with_tracer(opentelemetry::global::tracer(scope));
        }
        if let Some(logger) = owner.logger.otel_logger_arc() {
            signals = signals.with_log_sink(Arc::new(TppLogs(logger)));
        }
        signals
    }
}
