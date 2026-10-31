//! Object-safe log sinks accept explicit trace context for correlation.
pub use opentelemetry::logs::Severity;
use opentelemetry::{
    Context, KeyValue,
    logs::{AnyValue, LogRecord, Logger},
    trace::TraceContextExt,
};

/// Sinks must be non-panicking and fast; SDK processors own batching/export.
pub trait LogSink: Send + Sync {
    /// Emits a bounded structured event in the supplied operation context.
    fn emit(
        &self,
        context: &Context,
        level: Severity,
        event: &'static str,
        attributes: &[KeyValue],
    );
}

pub(crate) struct OtelLog<L>(pub L);
impl<L: Logger + Send + Sync> LogSink for OtelLog<L> {
    fn emit(&self, context: &Context, level: Severity, event: &'static str, attrs: &[KeyValue]) {
        if !self.0.event_enabled(level, "tpp.runtime", Some(event)) {
            return;
        }
        let mut record = self.0.create_log_record();
        record.set_event_name(event);
        record.set_severity_number(level);
        record.set_body(AnyValue::from(event));
        record.set_timestamp(std::time::SystemTime::now());
        for attr in attrs {
            record.add_attribute(attr.key.clone(), attr.value.to_string());
        }
        let span = context.span();
        let sc = span.span_context();
        if sc.is_valid() {
            record.set_trace_context(sc.trace_id(), sc.span_id(), Some(sc.trace_flags()));
        }
        self.0.emit(record);
    }
}

/// Optional log adapter for applications using a tracing subscriber.
/// Does not install a subscriber or create duplicate tracing spans.
pub struct TracingLogs;
impl LogSink for TracingLogs {
    fn emit(&self, cx: &Context, level: Severity, event: &'static str, attrs: &[KeyValue]) {
        let span = cx.span();
        let sc = span.span_context();
        match level {
            Severity::Error => {
                tracing::error!(target: "tpp.runtime", event, trace_id = %sc.trace_id(), span_id = %sc.span_id(), attributes = ?attrs)
            }
            Severity::Warn => {
                tracing::warn!(target: "tpp.runtime", event, trace_id = %sc.trace_id(), span_id = %sc.span_id(), attributes = ?attrs)
            }
            Severity::Info => {
                tracing::info!(target: "tpp.runtime", event, trace_id = %sc.trace_id(), span_id = %sc.span_id(), attributes = ?attrs)
            }
            _ => {
                tracing::debug!(target: "tpp.runtime", event, trace_id = %sc.trace_id(), span_id = %sc.span_id(), attributes = ?attrs)
            }
        }
    }
}
