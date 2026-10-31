//! A guard ends spans and balances active operations on every exit path.
use crate::{Observer, Outcome, Severity};
use opentelemetry::{
    Context, KeyValue,
    trace::{Status, TraceContextExt},
};
use std::time::Instant;

/// An in-flight operation. Drop reports cancellation (or panic) exactly once.
/// Attach its context with FutureExt::with_context, not Context::attach across
/// await. No raw error strings or application payloads are ever accepted here.
pub struct Operation {
    pub(crate) observer: Observer,
    pub(crate) context: Context,
    pub(crate) attributes: Vec<KeyValue>,
    pub(crate) started: Instant,
    pub(crate) finished: bool,
    pub(crate) read: u64,
    pub(crate) written: u64,
}
impl Operation {
    /// Context to attach per future poll or link from another task.
    pub fn context(&self) -> Context {
        self.context.clone()
    }
    /// Counts successfully delivered records, including partial stream results.
    pub fn read_rows(&mut self, rows: usize) {
        self.read = self.read.saturating_add(rows as u64);
    }
    /// Counts acknowledged records only; failed writes can have unknown outcomes.
    pub fn written_rows(&mut self, rows: usize) {
        self.written = self.written.saturating_add(rows as u64);
    }
    /// Completes this operation and prevents Drop from reporting it again.
    pub fn finish(mut self, outcome: Outcome) {
        self.complete(outcome);
    }

    fn complete(&mut self, outcome: Outcome) {
        if self.finished {
            return;
        }
        self.finished = true;
        let _attached = self.context.clone().attach();
        let duration = self.started.elapsed().as_secs_f64();
        if let Some(metrics) = &self.observer.0.metrics {
            metrics.active.add(-1, &self.attributes);
            let mut attrs = self.attributes.clone();
            attrs.push(KeyValue::new("outcome", outcome.as_str()));
            metrics.count.add(1, &attrs);
            metrics.duration.record(duration, &attrs);
            if self.read > 0 {
                metrics.read.add(self.read, &attrs);
            }
            if self.written > 0 {
                metrics.written.add(self.written, &attrs);
            }
        }
        let mut attributes = self.attributes.clone();
        attributes.push(KeyValue::new("outcome", outcome.as_str()));
        attributes.push(KeyValue::new("duration_seconds", duration));
        attributes.push(KeyValue::new("records_read", self.read as i64));
        attributes.push(KeyValue::new("records_written", self.written as i64));
        let span = self.context.span();
        // Without an injected tracer, never modify/end the caller's parent span.
        if self.observer.0.telemetry.tracer.is_some() {
            span.set_attribute(KeyValue::new("outcome", outcome.as_str()));
            if outcome.is_error() {
                span.set_status(Status::error(outcome.as_str()));
            }
        }
        if let Some(logs) = &self.observer.0.telemetry.logs {
            let level = if outcome.is_error() {
                Severity::Error
            } else if matches!(outcome, Outcome::Conflict | Outcome::NotFound) {
                Severity::Info
            } else {
                Severity::Debug
            };
            logs.emit(
                &self.context,
                level,
                "runtime.operation.completed",
                &attributes,
            );
        }
        if self.observer.0.telemetry.tracer.is_some() {
            span.end();
        }
    }
}
impl Drop for Operation {
    fn drop(&mut self) {
        self.complete(if std::thread::panicking() {
            Outcome::Panic
        } else {
            Outcome::Cancelled
        });
    }
}
