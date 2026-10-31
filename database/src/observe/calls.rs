//! One operation guard surrounds the entire awaited capability call.
use super::outcome;
use crate::store::Result;
use std::future::Future;
use telemetry::{FutureExt, Observer, SpanKind};

pub(crate) async fn run<T>(
    observer: &Observer,
    backend: &'static str,
    name: &'static str,
    future: impl Future<Output = Result<T>>,
    written: usize,
    read: fn(&T) -> usize,
) -> Result<T> {
    if !observer.is_enabled() {
        return future.await;
    }
    let mut operation = observer.start(backend, name, SpanKind::Client);
    let result = future.with_context(operation.context()).await;
    if let Ok(value) = &result {
        operation.read_rows(read(value));
        operation.written_rows(written);
    }
    operation.finish(outcome(result.as_ref().err()));
    result
}
