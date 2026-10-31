//! Each background batch links bounded producer spans and owns its write budget.
use super::{IngestConfig, queue::Wiring, worker::Outcome};
use crate::store::{Batcher, Error, Record, Resource};
use std::sync::Arc;
use telemetry::{Context, FutureExt, SpanKind, opentelemetry::trace::TraceContextExt};
use tokio::time::timeout;

/// Writes one batch. Owns everything it uses so several can be in flight.
pub(super) async fn flush(
    batch: Arc<dyn Batcher>,
    resource: Resource,
    entries: Vec<(Record, Context)>,
    config: IngestConfig,
    wiring: Wiring,
) -> Outcome {
    if entries.is_empty() {
        return Ok(());
    }
    let mut links = Vec::new();
    for (_, context) in &entries {
        let sc = context.span().span_context().clone();
        if sc.is_valid() && !links.contains(&sc) {
            links.push(sc);
        }
        if links.len() == 32 {
            break;
        }
    }
    let operation =
        wiring
            .observer
            .start_linked(wiring.backend, "batch_write", SpanKind::Internal, &links);
    let points = entries
        .into_iter()
        .map(|(record, _)| record)
        .collect::<Vec<_>>();
    let result = timeout(config.write_timeout, batch.create_many(&resource, &points))
        .with_context(operation.context())
        .await
        .map_err(|_| Arc::new(Error::Timeout))
        .and_then(|result| result.map(drop).map_err(Arc::new));
    operation.finish(crate::observe::outcome(
        result.as_ref().err().map(Arc::as_ref),
    ));
    result
}
