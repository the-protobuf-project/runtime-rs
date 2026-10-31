//! Each background batch links bounded producer spans and owns its write budget.
use super::{IngestConfig, queue::Wiring, worker::Outcome};
use crate::{Error, Record, Resource, TimeSeries};
use std::sync::Arc;
use telemetry::{Context, FutureExt, SpanKind, opentelemetry::trace::TraceContextExt};
use tokio::time::timeout;

pub(super) async fn flush(
    series: &dyn TimeSeries,
    resource: &Resource,
    pending: &mut Vec<(Record, Context)>,
    config: &IngestConfig,
    wiring: &Wiring,
) -> Outcome {
    if pending.is_empty() {
        return Ok(());
    }
    let entries = std::mem::take(pending);
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
    let result = timeout(config.write_timeout, series.write(resource, &points))
        .with_context(operation.context())
        .await
        .map_err(|_| Arc::new(Error::Timeout))
        .and_then(|result| result.map_err(Arc::new));
    operation.finish(crate::observe::outcome(
        result.as_ref().err().map(Arc::as_ref),
    ));
    result
}
