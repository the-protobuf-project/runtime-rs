//! Pages complete on return; stream observations continue until EOF/error/drop.
use super::{Observed, outcome, run, stream::ObservedStream};
use crate::{Page, Range, RecordStream, Resource, Result, Scan, SeriesReader};
use telemetry::{FutureExt, SpanKind};

#[async_trait::async_trait]
impl SeriesReader for Observed<dyn SeriesReader> {
    async fn page(&self, res: &Resource, range: &Range, token: Option<&str>) -> Result<Page> {
        run(
            &self.observer,
            self.backend,
            "page",
            self.next.page(res, range, token),
            0,
            |page| page.items.len(),
        )
        .await
    }
    async fn stream(&self, res: &Resource, scan: &Scan) -> Result<RecordStream> {
        let operation = self
            .observer
            .start(self.backend, "stream", SpanKind::Client);
        match self
            .next
            .stream(res, scan)
            .with_context(operation.context())
            .await
        {
            Ok(inner) => Ok(Box::pin(ObservedStream {
                inner,
                operation: Some(operation),
                ended: false,
            })),
            Err(error) => {
                operation.finish(outcome(Some(&error)));
                Err(error)
            }
        }
    }
}
