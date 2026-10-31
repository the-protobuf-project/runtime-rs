//! Stream observations continue until EOF, error or drop.
use super::{Observed, outcome, stream::ObservedStream};
use crate::store::{RecordStream, Resource, Result, Scan, SeriesReader};
use telemetry::{FutureExt, SpanKind};

#[async_trait::async_trait]
impl SeriesReader for Observed<dyn SeriesReader> {
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
