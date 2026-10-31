//! Instrument queue admission/barriers without confusing admission with commit.
use super::{BatchWriter, queue::Entry, worker::Command};
use crate::store::{Error, Record, Result};
use telemetry::{FutureExt, SpanKind};
use tokio::sync::oneshot;

impl BatchWriter {
    /// Validates and enqueues a point, waiting for capacity. Cancellation before
    /// admission does not alter queue depth. This is not a server receipt.
    pub async fn write(&self, record: Record) -> Result<()> {
        let operation =
            self.wiring
                .observer
                .start(self.wiring.backend, "enqueue", SpanKind::Internal);
        let context = operation.context();
        let result = async {
            self.resource.validate(&record)?;
            if serde_json::to_vec(&record)?.len() > self.max_record_bytes {
                return Err(Error::Invalid("point exceeds ingestion byte limit".into()));
            }
            let permit = self.sender.reserve().await.map_err(|_| self.stopped())?;
            permit.send(Command::Append(Entry::new(
                record,
                context.clone(),
                self.wiring.clone(),
            )));
            Ok(())
        }
        .with_context(context.clone())
        .await;
        operation.finish(crate::observe::outcome(result.as_ref().err()));
        result
    }

    /// Waits for server acknowledgement of records admitted before this barrier.
    pub async fn flush(&self) -> Result<()> {
        let operation =
            self.wiring
                .observer
                .start(self.wiring.backend, "flush", SpanKind::Internal);
        let result = async {
            let (sender, receiver) = oneshot::channel();
            self.sender
                .send(Command::Flush(sender))
                .await
                .map_err(|_| self.stopped())?;
            receiver
                .await
                .map_err(|_| self.stopped())?
                .map_err(Error::Ingestion)
        }
        .with_context(operation.context())
        .await;
        operation.finish(crate::observe::outcome(result.as_ref().err()));
        result
    }

    /// Ends admission and drains the worker. Cancelling this waiter does not
    /// abort the worker; shutdown and actual batch spans complete independently.
    pub async fn close(self) -> Result<()> {
        let operation =
            self.wiring
                .observer
                .start(self.wiring.backend, "close", SpanKind::Internal);
        let Self { sender, task, .. } = self;
        drop(sender);
        let result = async {
            task.await
                .map_err(|source| Error::Backend {
                    backend: "ingestion",
                    source: Box::new(source),
                })?
                .map_err(Error::Ingestion)
        }
        .with_context(operation.context())
        .await;
        operation.finish(crate::observe::outcome(result.as_ref().err()));
        result
    }
}
