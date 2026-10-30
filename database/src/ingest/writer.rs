//! Bounded queue admission, explicit durability barriers and draining shutdown.
use super::{
    IngestConfig,
    worker::{self, Command, Failure, Outcome},
};
use crate::{Error, Record, Resource, Result, TimeSeries};
use std::sync::{Arc, Mutex};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

/// A single background batch worker. Share by reference/Arc for concurrent
/// producers. Write success confirms admission only; flush/close confirm writes.
/// A terminal failure stops admission, drops uncommitted queued records, and is
/// returned by flush/close. Partial server writes remain possible.
///
/// Dropping closes the queue and lets the worker drain while Tokio is alive.
/// Call close explicitly to wait and observe errors. Cancelling close does not
/// abort the worker: it still owns and drains the admitted queue.
pub struct BatchWriter {
    resource: Resource,
    sender: mpsc::Sender<Command>,
    task: JoinHandle<Outcome>,
    failure: Failure,
    max_record_bytes: usize,
}

impl BatchWriter {
    /// Starts a worker on the active Tokio runtime; no backend request yet.
    pub fn new(
        series: Arc<dyn TimeSeries>,
        resource: Resource,
        config: IngestConfig,
    ) -> Result<Self> {
        config.validate()?;
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| Error::Invalid("batch writer needs a Tokio runtime".into()))?;
        let (sender, receiver) = mpsc::channel(config.queue_capacity);
        let failure = Arc::new(Mutex::new(None));
        let max_record_bytes = config.max_record_bytes;
        let task = runtime.spawn(worker::start(
            series,
            resource.clone(),
            config,
            receiver,
            failure.clone(),
        ));
        Ok(Self {
            resource,
            sender,
            task,
            failure,
            max_record_bytes,
        })
    }

    fn stopped(&self) -> Error {
        self.failure
            .lock()
            .expect("ingestion failure lock")
            .as_ref()
            .map(|error| Error::Ingestion(error.clone()))
            .unwrap_or(Error::Closed)
    }

    /// Validates and enqueues a point, waiting for capacity. Cancellation before
    /// send completes does not admit the point. This is not a server receipt.
    pub async fn write(&self, record: Record) -> Result<()> {
        self.resource.validate(&record)?;
        if serde_json::to_vec(&record)?.len() > self.max_record_bytes {
            return Err(Error::Invalid("point exceeds ingestion byte limit".into()));
        }
        self.sender
            .send(Command::Append(record))
            .await
            .map_err(|_| self.stopped())
    }

    /// Waits for every record admitted before this barrier to reach the server.
    /// Concurrent producers admitted after it are covered by a later barrier.
    pub async fn flush(&self) -> Result<()> {
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

    /// Ends admission, drains every admitted point, and reports worker failure.
    /// A cancelled waiter leaves the worker running until its queue is drained.
    pub async fn close(self) -> Result<()> {
        let Self { sender, task, .. } = self;
        drop(sender);
        task.await
            .map_err(|source| Error::Backend {
                backend: "ingestion",
                source: Box::new(source),
            })?
            .map_err(Error::Ingestion)
    }
}
