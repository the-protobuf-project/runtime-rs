//! Bounded queue admission, explicit durability barriers and draining shutdown.
use super::queue::Wiring;
use super::{
    IngestConfig,
    worker::{self, Command, Failure, Outcome},
};
use crate::store::{Batcher, Error, Resource, Result};
use std::sync::{Arc, Mutex};
use tokio::{sync::mpsc, task::JoinHandle};

/// A single background batch worker. Share by reference/Arc for concurrent
/// producers. Write success confirms admission only; flush/close confirm writes.
/// A terminal failure stops admission, drops uncommitted queued records, and is
/// returned by flush/close. Partial server writes remain possible.
///
/// Dropping closes the queue and lets the worker drain while Tokio is alive.
/// Call close explicitly to wait and observe errors. Cancelling close does not
/// abort the worker: it still owns and drains the admitted queue.
pub struct BatchWriter {
    pub(super) resource: Resource,
    pub(super) sender: mpsc::Sender<Command>,
    pub(super) task: JoinHandle<Outcome>,
    pub(super) failure: Failure,
    pub(super) max_record_bytes: usize,
    pub(super) wiring: Wiring,
}

impl BatchWriter {
    /// Starts a worker on the active Tokio runtime; no backend request yet.
    pub fn new(batch: Arc<dyn Batcher>, resource: Resource, config: IngestConfig) -> Result<Self> {
        Self::observed(
            batch,
            resource,
            config,
            telemetry::Telemetry::default(),
            "custom",
        )
    }

    pub(crate) fn observed(
        batch: Arc<dyn Batcher>,
        resource: Resource,
        config: IngestConfig,
        signals: telemetry::Telemetry,
        backend: &'static str,
    ) -> Result<Self> {
        config.validate()?;
        let wiring = Wiring {
            observer: signals.observer("database_ingestion"),
            backend,
        };
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| Error::Invalid("batch writer needs a Tokio runtime".into()))?;
        let (sender, receiver) = mpsc::channel(config.queue_capacity);
        let failure = Arc::new(Mutex::new(None));
        let max_record_bytes = config.max_record_bytes;
        let task = runtime.spawn(worker::start(
            batch,
            resource.clone(),
            config,
            receiver,
            failure.clone(),
            wiring.clone(),
        ));
        Ok(Self {
            resource,
            sender,
            task,
            failure,
            max_record_bytes,
            wiring,
        })
    }

    /// The descriptor this writer is bound to.
    pub fn resource(&self) -> &Resource {
        &self.resource
    }

    pub(super) fn stopped(&self) -> Error {
        self.failure
            .lock()
            .expect("ingestion failure lock")
            .as_ref()
            .map(|error| Error::Ingestion(error.clone()))
            .unwrap_or(Error::Closed)
    }
}
