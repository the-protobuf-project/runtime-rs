//! Controllable storage to test queue bounds and shutdown under real failures.
use database::{Aggregate, Buckets, Error, Range, Record, Resource, Result, TimeSeries};
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, Semaphore};

#[derive(Default)]
pub struct Sink {
    pub batches: Mutex<Vec<Vec<Record>>>,
    pub started: Notify,
    pub committed: Notify,
    pub gate: Option<Arc<Semaphore>>,
    pub fail: bool,
}

#[async_trait::async_trait]
impl TimeSeries for Sink {
    async fn write(&self, _: &Resource, records: &[Record]) -> Result<()> {
        self.started.notify_one();
        if let Some(gate) = &self.gate {
            gate.acquire().await.unwrap().forget();
        }
        if self.fail {
            return Err(Error::Invalid("injected write failure".into()));
        }
        self.batches.lock().unwrap().push(records.to_vec());
        self.committed.notify_one();
        Ok(())
    }
    async fn range(&self, _: &Resource, _: &Range) -> Result<Vec<Record>> {
        unreachable!()
    }
    async fn aggregate(&self, _: &Resource, _: &Aggregate) -> Result<Buckets> {
        unreachable!()
    }
}
