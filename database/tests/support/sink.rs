//! Controllable storage to test queue bounds and shutdown under real failures.
use database::store::{Batcher, Error, Record, Resource, Result, WriteResult};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, Semaphore};

#[derive(Default)]
pub struct Sink {
    pub batches: Mutex<Vec<Vec<Record>>>,
    pub started: Notify,
    pub committed: Notify,
    pub gate: Option<Arc<Semaphore>>,
    pub fail: bool,
    /// Batches that have started writing, finished or not.
    pub entered: AtomicUsize,
}

#[async_trait::async_trait]
impl Batcher for Sink {
    async fn create_many(&self, _: &Resource, records: &[Record]) -> Result<Vec<WriteResult>> {
        self.entered.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        if let Some(gate) = &self.gate {
            gate.acquire().await.unwrap().forget();
        }
        if self.fail {
            return Err(Error::Invalid("injected write failure".into()));
        }
        self.batches.lock().unwrap().push(records.to_vec());
        self.committed.notify_one();
        Ok(Vec::new())
    }
    async fn get_many(&self, _: &Resource, _: &[String]) -> Result<Vec<Record>> {
        unreachable!()
    }
}
