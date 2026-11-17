//! An in-memory records driver that counts what is asked of it, so a test asserts how many
//! times the backing store was reached rather than only that the right value came back: that
//! count is the entire point of a cache. The `fakeStore` of runtime-go's `cached_test.go`.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use database::records::{
    Batcher, Capabilities, DB, Driver, ListOptions, ListResult, Resource, Transactional, TxWork,
    WriteResult, key_of,
};
use database::{Error, Result};
use prost::Message;
use prost_reflect::DynamicMessage;
use tokio::sync::{Notify, Semaphore};

/// Records by key, as wire bytes, with a count of `get`s and gates to hold one.
#[derive(Default)]
pub struct MemoryRecords {
    recs: Mutex<HashMap<String, Vec<u8>>>,
    /// How many times `get` was called.
    pub gets: AtomicUsize,
    /// Signalled when a `get` arrives.
    pub arrived: Notify,
    gate: Mutex<Option<Arc<Semaphore>>>,
    /// When set, `get` fails with [`Error::Timeout`].
    pub broken: AtomicBool,
}

impl MemoryRecords {
    /// An empty store, shared.
    pub fn new() -> Arc<Self> {
        Arc::default()
    }

    /// Holds every later `get` until the returned semaphore is closed.
    pub fn hold(&self) -> Arc<Semaphore> {
        let gate = Arc::new(Semaphore::new(0));
        *self.gate.lock().unwrap() = Some(gate.clone());
        gate
    }

    /// How many `get`s reached the store.
    pub fn gets(&self) -> usize {
        self.gets.load(Ordering::SeqCst)
    }

    fn put(&self, res: &Resource, msg: &DynamicMessage, exists: bool) -> Result<String> {
        let key = key_of(res, msg)?;
        let mut recs = self.recs.lock().unwrap();
        match (recs.contains_key(&key), exists) {
            (true, false) => return Err(Error::AlreadyExists),
            (false, true) => return Err(Error::NotFound),
            _ => {}
        }
        recs.insert(key.clone(), msg.encode_to_vec());
        Ok(key)
    }
}

#[async_trait::async_trait]
impl Driver for MemoryRecords {
    async fn create(&self, res: &Resource, msg: &DynamicMessage) -> Result<WriteResult> {
        self.put(res, msg, false)?;
        Ok(WriteResult::done(msg.clone()))
    }

    async fn get(&self, res: &Resource, key: &str) -> Result<DynamicMessage> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.arrived.notify_one();
        let gate = self.gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            let _ = gate.acquire().await;
        }
        if self.broken.load(Ordering::SeqCst) {
            return Err(Error::Timeout);
        }
        let body = self.recs.lock().unwrap().get(key).cloned();
        let body = body.ok_or(Error::NotFound)?;
        DynamicMessage::decode(res.message.clone(), body.as_slice())
            .map_err(|e| Error::Invalid(e.to_string()))
    }

    async fn update(&self, res: &Resource, msg: &DynamicMessage) -> Result<WriteResult> {
        self.put(res, msg, true)?;
        Ok(WriteResult::done(msg.clone()))
    }

    async fn delete(&self, _: &Resource, key: &str) -> Result<()> {
        let removed = self.recs.lock().unwrap().remove(key);
        removed.map(|_| ()).ok_or(Error::NotFound)
    }

    async fn list(&self, _: &Resource, _: &ListOptions) -> Result<ListResult> {
        Ok(ListResult::default())
    }

    async fn count(&self, _: &Resource, _: &ListOptions) -> Result<i64> {
        Ok(self.recs.lock().unwrap().len() as i64)
    }

    async fn exists(&self, _: &Resource, key: &str) -> Result<bool> {
        Ok(self.recs.lock().unwrap().contains_key(key))
    }
}

#[async_trait::async_trait]
impl Batcher for MemoryRecords {
    async fn create_many(
        &self,
        res: &Resource,
        msgs: &[DynamicMessage],
    ) -> Result<Vec<WriteResult>> {
        let mut out = Vec::new();
        for msg in msgs {
            out.push(self.create(res, msg).await?);
        }
        Ok(out)
    }

    async fn get_many(
        &self,
        res: &Resource,
        keys: &[String],
    ) -> Result<Vec<Option<DynamicMessage>>> {
        let mut out = Vec::new();
        for key in keys {
            out.push(self.get(res, key).await.ok());
        }
        Ok(out)
    }
}

/// Runs the body against the store directly, with bulk writes: enough to exercise the
/// decorator's invalidation, which is what is under test rather than transaction semantics.
/// Go's `fakeTx`.
pub struct DirectTx(pub Arc<MemoryRecords>);

#[async_trait::async_trait]
impl Transactional for DirectTx {
    async fn run(&self, work: TxWork) -> Result<()> {
        let store = self.0.clone();
        let caps = Capabilities {
            batch: Some(store.clone()),
            ..Capabilities::new(store)
        };
        work(&DB::build("fake", "tx", caps, None)).await
    }
}

/// The store alone, with no capability: Go's `store.Build(backing, "fake", "test", nil)`.
pub fn plain(store: Arc<MemoryRecords>) -> DB {
    DB::build("fake", "test", Capabilities::new(store), None)
}

/// The store with transactions and bulk writes.
pub fn capable(store: Arc<MemoryRecords>) -> DB {
    let caps = Capabilities {
        tx: Some(Arc::new(DirectTx(store.clone()))),
        batch: Some(store.clone()),
        ..Capabilities::new(store)
    };
    DB::build("fake", "test", caps, None)
}
