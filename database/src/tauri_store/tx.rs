//! Transactions over an in-memory map: hold the store's lock, remember the selected
//! database's keys, run the work, then save once on success or put the keys back on failure.
//! Other writers wait for the lock, so the work is isolated and its writes reach the file
//! together or not at all.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::Value as Json;
use tauri::Runtime;

use super::TauriStoreDriver;
use super::driver::Inner;
use crate::Result;
use crate::records::{Capabilities, DB, Transactional, TxWork};

/// The selected database's keys and values at one moment.
pub(super) struct Snapshot(HashMap<String, Json>);

impl<R: Runtime> Inner<R> {
    pub(super) fn snapshot(&self) -> Snapshot {
        let head = self.keys.database();
        Snapshot(
            self.store
                .entries()
                .into_iter()
                .filter(|(k, _)| k.starts_with(head))
                .collect(),
        )
    }

    /// Puts the selected database back as `snap` found it.
    pub(super) fn restore(&self, snap: Snapshot) {
        let head = self.keys.database();
        for key in self
            .store
            .keys()
            .into_iter()
            .filter(|k| k.starts_with(head))
        {
            if !snap.0.contains_key(&key) {
                self.store.delete(key);
            }
        }
        for (key, value) in snap.0 {
            if self.store.get(&key).as_ref() != Some(&value) {
                self.store.set(key, value);
            }
        }
    }
}

#[async_trait::async_trait]
impl<R: Runtime> Transactional for TauriStoreDriver<R> {
    async fn run(&self, work: TxWork) -> Result<()> {
        if self.tx.is_some() {
            return Err(crate::Error::Unimplemented {
                backend: "tauri-store",
                capability: "nest a transaction",
            });
        }
        let _held = self.inner.lock.clone().lock_owned().await;
        let before = self.inner.snapshot();
        let open = Arc::new(AtomicBool::new(true));
        let bound = Arc::new(TauriStoreDriver {
            inner: self.inner.clone(),
            tx: Some(open.clone()),
        });
        let caps = Capabilities {
            schema: Some(bound.clone()),
            batch: Some(bound.clone()),
            ..Capabilities::new(bound)
        };
        let db = DB::build("tauri-store", self.inner.name.clone(), caps, None);
        let out = work(&db).await;
        open.store(false, Ordering::Release);
        match out {
            Ok(()) if self.inner.durable => self.inner.save(),
            Ok(()) => Ok(()),
            Err(e) => {
                self.inner.restore(before);
                Err(e)
            }
        }
    }
}
