//! The records driver over one Tauri store: CRUD, schema and bulk, each run under the
//! store's lock and, when durable, saved to disk before it returns.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use prost_reflect::DynamicMessage;
use tauri::Runtime;
use tauri_plugin_store::Store;

use super::keys::Keys;
use crate::records::{Batcher, Driver, ListOptions, ListResult, Migrator, Resource, WriteResult};
use crate::{Error, Result};

/// What every driver over one store shares.
pub(super) struct Inner<R: Runtime> {
    pub(super) store: Arc<Store<R>>,
    pub(super) keys: Keys,
    /// The selected database's name.
    pub(super) name: String,
    /// Serializes this store's writers: one lock per store, whichever database they select.
    pub(super) lock: Arc<tokio::sync::Mutex<()>>,
    /// Saves after every write, so a successful write is on disk.
    pub(super) durable: bool,
}

/// The records driver over a Tauri store, for one selected database. Build it with
/// [`super::TauriStoreProvider`].
pub struct TauriStoreDriver<R: Runtime> {
    pub(super) inner: Arc<Inner<R>>,
    /// Inside a transaction: whether it is still open. The transaction holds the lock, so a
    /// driver bound to it runs without taking it.
    pub(super) tx: Option<Arc<AtomicBool>>,
}

impl<R: Runtime> Clone for TauriStoreDriver<R> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            tx: self.tx.clone(),
        }
    }
}

impl<R: Runtime> TauriStoreDriver<R> {
    /// Runs `op` exclusively, then saves unless a transaction will.
    pub(super) async fn write<T>(&self, op: impl FnOnce(&Inner<R>) -> Result<T>) -> Result<T> {
        if let Some(open) = &self.tx {
            if !open.load(Ordering::Acquire) {
                return Err(Error::Invalid(
                    "tauri-store: the transaction has ended".into(),
                ));
            }
            return op(&self.inner);
        }
        let _held = self.inner.lock.lock().await;
        let out = op(&self.inner)?;
        if self.inner.durable {
            self.inner.save()?;
        }
        Ok(out)
    }

    /// Runs `op` exclusively, so it never sees a write half done.
    async fn read<T>(&self, op: impl FnOnce(&Inner<R>) -> Result<T>) -> Result<T> {
        match &self.tx {
            Some(_) => self.write(op).await,
            None => {
                let _held = self.inner.lock.lock().await;
                op(&self.inner)
            }
        }
    }
}

impl<R: Runtime> Inner<R> {
    /// Writes the store's file.
    pub(super) fn save(&self) -> Result<()> {
        self.store.save().map_err(|e| Error::Backend {
            backend: "tauri-store",
            source: Box::new(e),
        })
    }
}

#[async_trait::async_trait]
impl<R: Runtime> Driver for TauriStoreDriver<R> {
    async fn create(&self, res: &Resource, msg: &DynamicMessage) -> Result<WriteResult> {
        self.write(|s| s.create(res, msg)).await
    }

    async fn get(&self, res: &Resource, key: &str) -> Result<DynamicMessage> {
        let row = self
            .read(|s| s.load(res, key))
            .await?
            .ok_or(Error::NotFound)?;
        crate::records::columns_to_message(res, &row)
    }

    async fn update(&self, res: &Resource, msg: &DynamicMessage) -> Result<WriteResult> {
        self.write(|s| s.update(res, msg)).await
    }

    async fn delete(&self, res: &Resource, key: &str) -> Result<()> {
        self.write(|s| s.delete(res, key)).await
    }

    async fn list(&self, res: &Resource, opts: &ListOptions) -> Result<ListResult> {
        self.read(|s| s.list(res, opts)).await
    }

    async fn count(&self, res: &Resource, opts: &ListOptions) -> Result<i64> {
        self.read(|s| Ok(s.matching(res, &opts.filter)?.len() as i64))
            .await
    }

    async fn exists(&self, res: &Resource, key: &str) -> Result<bool> {
        self.read(|s| Ok(s.store.has(s.keys.record(res, key))))
            .await
    }
}

/// A store has no schema: a resource's keys appear with its first record. Dropping removes
/// every key of the resource, its reservations included.
#[async_trait::async_trait]
impl<R: Runtime> Migrator for TauriStoreDriver<R> {
    async fn ensure_schema(&self, _: &Resource) -> Result<()> {
        Ok(())
    }

    async fn drop_schema(&self, res: &Resource) -> Result<()> {
        self.write(|s| {
            let prefix = s.keys.table(res);
            for key in s
                .store
                .keys()
                .into_iter()
                .filter(|k| k.starts_with(&prefix))
            {
                s.store.delete(key);
            }
            Ok(())
        })
        .await
    }

    async fn has_schema(&self, _: &Resource) -> Result<bool> {
        Ok(true)
    }
}

/// Bulk writes in one save, all or nothing: a refused record undoes the ones before it.
#[async_trait::async_trait]
impl<R: Runtime> Batcher for TauriStoreDriver<R> {
    async fn create_many(
        &self,
        res: &Resource,
        msgs: &[DynamicMessage],
    ) -> Result<Vec<WriteResult>> {
        self.write(|s| {
            let before = s.snapshot();
            let out = msgs
                .iter()
                .map(|m| s.create(res, m))
                .collect::<Result<Vec<_>>>();
            if out.is_err() {
                s.restore(before);
            }
            out
        })
        .await
    }

    async fn get_many(
        &self,
        res: &Resource,
        keys: &[String],
    ) -> Result<Vec<Option<DynamicMessage>>> {
        self.read(|s| {
            keys.iter()
                .map(|k| {
                    s.load(res, k)?
                        .map(|row| crate::records::columns_to_message(res, &row))
                        .transpose()
                })
                .collect()
        })
        .await
    }
}
