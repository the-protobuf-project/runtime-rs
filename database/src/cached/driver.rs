//! The records driver that reads through a cache and writes through to the driver
//! underneath. Go's `cached.Driver`, in `database/cached/cached.go`.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use futures::FutureExt;
use prost::Message;
use prost_reflect::DynamicMessage;

use super::{Aside, Cache, Load, Overloaded};
use crate::records::{self, ListOptions, ListResult, Resource, WriteResult, key_of};
use crate::{Error, Result};

/// A records driver that reads through a cache and writes through to the driver underneath.
/// Go's `cached.Driver`.
///
/// `get` is cached, per record, keyed by primary key. Everything else is not, each for a
/// reason: `list` and `count` depend on the whole set, so any write to any record can change
/// them, and a stale listing is the kind of wrong that looks right; `exists` asks a question
/// the store answers cheaply, and serving it from the cache would transfer a whole record and
/// fill the cache with records nobody asked to read.
///
/// Writes go to the backing store first and invalidate the cache after; see
/// its [`create`](crate::records::Driver::create).
pub struct Driver {
    next: Arc<dyn records::Driver>,
    cache: Arc<dyn Cache>,
    asides: RwLock<HashMap<String, Arc<dyn Aside>>>,
}

impl Driver {
    /// `next` read through `cache`. Prefer [`super::wrap`] where there is a
    /// [`DB`](records::DB): a bare driver carries none of its capabilities. Go's `cached.New`.
    pub fn new(next: Arc<dyn records::Driver>, cache: Arc<dyn Cache>) -> Self {
        Self {
            next,
            cache,
            asides: RwLock::new(HashMap::new()),
        }
    }

    /// The driver underneath, for reads that must not be cached. Go's `Driver.Unwrap`.
    pub fn unwrap(&self) -> &Arc<dyn records::Driver> {
        &self.next
    }

    /// The resource's read-through view, built once: a view is bound to a loader, and the
    /// loader has to know which resource it loads.
    fn aside(&self, res: &Resource) -> Arc<dyn Aside> {
        if let Some(a) = self.asides.read().expect("asides lock").get(&res.name) {
            return a.clone();
        }
        let mut asides = self.asides.write().expect("asides lock");
        if let Some(a) = asides.get(&res.name) {
            return a.clone();
        }
        let (next, owned) = (self.next.clone(), Arc::new(res.clone()));
        let load: Load = Arc::new(move |key: String| {
            let (next, res) = (next.clone(), owned.clone());
            // NotFound travels as itself, to be remembered.
            async move { Ok(next.get(&res, &key).await?.encode_to_vec()) }.boxed()
        });
        let a = self.cache.aside(load);
        asides.insert(res.name.clone(), a.clone());
        a
    }

    /// Drops a cached record. A failure fails nothing: the write it followed has happened, and
    /// reporting a cache error as a write error would say the write failed when it did not.
    /// The entry is wrong until it expires, which is why the cache wants a TTL.
    pub(super) async fn forget(&self, res: &Resource, key: &str) {
        if key.is_empty() {
            return;
        }
        let _ = self.aside(res).invalidate(&[key.to_owned()]).await;
    }
}

#[async_trait::async_trait]
impl records::Driver for Driver {
    /// Stores a record and drops whatever the cache believed about its key. Not housekeeping:
    /// a `get` for a key that was not there remembers the absence, so a record created
    /// afterwards would stay invisible until that memory expired, the one bug a read-through
    /// cache produces that looks exactly like a database that lost a write.
    async fn create(&self, res: &Resource, msg: &DynamicMessage) -> Result<WriteResult> {
        let out = self.next.create(res, msg).await?;
        if let Ok(key) = key_of(res, &out.message).or_else(|_| key_of(res, msg)) {
            self.forget(res, &key).await;
        }
        Ok(out)
    }

    /// The record under `key`, from the cache where it is there. The cached bytes are proto
    /// wire format, decoded against the resource's descriptor, so the cache only ever sees
    /// opaque bytes. A cache refusing new loads is read through directly: the load happens
    /// either way, and this way it runs on the caller's task, which something upstream is
    /// already bounding.
    async fn get(&self, res: &Resource, key: &str) -> Result<DynamicMessage> {
        let body = match self.aside(res).get_or_load(key).await {
            Ok(body) => body,
            Err(err) if Overloaded::is(&err) => return self.next.get(res, key).await,
            Err(err) => return Err(err),
        };
        DynamicMessage::decode(res.message.clone(), body.as_slice()).map_err(|e| Error::Backend {
            backend: "cache",
            source: format!("cached {} does not decode: {e}", res.name).into(),
        })
    }

    /// Read from the provider, never the cache: what a write is about to check must be what
    /// is stored.
    async fn get_for_update(&self, res: &Resource, key: &str) -> Result<DynamicMessage> {
        self.next.get_for_update(res, key).await
    }

    /// Overwrites a record and drops the cached copy.
    async fn update(&self, res: &Resource, msg: &DynamicMessage) -> Result<WriteResult> {
        let out = self.next.update(res, msg).await?;
        if let Ok(key) = key_of(res, msg) {
            self.forget(res, &key).await;
        }
        Ok(out)
    }

    /// Removes a record and drops the cached copy.
    async fn delete(&self, res: &Resource, key: &str) -> Result<()> {
        self.next.delete(res, key).await?;
        self.forget(res, key).await;
        Ok(())
    }

    /// Passes through; see [`Driver`].
    async fn list(&self, res: &Resource, opts: &ListOptions) -> Result<ListResult> {
        self.next.list(res, opts).await
    }

    /// Passes through; see [`Driver`].
    async fn count(&self, res: &Resource, opts: &ListOptions) -> Result<i64> {
        self.next.count(res, opts).await
    }

    /// Passes through; see [`Driver`].
    async fn exists(&self, res: &Resource, key: &str) -> Result<bool> {
        self.next.exists(res, key).await
    }
}
