//! A view of a driver bound to one resource and one generated message type, and the registry
//! descriptors are looked up in. Go's `store/typed.go` and `store/registry.go`.

use std::collections::{BTreeSet, HashMap};
use std::marker::PhantomData;
use std::sync::Arc;

use prost::Message;
use prost_reflect::ReflectMessage;

use super::{DB, ListOptions, Resource, WriteResult};
use crate::{Error, Result};

/// Resource descriptors by name; read-only once built. A later duplicate name replaces an
/// earlier one. Go's `store.Registry`.
#[derive(Clone, Debug, Default)]
pub struct Registry {
    by_name: HashMap<String, Arc<Resource>>,
}

impl Registry {
    /// Indexes `resources` by name.
    pub fn new(resources: impl IntoIterator<Item = Resource>) -> Self {
        Self {
            by_name: resources
                .into_iter()
                .map(|r| (r.name.clone(), Arc::new(r)))
                .collect(),
        }
    }

    /// The descriptor registered as `name`.
    pub fn resource(&self, name: &str) -> Result<Arc<Resource>> {
        self.by_name
            .get(name)
            .cloned()
            .ok_or_else(|| Error::Invalid(format!("no resource registered as {name:?}")))
    }

    /// The registered names, sorted.
    pub fn names(&self) -> Vec<String> {
        self.by_name
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
}

/// A [`DB`] bound to one resource, reading and writing `T`, so neither the descriptor nor a
/// conversion is repeated on every call. Go's `store.Typed`.
pub struct Typed<T> {
    db: DB,
    res: Arc<Resource>,
    _t: PhantomData<fn() -> T>,
}

impl<T> Clone for Typed<T> {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            res: self.res.clone(),
            _t: PhantomData,
        }
    }
}

impl<T: ReflectMessage + Message + Default> Typed<T> {
    /// `db` bound to `res`. The pairing is checked here, not on every call: `res` must
    /// describe `T`. Go's `store.For`.
    pub fn new(db: &DB, res: Arc<Resource>) -> Result<Self> {
        let want = T::default().descriptor();
        if want.full_name() != res.message.full_name() {
            return Err(Error::Invalid(format!(
                "resource {:?} describes {}, not {}: the descriptor and the type do not match",
                res.name,
                res.message.full_name(),
                want.full_name()
            )));
        }
        Ok(Self {
            db: db.clone(),
            res,
            _t: PhantomData,
        })
    }

    /// The database and descriptor, for what the view does not cover.
    pub fn unwrap(&self) -> (&DB, &Resource) {
        (&self.db, &self.res)
    }

    /// Stores `msg` as a new record.
    pub async fn create(&self, msg: &T) -> Result<WriteResult> {
        self.db.create(&self.res, &msg.transcode_to_dynamic()).await
    }

    /// The record under `key`.
    pub async fn get(&self, key: &str) -> Result<T> {
        decode(self.db.get(&self.res, key).await?)
    }

    /// Overwrites the record identified by `msg`'s primary key.
    pub async fn update(&self, msg: &T) -> Result<WriteResult> {
        self.db.update(&self.res, &msg.transcode_to_dynamic()).await
    }

    /// Removes the record under `key`.
    pub async fn delete(&self, key: &str) -> Result<()> {
        self.db.delete(&self.res, key).await
    }

    /// Whether a record with `key` exists.
    pub async fn exists(&self, key: &str) -> Result<bool> {
        self.db.exists(&self.res, key).await
    }

    /// How many records match `opts.filter`.
    pub async fn count(&self, opts: &ListOptions) -> Result<i64> {
        self.db.count(&self.res, opts).await
    }

    /// A page of records, the next page's token, and the total.
    pub async fn list(&self, opts: &ListOptions) -> Result<(Vec<T>, String, i64)> {
        let page = self.db.list(&self.res, opts).await?;
        let items = page.items.into_iter().map(decode).collect::<Result<_>>()?;
        Ok((items, page.next_page_token, page.total))
    }

    /// Every record matching `opts`, following page tokens; a repeated token is an error
    /// rather than a loop. Go's `Typed.All`.
    pub async fn all(&self, opts: &ListOptions) -> Result<Vec<T>> {
        let (mut opts, mut out, mut seen) = (opts.clone(), Vec::new(), BTreeSet::new());
        loop {
            let (page, next, _) = self.list(&opts).await?;
            out.extend(page);
            if next.is_empty() {
                return Ok(out);
            }
            if !seen.insert(next.clone()) {
                return Err(Error::Invalid(format!(
                    "{} repeated page token {next:?}; refusing to loop",
                    self.res.name
                )));
            }
            opts.page_token = next;
        }
    }
}

fn decode<T: Message + Default>(msg: prost_reflect::DynamicMessage) -> Result<T> {
    msg.transcode_to::<T>()
        .map_err(|e| Error::Invalid(format!("decoding a record: {e}")))
}
