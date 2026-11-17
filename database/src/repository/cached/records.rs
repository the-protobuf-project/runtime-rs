//! The record cache: read-through views and the work after a write.

use std::sync::Arc;

use cache::CacheError;
use cache::core::{Aside, Loader};
use futures::FutureExt;
use prost::Message;

use super::wire::{once, record_options, refile, wire};
use super::{CachedRepository, Keyspace, List};
use crate::cached::aside::cache_error;
use crate::{Error, Result};

impl<M> CachedRepository<M>
where
    M: Message + Default + Send + Sync + 'static,
{
    /// The record database `name` is cached in.
    pub(super) fn record_db(&self, keyspace: &Keyspace, name: &str) -> Result<Arc<cache::DB>> {
        if keyspace.fixed().is_some() {
            return once(&self.fixed, || {
                self.cache.under(&keyspace.fixed().unwrap_or_default())
            });
        }
        let namespace = keyspace.namespace(name)?;
        self.cache
            .under(&namespace)
            .map(Arc::new)
            .map_err(cache_error)
    }

    pub(super) fn list_db(&self, list: &List) -> Result<Arc<cache::DB>> {
        once(&self.lists, || self.cache.under(list.namespace))
    }

    /// The read-through view of `db`, loading from the inner repository.
    pub(super) fn aside(&self, db: &cache::DB) -> Arc<dyn Aside> {
        let inner = self.inner.clone();
        let loader: Loader = Arc::new(move |name: String| {
            let inner = inner.clone();
            async move {
                match inner.get(&name).await {
                    Ok(m) => wire(&m.encode_to_vec()),
                    Err(Error::NotFound) => Err(CacheError::NotFound),
                    Err(other) => Err(CacheError::Loader(Arc::new(other))),
                }
            }
            .boxed()
        });
        db.aside(loader)
    }

    /// Runs one step after a committed write: reports a failure, and returns it only under
    /// strict writes.
    pub(super) fn step(&self, resource: &str, op: &str, res: Result<()>) -> Result<()> {
        let Err(err) = res else { return Ok(()) };
        if let Some(observe) = &self.hooks.write_errors {
            observe(resource, op, &err);
        }
        if self.hooks.strict_writes {
            return Err(Error::Backend {
                backend: "cache",
                source: format!(
                    "{resource}: {op} failed after the store write had already committed: {err}"
                )
                .into(),
            });
        }
        Ok(())
    }

    /// After a create, update or undelete: refresh, refile, fire the edges, rotate lists.
    pub(super) async fn after(&self, m: &M) -> Result<()> {
        let name = (self.plan.name)(m);
        if let Some(record) = &self.plan.record {
            let resource = record.keyspace.resource;
            match self.record_db(record.keyspace, name) {
                Err(err) => self.step(resource, "refresh", Err(err))?,
                Ok(db) => {
                    let refreshed = self
                        .aside(&db)
                        .refresh(name, &record_options(record))
                        .await
                        .map_err(cache_error);
                    self.step(resource, "refresh", refreshed)?;
                    if !record.indexes.is_empty() {
                        let refiled = refile(&db, record, name, m).await;
                        self.step(resource, "refile", refiled)?;
                    }
                }
            }
            for edge in &record.edges {
                let Some(value) = (edge.via)(m).filter(|v| !v.is_empty()) else {
                    continue;
                };
                let op = format!("invalidate {}", edge.target.model);
                let dropped = self.invalidate(edge.target, value).await;
                self.step(resource, &op, dropped)?;
            }
        }
        self.rotate(name).await;
        Ok(())
    }

    /// After a delete or purge: unfile, drop the entry, rotate lists.
    pub(super) async fn gone(&self, name: &str) -> Result<()> {
        if let Some(record) = &self.plan.record {
            let resource = record.keyspace.resource;
            match self.record_db(record.keyspace, name) {
                Err(err) => self.step(resource, "invalidate", Err(err))?,
                Ok(db) => {
                    if !record.indexes.is_empty() {
                        let unfiled = match db.indexed.delete(name).await {
                            Ok(()) | Err(CacheError::NotFound) => Ok(()),
                            Err(err) => Err(cache_error(err)),
                        };
                        self.step(resource, "unfile", unfiled)?;
                    }
                    let dropped = self.aside(&db).invalidate(name).await.map_err(cache_error);
                    self.step(resource, "invalidate", dropped)?;
                }
            }
        }
        self.rotate(name).await;
        Ok(())
    }

    /// Drops `target`'s entry for the record `value` references.
    pub(super) async fn invalidate(&self, target: &Keyspace, value: &str) -> Result<()> {
        let name = target.resolve(value).ok_or_else(|| {
            Error::Invalid(format!(
                "cache: {}: {value:?} names no record of the pattern {:?}",
                target.resource, target.pattern
            ))
        })?;
        let db = self.record_db_of(target, &name)?;
        self.aside(&db).invalidate(&name).await.map_err(cache_error)
    }

    /// The database of another resource's entry: never this resource's fixed one.
    pub(super) fn record_db_of(&self, keyspace: &Keyspace, name: &str) -> Result<Arc<cache::DB>> {
        let namespace = keyspace.namespace(name)?;
        self.cache
            .under(&namespace)
            .map(Arc::new)
            .map_err(cache_error)
    }
}
