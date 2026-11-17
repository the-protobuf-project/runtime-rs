//! [`Repository`] for the decorator.

use cache::CacheError;
use cache::core::Options;
use prost::Message;

use super::CachedRepository;
use super::lists::{decode_page, encode_page};
use super::wire::{decoding, record_options, unwire};
use crate::cached::aside::{cache_error, restore};
use crate::repository::{ListInput, Page, Repository};
use crate::{Error, Result};

#[async_trait::async_trait]
impl<M> Repository<M> for CachedRepository<M>
where
    M: Message + Default + Send + Sync + 'static,
{
    async fn create(&self, m: &M) -> Result<M> {
        let out = self.inner.create(m).await?;
        self.after(&out).await?;
        Ok(out)
    }

    async fn get(&self, name: &str) -> Result<M> {
        let Some(record) = &self.plan.record else {
            return self.inner.get(name).await;
        };
        // A name not of the pattern names nothing this cache holds; the repository says
        // what it makes of it.
        let Ok(db) = self.record_db(record.keyspace, name) else {
            return self.inner.get(name).await;
        };
        let aside = self.aside(&db);
        let mut body = Vec::new();
        match aside
            .get_or_load(name, &mut body, &record_options(record))
            .await
        {
            Ok(()) => match unwire(&body).and_then(|b| M::decode(b.as_slice()).map_err(decoding)) {
                Ok(m) => Ok(m),
                Err(err) => {
                    // Not a value this decorator wrote: drop it and read through.
                    self.step(record.keyspace.resource, "decode", Err(err)).ok();
                    let _ = aside.invalidate(name).await;
                    self.inner.get(name).await
                }
            },
            Err(CacheError::NotFound) => Err(Error::NotFound),
            Err(CacheError::Overloaded) => self.inner.get(name).await,
            Err(CacheError::Loader(source)) => Err(restore(source)),
            Err(other) => Err(cache_error(other)),
        }
    }

    async fn list(&self, input: &ListInput) -> Result<Page<M>> {
        let Some(list) = &self.plan.list else {
            return self.inner.list(input).await;
        };
        let db = match self.list_db(list) {
            Ok(db) => db,
            Err(err) => {
                self.report_list(list, "version", &err);
                return self.inner.list(input).await;
            }
        };
        let key = self.page_key(list, &db, input).await;
        if let Some(key) = &key {
            let mut body = Vec::new();
            match db.volatile.get(key, &mut body).await {
                Ok(()) => match decode_page(&body) {
                    Ok(page) => return Ok(page),
                    Err(err) => self.report_list(list, "decode", &err),
                },
                Err(CacheError::NotFound) => {}
                Err(err) => self.report_list(list, "read", &cache_error(err)),
            }
        }
        let page = self.inner.list(input).await?;
        if let Some(key) = key {
            let opts = Options::default().with_ttl(list.ttl);
            if let Err(err) = db
                .volatile
                .set(&key, encode_page(&page).as_bytes(), &opts)
                .await
            {
                self.report_list(list, "store", &cache_error(err));
            }
        }
        Ok(page)
    }

    async fn count(&self, input: &ListInput) -> Result<i64> {
        self.inner.count(input).await
    }

    async fn update(&self, m: &M, paths: &[String]) -> Result<M> {
        let out = self.inner.update(m, paths).await?;
        self.after(&out).await?;
        Ok(out)
    }

    async fn update_output_only(&self, m: &M, paths: &[String]) -> Result<M> {
        let out = self.inner.update_output_only(m, paths).await?;
        self.after(&out).await?;
        Ok(out)
    }

    async fn delete(&self, name: &str, if_match: Option<&str>) -> Result<()> {
        self.inner.delete(name, if_match).await?;
        self.gone(name).await
    }

    async fn undelete(&self, name: &str, if_match: Option<&str>) -> Result<M> {
        let out = self.inner.undelete(name, if_match).await?;
        self.after(&out).await?;
        Ok(out)
    }

    async fn purge(&self, name: &str) -> Result<()> {
        self.inner.purge(name).await?;
        self.gone(name).await
    }
}
