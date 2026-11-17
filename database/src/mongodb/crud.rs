//! [`Driver`] over MongoDB: CRUD on one collection per resource, keyed by `_id`. Go's
//! `Create`..`List` in `mongodb/driver.go`.

use bson::doc;
use mongodb::options::FindOptions;
use prost_reflect::DynamicMessage;

use super::MongoDriver;
use super::codec::{from_document, key_bson, to_document};
use super::driver::exec;
use super::error::{backend, write};
use super::filter::{parse_filter, sort_of};
use crate::records::{
    Driver, ListOptions, ListResult, Resource, WriteResult, columns_to_message, core, key_of,
    message_to_columns,
};
use crate::{Error, Result};

#[async_trait::async_trait]
impl Driver for MongoDriver {
    /// Inserts `msg`; a taken key or unique value, one write exception either way, is
    /// `AlreadyExists`. Returns what the driver filled in: a generated key, the audit times.
    async fn create(&self, res: &Resource, msg: &DynamicMessage) -> Result<WriteResult> {
        let coll = self.collection(res)?;
        let mut cols = message_to_columns(res, msg)?;
        core::fill_managed(res, &mut cols, true);
        let doc = to_document(res, &cols)?;
        exec!(self, coll.insert_one(doc))
            .map_err(|e| write(format!("cannot insert into {}", res.table), e))?;
        Ok(WriteResult::done(columns_to_message(res, &cols)?))
    }

    async fn get(&self, res: &Resource, key: &str) -> Result<DynamicMessage> {
        let coll = self.collection(res)?;
        let found = exec!(self, coll.find_one(doc! { "_id": key_bson(res, key) }))
            .map_err(|e| backend(format!("cannot read {key}"), e))?;
        from_document(res, found.ok_or(Error::NotFound)?)
    }

    /// Replaces the document whole rather than `$set`ting the columns present: the contract
    /// says update overwrites, and a field cleared in `msg` must not survive.
    async fn update(&self, res: &Resource, msg: &DynamicMessage) -> Result<WriteResult> {
        let coll = self.collection(res)?;
        let key = key_of(res, msg)?;
        let mut cols = message_to_columns(res, msg)?;
        core::fill_managed(res, &mut cols, false);
        let doc = to_document(res, &cols)?;
        let replaced = exec!(
            self,
            coll.replace_one(doc! { "_id": key_bson(res, &key) }, doc)
        )
        .map_err(|e| write(format!("cannot update {key}"), e))?;
        if replaced.matched_count == 0 {
            return Err(Error::NotFound);
        }
        Ok(WriteResult::done(columns_to_message(res, &cols)?))
    }

    async fn delete(&self, res: &Resource, key: &str) -> Result<()> {
        let coll = self.collection(res)?;
        let deleted = exec!(self, coll.delete_one(doc! { "_id": key_bson(res, key) }))
            .map_err(|e| backend(format!("cannot delete {key}"), e))?;
        if deleted.deleted_count == 0 {
            return Err(Error::NotFound);
        }
        Ok(())
    }

    /// Server-side filtering, ordering and paging with [`core`] offset tokens; a page past
    /// `omit_total` reads one extra document to learn whether another follows.
    async fn list(&self, res: &Resource, opts: &ListOptions) -> Result<ListResult> {
        let coll = self.collection(res)?;
        let filter = parse_filter(res, &opts.filter)?;
        let total = if opts.omit_total {
            core::NO_TOTAL
        } else {
            self.count_filter(res, &coll, filter.clone()).await?
        };
        let limit = core::page_size(opts.page_size);
        let offset = core::decode_token(&opts.page_token);
        let find = FindOptions::builder()
            .sort(sort_of(res, &opts.order_by))
            .limit(core::fetch_limit(limit, opts.omit_total))
            .skip((offset > 0).then_some(offset as u64))
            .build();
        let docs = self
            .find(&coll, filter, Some(find))
            .await
            .map_err(|e| backend(format!("cannot list {}", res.table), e))?;
        let items = docs
            .into_iter()
            .map(|d| from_document(res, d))
            .collect::<Result<Vec<_>>>()?;
        let (items, next_page_token) =
            core::trim_page(items, offset, limit, total, opts.omit_total);
        Ok(ListResult {
            items,
            next_page_token,
            total,
        })
    }

    async fn count(&self, res: &Resource, opts: &ListOptions) -> Result<i64> {
        let coll = self.collection(res)?;
        let filter = parse_filter(res, &opts.filter)?;
        self.count_filter(res, &coll, filter).await
    }

    /// Projects nothing but `_id`, so the answer costs an index lookup, not a document.
    async fn exists(&self, res: &Resource, key: &str) -> Result<bool> {
        let coll = self.collection(res)?;
        let found = exec!(
            self,
            coll.find_one(doc! { "_id": key_bson(res, key) })
                .projection(doc! { "_id": 1 })
        )
        .map_err(|e| backend(format!("cannot check {key}"), e))?;
        Ok(found.is_some())
    }
}
