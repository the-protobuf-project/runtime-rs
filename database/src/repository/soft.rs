//! `delete` and `undelete`: AIP-164 soft delete where the rules name a `delete_time`, else a
//! removal. Go's `Delete`, `markDeleted` and `Undelete`, with the `if_match` precondition
//! Go reads from `repox.IfMatch`.

use chrono::Utc;
use prost::Message;
use prost_reflect::ReflectMessage;

use super::RecordsRepository;
use super::atomic::atomic;
use super::shape::set_time;
use crate::records::core;
use crate::{Error, Result};

impl<M> RecordsRepository<M>
where
    M: ReflectMessage + Message + Default + Send + Sync + 'static,
{
    /// Under soft delete, marks the live record deleted in one write: `delete_time` and
    /// `update_time` both now, `expire_time` now plus the retention (unset at zero), a new
    /// etag. A record already deleted is `NotFound`, as Go's guarded `UPDATE` matches no
    /// row. Without soft delete, removes the record. `if_match`, when not empty and the
    /// resource has an etag, must be the stored etag, else `Conflict`.
    pub(super) async fn delete_record(&self, name: &str, if_match: &str) -> Result<()> {
        self.inner.before_delete(name).await?;
        let inner = self.inner.clone();
        let guarded = inner.shape.etag.is_some() && !if_match.is_empty();
        if inner.shape.delete_time.is_none() && !guarded {
            return self.db.delete(&inner.res, name).await;
        }
        let (name, want) = (name.to_owned(), if_match.to_owned());
        atomic(&self.db, move |tx| {
            let (inner, name, want) = (inner.clone(), name.clone(), want.clone());
            async move {
                let mut stored = tx.get_for_update(&inner.res, &name).await?;
                let shape = &inner.shape;
                let Some((delete_time, _)) = &shape.delete_time else {
                    shape.check(&stored, &want)?;
                    return tx.delete(&inner.res, &name).await;
                };
                if shape.deleted(&stored) {
                    return Err(Error::NotFound);
                }
                shape.check(&stored, &want)?;
                let now = Utc::now();
                set_time(&mut stored, delete_time, Some(now));
                if let Some(expire_time) = &shape.expire_time {
                    let expire = (!shape.retention.is_zero())
                        .then(|| chrono::Duration::from_std(shape.retention).ok())
                        .flatten()
                        .and_then(|d| now.checked_add_signed(d));
                    set_time(&mut stored, expire_time, expire);
                }
                shape.mint(&mut stored);
                core::at(now, tx.update(&inner.res, &stored))
                    .await
                    .map(drop)
            }
        })
        .await
    }

    /// Clears `delete_time` and `expire_time` and mints a new etag, then returns the record.
    /// `AlreadyExists` for a live record, `NotFound` for none, `Conflict` when `if_match`
    /// is not a deleted record's etag; refused without soft delete.
    pub(super) async fn undelete_record(&self, name: &str, if_match: &str) -> Result<M> {
        if self.inner.shape.delete_time.is_none() {
            return Err(Error::Unimplemented {
                backend: "repository",
                capability: "undelete a resource without a delete_time rule",
            });
        }
        let inner = self.inner.clone();
        let (key, want) = (name.to_owned(), if_match.to_owned());
        atomic(&self.db, move |tx| {
            let (inner, name, want) = (inner.clone(), key.clone(), want.clone());
            async move {
                let mut stored = tx.get_for_update(&inner.res, &name).await?;
                let shape = &inner.shape;
                if !shape.deleted(&stored) {
                    return Err(Error::AlreadyExists);
                }
                shape.check(&stored, &want)?;
                if let Some((delete_time, _)) = &shape.delete_time {
                    set_time(&mut stored, delete_time, None);
                }
                if let Some(expire_time) = &shape.expire_time {
                    set_time(&mut stored, expire_time, None);
                }
                shape.mint(&mut stored);
                tx.update(&inner.res, &stored).await.map(drop)
            }
        })
        .await?;
        let inner = &self.inner;
        inner.read(&self.db.get(&inner.res, name).await?).await
    }
}
