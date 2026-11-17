//! `create`, `update` and `update_output_only`: Go's `Create`, `Update` and
//! `UpdateOutputOnly`. Each write mints a new etag; an update merges its mask onto the stored
//! record and checks the caller's etag in the same transaction as the write.

use prost::Message;
use prost_reflect::ReflectMessage;

use super::RecordsRepository;
use super::atomic::atomic;
use super::mask;
use crate::records::key_of;
use crate::{Error, Result};

impl<M> RecordsRepository<M>
where
    M: ReflectMessage + Message + Default + Send + Sync + 'static,
{
    /// Runs `before_create`, mints the etag, stores the record, and returns it read back,
    /// audit times and generated key included, through `after_read`. A taken name or unique
    /// value is `AlreadyExists`. The name is the caller's: Go's adapter mints one from the
    /// resource pattern, which a generated service does here before calling.
    pub(super) async fn create_record(&self, m: &M) -> Result<M> {
        let inner = &self.inner;
        let mut m = inner.decode(&m.transcode_to_dynamic())?;
        if let Some(h) = &inner.hooks.before_create {
            m = h(m).await?;
        }
        let mut msg = m.transcode_to_dynamic();
        inner.shape.mint(&mut msg);
        let stored = self.db.create(&inner.res, &msg).await?.message;
        let key = key_of(&inner.res, &stored)?;
        inner.read(&self.db.get(&inner.res, &key).await?).await
    }

    /// The masked write onto the stored record, in one transaction: read it (locked where the
    /// provider locks rows), check `m`'s etag, merge, run `before_update` (not for output-only
    /// writes, as in Go), mint the etag, write. The result is read back after the commit.
    pub(super) async fn update_record(&self, m: &M, paths: &[String], output: bool) -> Result<M> {
        let inner = self.inner.clone();
        let input = m.transcode_to_dynamic();
        let key = key_of(&inner.res, &input)?;
        if key.is_empty() {
            return Err(Error::Invalid("update needs the record's name".into()));
        }
        let allowed = match output {
            true => &inner.shape.output,
            false => &inner.shape.mutable,
        };
        let fields = mask::plan(&inner.res.message, paths, allowed)?;
        let (paths, k) = (paths.to_vec(), key.clone());
        atomic(&self.db, move |tx| {
            let (inner, input, fields, paths, key) = (
                inner.clone(),
                input.clone(),
                fields.clone(),
                paths.clone(),
                k.clone(),
            );
            async move {
                let stored = tx.get_for_update(&inner.res, &key).await?;
                inner.shape.check(&stored, &inner.shape.etag_of(&input))?;
                let mut merged = stored.clone();
                mask::copy(&mut merged, &input, &fields);
                if let (Some(h), false) = (&inner.hooks.before_update, output) {
                    let next = h(inner.decode(&stored)?, inner.decode(&merged)?, paths).await?;
                    merged = next.transcode_to_dynamic();
                }
                inner.shape.mint(&mut merged);
                tx.update(&inner.res, &merged).await.map(drop)
            }
        })
        .await?;
        let inner = &self.inner;
        inner.read(&self.db.get(&inner.res, &key).await?).await
    }
}
