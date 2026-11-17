//! [`Migrator`] over MongoDB: collections, and the unique indexes the descriptor declares.
//! Go's `EnsureSchema`, `DropSchema` and `HasSchema` in `mongodb/schema.go`.

use bson::doc;
use mongodb::IndexModel;
use mongodb::error::ErrorKind;
use mongodb::options::IndexOptions;

use super::MongoDriver;
use super::error::backend;
use crate::Result;
use crate::records::{Migrator, Resource};

/// The code MongoDB answers `create` with for a collection already there.
const NAMESPACE_EXISTS: i32 = 48;

#[async_trait::async_trait]
impl Migrator for MongoDriver {
    /// Creates the collection and a `uniq_<column>` unique index per [`unique`] column; safe to
    /// repeat. MongoDB would create the collection on first write anyway: what cannot wait is
    /// the index, since a column the descriptor calls unique is only unique if something
    /// enforces it. The key needs none: `_id` is unique by construction.
    ///
    /// [`unique`]: crate::records::Column::unique
    async fn ensure_schema(&self, res: &Resource) -> Result<()> {
        let coll = self.collection(res)?;
        let db = self.client.database(coll.namespace().db.as_str());
        if let Err(e) = db.create_collection(&res.table).await {
            let exists = matches!(e.kind.as_ref(),
                ErrorKind::Command(c) if c.code == NAMESPACE_EXISTS)
                || e.to_string().contains("already exists");
            if !exists {
                return Err(backend(format!("cannot create {}", res.table), e));
            }
        }
        // The key column is `_id`, unique by construction, whichever column is flagged primary.
        for c in res
            .columns
            .iter()
            .filter(|c| c.unique && c.name != res.pk_column)
        {
            let index = IndexModel::builder()
                .keys(doc! { c.name.clone(): 1 })
                .options(
                    IndexOptions::builder()
                        .unique(true)
                        .name(format!("uniq_{}", c.name))
                        .build(),
                )
                .build();
            coll.create_index(index)
                .await
                .map_err(|e| backend(format!("cannot create the unique index on {}", c.name), e))?;
        }
        Ok(())
    }

    /// Drops the collection and everything in it.
    async fn drop_schema(&self, res: &Resource) -> Result<()> {
        self.collection(res)?
            .drop()
            .await
            .map_err(|e| backend(format!("cannot drop {}", res.table), e))
    }

    /// Whether the collection is there.
    async fn has_schema(&self, res: &Resource) -> Result<bool> {
        let coll = self.collection(res)?;
        let names = self
            .client
            .database(coll.namespace().db.as_str())
            .list_collection_names()
            .filter(doc! { "name": &res.table })
            .await
            .map_err(|e| backend("cannot list collections", e))?;
        Ok(!names.is_empty())
    }
}
