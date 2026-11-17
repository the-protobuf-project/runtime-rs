//! Bulk writes through the cached database, and what any write does about the cache: drop
//! the key now, or record it for after a commit. A Rust extension: Go's cached driver has no
//! `Batcher`, so Go's `Wrap` hides the backend's.

use std::sync::{Arc, Mutex};

use prost_reflect::DynamicMessage;

use super::Driver;
use crate::Result;
use crate::records::{Batcher, DB, Resource, WriteResult, key_of};

/// Records a transaction wrote, to invalidate once it commits.
pub(super) type Touched = Arc<Mutex<Vec<(Resource, String)>>>;

/// What a write does about the cache: drop the key now, or remember it for after a commit.
#[derive(Clone)]
pub(super) enum Touch {
    Forget(Arc<Driver>),
    Record(Touched),
}

impl Touch {
    pub(super) async fn wrote(&self, res: &Resource, key: String) {
        match self {
            Touch::Forget(driver) => driver.forget(res, &key).await,
            Touch::Record(touched) => touched
                .lock()
                .expect("touched lock")
                .push((res.clone(), key)),
        }
    }
}

/// `db`'s bulk writer with each write's key passed to `touch`, or none where `db` refuses
/// bulk work, so the result refuses it too.
pub(super) fn bulk(db: &DB, touch: Touch) -> Option<Arc<dyn Batcher>> {
    let next = db.batch.clone();
    db.has_batch()
        .then(|| Arc::new(Bulk { next, touch }) as Arc<dyn Batcher>)
}

/// Bulk writes invalidate as single writes do; bulk reads pass through uncached, as `list`
/// does. A Rust extension: Go's cached driver has no `Batcher`.
struct Bulk {
    next: Arc<dyn Batcher>,
    touch: Touch,
}

#[async_trait::async_trait]
impl Batcher for Bulk {
    async fn create_many(
        &self,
        res: &Resource,
        msgs: &[DynamicMessage],
    ) -> Result<Vec<WriteResult>> {
        let out = self.next.create_many(res, msgs).await?;
        for (i, w) in out.iter().enumerate() {
            let key =
                key_of(res, &w.message).or_else(|e| msgs.get(i).map_or(Err(e), |m| key_of(res, m)));
            if let Ok(key) = key {
                self.touch.wrote(res, key).await;
            }
        }
        Ok(out)
    }

    async fn get_many(
        &self,
        res: &Resource,
        keys: &[String],
    ) -> Result<Vec<Option<DynamicMessage>>> {
        self.next.get_many(res, keys).await
    }
}
