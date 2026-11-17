//! A read-check-write run as one unit: in the provider's transaction where it has one, else
//! directly. Go runs the same steps in a GORM transaction.
//!
//! | Provider | What makes the etag check and the write one step |
//! | --- | --- |
//! | SQL | A transaction whose read is `SELECT ... FOR UPDATE` (Postgres, MySQL), so a second writer waits and then sees the first one's etag; SQLite locks the database |
//! | MongoDB | A session transaction: a concurrent write to the document fails the later one with a write conflict, reported as `Conflict` |
//! | Tauri store | The transaction holds the store's lock |
//! | Redis | No transactions: the check and the write are two steps, so a writer landing between them is overwritten. Each step is atomic on its own |

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;

use crate::records::{DB, TxWork};
use crate::{Error, Result};

/// Runs `work` in a transaction of `db` and returns what it produced; on a provider without
/// transactions (refusing with `Unimplemented` before running anything), runs it on `db`.
pub(super) async fn atomic<T, F, Fut>(db: &DB, work: F) -> Result<T>
where
    F: Fn(DB) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<T>> + Send + 'static,
    T: Send + 'static,
{
    let work = Arc::new(work);
    let out: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
    let started = Arc::new(AtomicBool::new(false));
    let (w, o, s) = (work.clone(), out.clone(), started.clone());
    let ran = db
        .tx
        .run(boxed(move |tx: &DB| {
            let tx = tx.clone();
            Box::pin(async move {
                s.store(true, Ordering::SeqCst);
                let value = w(tx).await?;
                *o.lock().unwrap_or_else(|e| e.into_inner()) = Some(value);
                Ok(())
            })
        }))
        .await;
    match ran {
        Ok(()) => out
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .ok_or_else(|| Error::Invalid("a committed transaction returned nothing".into())),
        Err(Error::Unimplemented { .. }) if !started.load(Ordering::SeqCst) => {
            work(db.clone()).await
        }
        Err(e) => Err(e),
    }
}

fn boxed<F>(f: F) -> TxWork
where
    F: for<'a> FnOnce(&'a DB) -> BoxFuture<'a, Result<()>> + Send + 'static,
{
    Box::new(f)
}
