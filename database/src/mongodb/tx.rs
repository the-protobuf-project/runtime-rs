//! [`Transactional`] over MongoDB sessions. Go's `Driver.Run` and `sessionDriver` in
//! `mongodb/schema.go`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use mongodb::ClientSession;
use mongodb::error::UNKNOWN_TRANSACTION_COMMIT_RESULT;
use tokio::sync::Mutex;

use super::error::{backend, invalid};
use super::{BACKEND, MongoDriver};
use crate::Result;
use crate::records::{Capabilities, DB, Transactional, TxWork};

/// How long a commit whose outcome the server could not report is retried, as Go's
/// `WithTransaction` does.
const COMMIT_DEADLINE: Duration = Duration::from_secs(120);

#[async_trait::async_trait]
impl Transactional for MongoDriver {
    /// Runs `work` in one session's transaction: committed when it returns `Ok`, aborted when
    /// it errs, and aborted by the driver when it panics and drops the session.
    ///
    /// The [`DB`] given to `work` has its CRUD and bulk calls bound to the session, so every
    /// one joins the transaction whatever the caller does; schema and watch calls run
    /// outside it, as in Go. A commit with an unknown outcome is retried; the work itself is
    /// not, being `FnOnce`, where Go's `WithTransaction` reruns it on a transient error.
    /// Nesting is refused: MongoDB has one transaction per session and none inside another.
    ///
    /// Needs a replica set: a standalone server has no multi-document transactions, and its
    /// error is reported rather than hidden.
    async fn run(&self, work: TxWork) -> Result<()> {
        if self.session.is_some() {
            return Err(invalid(
                "a transaction cannot be nested; MongoDB runs one per session",
            ));
        }
        let mut session = self
            .client
            .start_session()
            .await
            .map_err(|e| backend("cannot start a session", e))?;
        session
            .start_transaction()
            .await
            .map_err(|e| backend("cannot start a transaction", e))?;
        let session = Arc::new(Mutex::new(session));
        let bound = Arc::new(MongoDriver {
            session: Some(session.clone()),
            ..self.clone()
        });
        let outside = Arc::new(self.clone());
        let tx = DB::build(
            BACKEND,
            self.db.clone(),
            Capabilities {
                driver: bound.clone(),
                tx: Some(bound.clone()),
                schema: Some(outside.clone()),
                batch: Some(bound),
                watch: Some(outside),
            },
            None,
        );
        let result = work(&tx).await;
        drop(tx);
        let mut session = session.lock().await;
        match result {
            Ok(()) => commit(&mut session).await,
            Err(err) => {
                // The caller's error is the one worth reporting; a failed abort leaves the
                // transaction to the server's timeout, which discards it all the same.
                let _ = session.abort_transaction().await;
                Err(err)
            }
        }
    }
}

/// Commits, retrying while the server cannot say whether the last attempt took.
async fn commit(session: &mut ClientSession) -> Result<()> {
    let started = Instant::now();
    loop {
        match session.commit_transaction().await {
            Ok(()) => return Ok(()),
            Err(e)
                if e.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT)
                    && started.elapsed() < COMMIT_DEADLINE => {}
            Err(e) => return Err(backend("cannot commit the transaction", e)),
        }
    }
}
