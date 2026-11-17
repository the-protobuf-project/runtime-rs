//! [`MongoDriver`]: the client, the selected database and, inside a transaction, the session
//! every operation joins. Go's `mongodb.Driver` and `sessionDriver`.

use std::sync::Arc;

use bson::Document;
use futures::TryStreamExt;
use mongodb::options::FindOptions;
use mongodb::{ClientSession, Collection};
use tokio::sync::Mutex;

use super::error::{backend, invalid};
use crate::Result;
use crate::records::Resource;

/// Runs a driver action, inside the transaction when the driver is bound to one.
macro_rules! exec {
    ($driver:expr, $action:expr) => {
        match &$driver.session {
            None => $action.await,
            Some(session) => {
                let mut session = session.lock().await;
                $action.session(&mut *session).await
            }
        }
    };
}
pub(super) use exec;

/// A [`Driver`](crate::records::Driver) over MongoDB, and every capability beyond it. Cheap
/// to clone. Go's `mongodb.Driver`.
#[derive(Clone)]
pub struct MongoDriver {
    pub(super) client: mongodb::Client,
    pub(super) db: String,
    /// The transaction every operation joins; `None` outside one.
    pub(super) session: Option<Arc<Mutex<ClientSession>>>,
}

impl MongoDriver {
    /// A driver over a client you own, writing each resource into the database its
    /// [`Resource::schema`] names. [`MongoProvider`](super::MongoProvider) selects one at
    /// runtime instead. Go's `mongodb.New`.
    pub fn new(client: &super::MongoClient) -> Self {
        Self::over(client.unwrap().clone(), String::new())
    }

    pub(super) fn over(client: mongodb::Client, db: String) -> Self {
        Self {
            client,
            db,
            session: None,
        }
    }

    /// The collection `res` lives in. A selected database wins over the descriptor's: the
    /// descriptor says what a Book is, the selection says whose.
    pub(super) fn collection(&self, res: &Resource) -> Result<Collection<Document>> {
        let db = if self.db.is_empty() {
            &res.schema
        } else {
            &self.db
        };
        if db.is_empty() {
            return Err(invalid(format!(
                "resource {:?} names no database and none was selected",
                res.name
            )));
        }
        Ok(self.client.database(db).collection(&res.table))
    }

    /// Every document `filter` matches, read inside the transaction when bound to one.
    pub(super) async fn find(
        &self,
        coll: &Collection<Document>,
        filter: Document,
        opts: Option<FindOptions>,
    ) -> mongodb::error::Result<Vec<Document>> {
        match &self.session {
            None => {
                coll.find(filter)
                    .with_options(opts)
                    .await?
                    .try_collect()
                    .await
            }
            Some(session) => {
                let mut session = session.lock().await;
                let mut cursor = coll
                    .find(filter)
                    .with_options(opts)
                    .session(&mut *session)
                    .await?;
                cursor.stream(&mut session).try_collect().await
            }
        }
    }
}

impl std::fmt::Debug for MongoDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongoDriver")
            .field("db", &self.db)
            .field("in_transaction", &self.session.is_some())
            .finish_non_exhaustive()
    }
}

impl MongoDriver {
    /// How many documents `filter` matches.
    pub(super) async fn count_filter(
        &self,
        res: &Resource,
        coll: &Collection<Document>,
        filter: Document,
    ) -> Result<i64> {
        let n = exec!(self, coll.count_documents(filter))
            .map_err(|e| backend(format!("cannot count {}", res.table), e))?;
        Ok(n as i64)
    }
}
