//! The repository suite on SQLite, in process: every case, filters and racing writers
//! included (SQLite serializes writers by locking the database), and the Go parity cases.

mod support {
    #[path = "articles.rs"]
    pub mod articles;
    #[path = "library.rs"]
    pub mod library;
    #[path = "repo_crud.rs"]
    pub mod repo_crud;
    #[path = "repo_delete.rs"]
    pub mod repo_delete;
    #[path = "repo_guard.rs"]
    pub mod repo_guard;
    #[path = "repo_more.rs"]
    pub mod repo_more;
    #[path = "repo_parent.rs"]
    pub mod repo_parent;
    #[path = "repo_parity.rs"]
    pub mod repo_parity;
    #[path = "repo_suite.rs"]
    pub mod repo_suite;
    #[path = "repo_update.rs"]
    pub mod repo_update;
}

use std::sync::Arc;

use database::orm::OrmProvider;
use database::orm::sqlx::{self, AssertSqlSafe};
use database::records::{DB, Provider};
use futures::FutureExt;
use support::repo_parity::{Parity, Raw};
use support::repo_suite::Scenario;

#[path = "support/orm.rs"]
mod orm;

async fn run(case: Scenario) {
    let db: DB = OrmProvider::new(orm::sqlite().await)
        .set_database("")
        .await
        .unwrap();
    case(db).await;
}

/// A parity case, reading the table raw through the same pool.
async fn parity(case: Parity) {
    let pool = orm::sqlite().await;
    let db = OrmProvider::new(pool.clone())
        .set_database("")
        .await
        .unwrap();
    let raw: Raw = Arc::new(move |table: &str, col: &str, name: &str| {
        let sql = format!("SELECT CAST({col} AS TEXT) FROM {table} WHERE name = ?");
        let (pool, name) = (pool.clone(), name.to_owned());
        async move {
            let q = sqlx::query_scalar::<_, Option<String>>(AssertSqlSafe(sql)).bind(name);
            q.fetch_one(&pool).await.unwrap()
        }
        .boxed()
    });
    case(db, raw, String::new()).await;
}

crate::repository_suite!(run);
crate::repository_parity!(parity);
crate::repository_filters!(run);
crate::repository_races!(run);
