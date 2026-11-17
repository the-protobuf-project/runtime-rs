//! The repository suite on SQLite, in process: every case, filters and racing writers
//! included (SQLite serializes writers by locking the database).

mod support {
    #[path = "articles.rs"]
    pub mod articles;
    #[path = "repo_crud.rs"]
    pub mod repo_crud;
    #[path = "repo_delete.rs"]
    pub mod repo_delete;
    #[path = "repo_more.rs"]
    pub mod repo_more;
    #[path = "repo_suite.rs"]
    pub mod repo_suite;
    #[path = "repo_update.rs"]
    pub mod repo_update;
}

use database::orm::OrmProvider;
use database::records::{DB, Provider};
use support::repo_suite::Scenario;

#[path = "support/orm.rs"]
#[allow(dead_code)]
mod orm;

async fn run(case: Scenario) {
    let db: DB = OrmProvider::new(orm::sqlite().await)
        .set_database("")
        .await
        .unwrap();
    case(db).await;
}

crate::repository_suite!(run);
crate::repository_filters!(run);
crate::repository_races!(run);
