//! The repository suite against live PostgreSQL and MySQL, each case in a schema (Postgres)
//! or database (MySQL) of its own, dropped afterwards. Racing writers exercise the
//! `SELECT ... FOR UPDATE` the etag check reads through.
//!
//! ```text
//! docker compose -p runtime-database-repository-sql -f database/docker/compose.orm.live.yaml up -d --wait
//! cargo test -p tpp-database --features orm,live-tests --test repository_sql
//! docker compose -p runtime-database-repository-sql -f database/docker/compose.orm.live.yaml down
//! ```
//!
//! `RUNTIME_DATABASE_POSTGRES_ADDRESS` defaults to `127.0.0.1:15432` and
//! `RUNTIME_DATABASE_MYSQL_ADDRESS` to `127.0.0.1:13306`.

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

use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicUsize, Ordering};

use database::orm::sqlx::{self, AssertSqlSafe, MySqlPool, PgPool};
use database::orm::{OrmProvider, Pool};
use database::records::Provider;
use futures::FutureExt;
use support::repo_suite::Scenario;

fn address(var: &str, default: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| default.to_owned())
}

fn unique() -> String {
    static SEQ: AtomicUsize = AtomicUsize::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("repo_{}_{n}", std::process::id())
}

/// Runs `case` in a fresh schema of `pool`, made by `make` and dropped by `drop`.
async fn within(
    pool: Pool,
    exec: impl Fn(String) -> futures::future::BoxFuture<'static, ()>,
    case: Scenario,
    drop: &str,
) {
    let name = unique();
    exec(format!("CREATE SCHEMA {name}")).await;
    let db = OrmProvider::new(pool).set_database(&name).await.unwrap();
    let outcome = AssertUnwindSafe(case(db)).catch_unwind().await;
    exec(format!("DROP SCHEMA {name} {drop}")).await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

mod postgres {
    use super::*;

    pub async fn run(case: Scenario) {
        let at = address("RUNTIME_DATABASE_POSTGRES_ADDRESS", "127.0.0.1:15432");
        let pg = PgPool::connect(&format!("postgres://postgres:postgres@{at}/runtime"))
            .await
            .unwrap_or_else(|e| panic!("no Postgres at {at}: {e}"));
        let exec = {
            let pg = pg.clone();
            move |sql: String| {
                let pg = pg.clone();
                async move {
                    sqlx::query(AssertSqlSafe(sql)).execute(&pg).await.unwrap();
                }
                .boxed()
            }
        };
        within(Pool::from(pg), exec, case, "CASCADE").await;
    }

    crate::repository_suite!(run);
    crate::repository_filters!(run);
    crate::repository_races!(run);
}

mod mysql {
    use super::*;

    pub async fn run(case: Scenario) {
        let at = address("RUNTIME_DATABASE_MYSQL_ADDRESS", "127.0.0.1:13306");
        let my = MySqlPool::connect(&format!("mysql://root:mysql@{at}/runtime"))
            .await
            .unwrap_or_else(|e| panic!("no MySQL at {at}: {e}"));
        let exec = {
            let my = my.clone();
            move |sql: String| {
                let my = my.clone();
                async move {
                    sqlx::query(AssertSqlSafe(sql)).execute(&my).await.unwrap();
                }
                .boxed()
            }
        };
        within(Pool::from(my), exec, case, "").await;
    }

    crate::repository_suite!(run);
    crate::repository_filters!(run);
    crate::repository_races!(run);
}
