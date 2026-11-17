//! The repository suite against live PostgreSQL and MySQL, each case in a schema (Postgres)
//! or database (MySQL) of its own, dropped afterwards, with the Go parity cases read back
//! raw. Racing writers exercise the
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

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use database::orm::sqlx::{self, AssertSqlSafe, MySqlPool, PgPool};
use database::orm::{OrmProvider, Pool};
use database::records::{DB, Provider};
use futures::FutureExt;
use support::repo_parity::{Parity, Raw};
use support::repo_suite::Scenario;

fn address(var: &str, default: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| default.to_owned())
}

fn unique() -> String {
    static SEQ: AtomicUsize = AtomicUsize::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("repo_{}_{n}", std::process::id())
}

/// Runs `case` on a fresh schema of `pool`, made and dropped with `exec`.
async fn within<E, X, C, F>(pool: Pool, exec: E, drop: &str, case: C)
where
    E: Fn(String) -> X,
    X: Future<Output = ()>,
    C: FnOnce(DB, String) -> F,
    F: Future<Output = ()>,
{
    let name = unique();
    exec(format!("CREATE SCHEMA {name}")).await;
    let db = OrmProvider::new(pool).set_database(&name).await.unwrap();
    let outcome = AssertUnwindSafe(case(db, name.clone()))
        .catch_unwind()
        .await;
    exec(format!("DROP SCHEMA {name} {drop}")).await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

mod postgres {
    use super::*;

    async fn pool() -> PgPool {
        let at = address("RUNTIME_DATABASE_POSTGRES_ADDRESS", "127.0.0.1:15432");
        PgPool::connect(&format!("postgres://postgres:postgres@{at}/runtime"))
            .await
            .unwrap_or_else(|e| panic!("no Postgres at {at}: {e}"))
    }

    async fn on(
        pg: PgPool,
        case: impl FnOnce(DB, String) -> futures::future::BoxFuture<'static, ()>,
    ) {
        let exec = |sql: String| {
            let pg = pg.clone();
            async move {
                sqlx::query(AssertSqlSafe(sql)).execute(&pg).await.unwrap();
            }
        };
        within(Pool::from(pg.clone()), exec, "CASCADE", case).await;
    }

    pub async fn run(case: Scenario) {
        on(pool().await, |db, _| case(db)).await;
    }

    /// The enum is stored in a Postgres enum type, `ensure_schema` creates it.
    pub async fn parity(case: Parity) {
        let pg = pool().await;
        let reader = pg.clone();
        on(pg, move |db, schema| {
            let raw: Raw = Arc::new(move |table: &str, col: &str, name: &str| {
                let sql = format!("SELECT {col}::text FROM {schema}.{table} WHERE name = $1");
                let (pg, name) = (reader.clone(), name.to_owned());
                async move {
                    let q = sqlx::query_scalar::<_, Option<String>>(AssertSqlSafe(sql)).bind(name);
                    q.fetch_one(&pg).await.unwrap()
                }
                .boxed()
            });
            let schema = db.name.clone();
            case(db, raw, format!("\"{schema}\".\"novel_format\""))
        })
        .await;
    }

    crate::repository_suite!(run);
    crate::repository_filters!(run);
    crate::repository_races!(run);
    crate::repository_parity!(parity);
}

mod mysql {
    use super::*;

    async fn pool() -> MySqlPool {
        let at = address("RUNTIME_DATABASE_MYSQL_ADDRESS", "127.0.0.1:13306");
        MySqlPool::connect(&format!("mysql://root:mysql@{at}/runtime"))
            .await
            .unwrap_or_else(|e| panic!("no MySQL at {at}: {e}"))
    }

    async fn on(
        my: MySqlPool,
        case: impl FnOnce(DB, String) -> futures::future::BoxFuture<'static, ()>,
    ) {
        let exec = |sql: String| {
            let my = my.clone();
            async move {
                sqlx::query(AssertSqlSafe(sql)).execute(&my).await.unwrap();
            }
        };
        within(Pool::from(my.clone()), exec, "", case).await;
    }

    pub async fn run(case: Scenario) {
        on(pool().await, |db, _| case(db)).await;
    }

    pub async fn parity(case: Parity) {
        let my = pool().await;
        let reader = my.clone();
        on(my, move |db, schema| {
            let raw: Raw = Arc::new(move |table: &str, col: &str, name: &str| {
                let sql =
                    format!("SELECT CAST({col} AS CHAR) FROM {schema}.{table} WHERE name = ?");
                let (my, name) = (reader.clone(), name.to_owned());
                async move {
                    let q = sqlx::query_scalar::<_, Option<String>>(AssertSqlSafe(sql)).bind(name);
                    q.fetch_one(&my).await.unwrap()
                }
                .boxed()
            });
            case(db, raw, String::new())
        })
        .await;
    }

    crate::repository_suite!(run);
    crate::repository_filters!(run);
    crate::repository_races!(run);
    crate::repository_parity!(parity);
}
