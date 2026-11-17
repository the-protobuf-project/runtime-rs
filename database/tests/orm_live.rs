//! The SQL backend against live PostgreSQL and MySQL: runtime-go's `orm/dialect_test.go`
//! matrix, and every scenario the in-process SQLite suite runs. SQLite is permissive (any
//! type name, ANSI quotes), so these are what reach the dialect-specific paths.
//!
//! Selected explicitly with `live-tests` rather than marked ignored; once enabled, an engine
//! that is not up is a failure, never a skip. From the repository root:
//!
//! ```text
//! docker compose -p runtime-database-orm-live -f database/docker/compose.orm.live.yaml up -d --wait
//! cargo test -p tpp-database --features orm,live-tests --test orm_live
//! docker compose -p runtime-database-orm-live -f database/docker/compose.orm.live.yaml down
//! ```
//!
//! `RUNTIME_DATABASE_POSTGRES_ADDRESS` defaults to `127.0.0.1:15432` and
//! `RUNTIME_DATABASE_MYSQL_ADDRESS` to `127.0.0.1:13306`; the credentials and the `runtime`
//! database are the Compose file's. Every run makes its own tables and drops them.

mod support {
    #[path = "orm.rs"]
    pub mod orm;
    #[path = "orm_edges.rs"]
    pub mod orm_edges;
    #[path = "orm_scenarios.rs"]
    pub mod orm_scenarios;
    #[path = "orm_tx.rs"]
    pub mod orm_tx;
    #[path = "records.rs"]
    pub mod records;
}

use std::time::Duration;

use database::Error;
use database::orm::sqlx::{self, AssertSqlSafe, MySqlPool, PgPool};
use database::orm::{Dialect, OrmProvider, Pool};
use database::records::{DB, Provider};
use support::orm::new_book;
use support::{orm_edges, orm_scenarios, orm_tx};

fn address(var: &str, default: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| default.to_owned())
}

/// The pool for `dialect`, failing when its engine is not up.
async fn pool(dialect: Dialect) -> Pool {
    match dialect {
        Dialect::Postgres => {
            let at = address("RUNTIME_DATABASE_POSTGRES_ADDRESS", "127.0.0.1:15432");
            PgPool::connect(&format!("postgres://postgres:postgres@{at}/runtime"))
                .await
                .map(Pool::from)
        }
        Dialect::MySql => {
            let at = address("RUNTIME_DATABASE_MYSQL_ADDRESS", "127.0.0.1:13306");
            MySqlPool::connect(&format!("mysql://root:mysql@{at}/runtime"))
                .await
                .map(Pool::from)
        }
        other => panic!("{other:?} is not a live engine"),
    }
    .unwrap_or_else(|e| panic!("no {} engine up: {e}", dialect.name()))
}

async fn open(dialect: Dialect) -> DB {
    OrmProvider::new(pool(dialect).await)
        .set_database("")
        .await
        .unwrap()
}

/// Runs a statement the backend does not issue, e.g. making a tenant's schema.
async fn exec(pool: &Pool, sql: &str) {
    let sql = AssertSqlSafe(sql.to_owned());
    match pool {
        Pool::Postgres(p) => sqlx::raw_sql(sql).execute(p).await.map(|_| ()),
        Pool::MySql(p) => sqlx::raw_sql(sql).execute(p).await.map(|_| ()),
        _ => unreachable!(),
    }
    .unwrap();
}

macro_rules! on_every_engine {
    ($($name:ident => $body:expr;)+) => {
        mod postgres {
            use super::*;
            $(#[tokio::test] async fn $name() { let d = Dialect::Postgres; ($body)(d).await; })+
        }
        mod mysql {
            use super::*;
            $(#[tokio::test] async fn $name() { let d = Dialect::MySql; ($body)(d).await; })+
        }
    };
}

on_every_engine! {
    every_dialect => |d| async move { orm_scenarios::every_dialect(&open(d).await).await };
    every_dialect_transactions => |d| async move { orm_tx::transactions(&open(d).await).await };
    schema_lifecycle => |d| async move { orm_scenarios::schema_lifecycle(&open(d).await).await };
    every_kind => |d| async move { orm_edges::every_kind(&open(d).await, d).await };
    contract_edges => |d| async move { orm_edges::edges(&open(d).await).await };
    omit_total_pages_to_the_end => |d| async move {
        orm_scenarios::omit_total_pages_to_the_end(&open(d).await).await
    };
    selecting_a_database_redirects_every_resource => |d| async move {
        let pool = pool(d).await;
        // A Postgres schema, a MySQL database: the backend selects one but never makes it.
        let make = if d == Dialect::Postgres { "CREATE SCHEMA" } else { "CREATE DATABASE" };
        for t in ["tenant_a", "tenant_b"] {
            exec(&pool, &format!("{make} IF NOT EXISTS {t}")).await;
        }
        let p = OrmProvider::new(pool);
        let (a, b) = (p.set_database("tenant_a").await.unwrap(), p.set_database("tenant_b").await.unwrap());
        orm_scenarios::tenants(&a, &b).await;
    };
    an_unchanged_update_still_finds_its_row => |d| async move {
        // MySQL counts changed rows unless the client asks for found ones; a no-op update
        // must not read as a missing record.
        let db = open(d).await;
        let res = orm_scenarios::table(&db).await;
        let dune = new_book("books/dune", "Dune", 1965, 1);
        db.create(&res, &dune).await.unwrap();
        db.update(&res, &dune).await.expect("an identical update");
        orm_scenarios::drop(&db, &res).await;
    };
    an_unreachable_server_fails_on_selection => |d| async move {
        let url = match d {
            Dialect::Postgres => "postgres://postgres:postgres@127.0.0.1:1/runtime",
            _ => "mysql://root:mysql@127.0.0.1:1/runtime",
        };
        let pool: Pool = match d {
            Dialect::Postgres => sqlx::postgres::PgPoolOptions::new()
                .acquire_timeout(Duration::from_secs(2)).connect_lazy(url).unwrap().into(),
            _ => sqlx::mysql::MySqlPoolOptions::new()
                .acquire_timeout(Duration::from_secs(2)).connect_lazy(url).unwrap().into(),
        };
        let err = OrmProvider::new(pool).set_database("runtime").await.unwrap_err();
        assert!(matches!(&err, Error::Backend { backend: "orm", .. }), "{err}");
        assert!(err.to_string().contains("not reachable"), "{err}");
    };
}
