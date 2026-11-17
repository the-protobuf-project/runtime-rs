//! The SQLite leg of runtime-go's `orm/dialect_test.go` matrix, in-process: every scenario
//! `orm_live` runs against Postgres and MySQL, on a database the provider selects. This is
//! where Go's `TestTransactionCommits` and `TestTransactionRollsBack` run for SQLite.

mod support {
    #[path = "filter_conformance.rs"]
    pub mod filter_conformance;
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

use database::orm::{Dialect, OrmProvider};
use database::records::{DB, Provider};
use support::orm::sqlite;
use support::{orm_edges, orm_scenarios, orm_tx};

async fn open() -> DB {
    OrmProvider::new(sqlite().await)
        .set_database("")
        .await
        .unwrap()
}

#[tokio::test]
async fn every_dialect() {
    orm_scenarios::every_dialect(&open().await).await;
}

#[tokio::test]
async fn every_dialect_transactions() {
    orm_tx::transactions(&open().await).await;
}

#[tokio::test]
async fn schema_lifecycle() {
    orm_scenarios::schema_lifecycle(&open().await).await;
}

#[tokio::test]
async fn every_kind() {
    orm_edges::every_kind(&open().await, Dialect::Sqlite).await;
}

#[tokio::test]
async fn contract_edges() {
    orm_edges::edges(&open().await).await;
}

#[tokio::test]
async fn omit_total_pages_to_the_end() {
    orm_scenarios::omit_total_pages_to_the_end(&open().await).await;
}

#[tokio::test]
async fn aip160_conformance() {
    orm_edges::conformance(&open().await, Dialect::Sqlite).await;
}
