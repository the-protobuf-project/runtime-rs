//! What a listing costs, counted in statements: runtime-go's `TestOmitTotalHalvesTheQueries`.
//! Go wraps GORM's logger; here a subscriber counts the `sqlx::query` events sqlx emits for
//! every statement. Its own binary, since the subscriber is the process-wide default.

mod support {
    #[path = "orm.rs"]
    pub mod orm;
}

use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};

use database::orm::OrmProvider;
use database::records::{ListOptions, Provider};
use support::orm::{book_res, new_book, sqlite};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

static COUNTS: AtomicU64 = AtomicU64::new(0);
static SELECTS: AtomicU64 = AtomicU64::new(0);

/// Counts the statements sqlx logs, split as Go's `stmtCounter` splits them.
struct Statements;

#[derive(Default)]
struct Sql(String);

impl Visit for Sql {
    fn record_str(&mut self, _: &Field, value: &str) {
        self.0.push_str(value);
    }

    fn record_debug(&mut self, _: &Field, value: &dyn Debug) {
        self.0.push_str(&format!("{value:?}"));
    }
}

impl Subscriber for Statements {
    fn enabled(&self, meta: &Metadata<'_>) -> bool {
        meta.target() == "sqlx::query"
    }

    fn event(&self, event: &Event<'_>) {
        let mut sql = Sql::default();
        event.record(&mut sql);
        let sql = sql.0.to_uppercase();
        if sql.contains("COUNT(") {
            COUNTS.fetch_add(1, Ordering::SeqCst);
        } else if sql.trim_start().starts_with("SELECT") {
            SELECTS.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}

/// Every listing used to cost two queries, one counting every match and one reading the
/// page. The count is the expensive half: `omit_total` skips it.
#[tokio::test]
async fn omit_total_halves_the_queries() {
    tracing::subscriber::set_global_default(Statements).unwrap();
    let db = OrmProvider::new(sqlite().await)
        .set_database("")
        .await
        .unwrap();
    let res = book_res();
    db.schema.ensure_schema(&res).await.unwrap();
    for i in 0..30 {
        let (id, title) = (format!("books/{i:02}"), format!("t{i:02}"));
        db.create(&res, &new_book(&id, &title, 2000, 1))
            .await
            .unwrap();
    }

    COUNTS.store(0, Ordering::SeqCst);
    SELECTS.store(0, Ordering::SeqCst);
    db.list(&res, &ListOptions::default().page_size(10))
        .await
        .unwrap();
    let with_total = COUNTS.load(Ordering::SeqCst);
    assert_eq!(SELECTS.load(Ordering::SeqCst), 1, "one page read");

    COUNTS.store(0, Ordering::SeqCst);
    let opts = ListOptions::default().page_size(10).omit_total(true);
    let out = db.list(&res, &opts).await.unwrap();
    let without_total = COUNTS.load(Ordering::SeqCst);

    assert_eq!(
        with_total, 1,
        "the counted listing ran no count: the probe measures nothing"
    );
    assert_eq!(without_total, 0, "omit_total still counted");
    assert_eq!(out.total, -1);
    assert_eq!(out.items.len(), 10, "the probe row leaked into the page");
    assert_eq!(out.next_page_token, "10");
}
