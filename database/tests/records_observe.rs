//! A records database is observed: every call one client span, through the process's
//! default signals when it was handed none, and observing it again replaces rather than
//! stacks.
mod support {
    #[allow(dead_code)]
    #[path = "observability.rs"]
    pub mod observability;
    #[path = "orm.rs"]
    pub mod orm;
}

use database::Error;
use database::orm::OrmProvider;
use database::records::observe::instrument;
use database::records::{DB, Provider};
use support::observability::Harness;
use support::orm::{book_res, new_book, sqlite};
use telemetry::Telemetry;
use telemetry::opentelemetry::trace::Status;

fn outcome(h: &Harness, name: &str) -> Vec<String> {
    h.traces
        .get_finished_spans()
        .unwrap()
        .iter()
        .filter(|s| s.name == name)
        .map(|s| {
            s.attributes
                .iter()
                .find(|kv| kv.key.as_str() == "outcome")
                .map(|kv| kv.value.to_string())
                .unwrap_or_default()
        })
        .collect()
}

#[tokio::test]
async fn a_selected_database_reports_through_the_process_default() {
    let h = Harness::new();
    telemetry::set_process_default(h.signals.clone());
    let db: DB = OrmProvider::new(sqlite().await)
        .set_database("")
        .await
        .unwrap();
    telemetry::set_process_default(Telemetry::default());

    let res = book_res();
    db.schema.ensure_schema(&res).await.unwrap();
    db.create(&res, &new_book("b-1", "Dune", 1965, 1))
        .await
        .unwrap();
    assert!(matches!(
        db.get(&res, "missing").await,
        Err(Error::NotFound)
    ));

    assert_eq!(outcome(&h, "database.create"), ["ok"]);
    assert_eq!(outcome(&h, "database.get"), ["not_found"]);
    let spans = h.traces.get_finished_spans().unwrap();
    let get = spans.iter().find(|s| s.name == "database.get").unwrap();
    assert!(
        !matches!(get.status, Status::Error { .. }),
        "a miss failed its span"
    );
    assert!(spans.iter().any(|s| s.name == "database.ensure_schema"));
}

#[tokio::test]
async fn observing_again_replaces_rather_than_stacks() {
    let (first, second) = (Harness::new(), Harness::new());
    let db: DB = OrmProvider::new(sqlite().await)
        .set_database("")
        .await
        .unwrap();
    let db = instrument(
        instrument(db, first.signals.clone()),
        second.signals.clone(),
    );

    let res = book_res();
    db.schema.ensure_schema(&res).await.unwrap();
    db.create(&res, &new_book("b-2", "Emma", 1815, 1))
        .await
        .unwrap();

    assert!(first.traces.get_finished_spans().unwrap().is_empty());
    assert_eq!(outcome(&second, "database.create"), ["ok"]);
    assert_eq!(outcome(&second, "database.ensure_schema"), ["ok"]);
}
