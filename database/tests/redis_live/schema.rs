//! Schema and transactions: always ready, dropped completely, and transactions refused.

use database::Error;
use database::records::ListOptions;

use crate::support::{book, setup, teardown};

/// Redis has no transactions across these keys, and the contract says so.
#[tokio::test]
async fn transactions_are_refused_by_name() {
    let (db, res) = setup().await;
    let err = db
        .tx
        .run(Box::new(|_| Box::pin(async { Ok(()) })))
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            Error::Unimplemented {
                backend: "redis",
                ..
            }
        ),
        "{err}"
    );
    assert!(err.to_string().contains("redis"), "{err}");
    teardown(&db, &res).await;
}

/// Redis has no schema, so a program that migrates on startup runs unchanged here.
#[tokio::test]
async fn schema_is_always_ready() {
    let (db, res) = setup().await;
    db.schema.ensure_schema(&res).await.unwrap();
    assert!(db.schema.has_schema(&res).await.unwrap());
    teardown(&db, &res).await;
}

#[tokio::test]
async fn drop_schema_removes_everything() {
    let (db, res) = setup().await;
    for i in 0..5 {
        let id = i.to_string();
        db.create(&res, &book(&id, &id)).await.unwrap();
    }
    db.schema.drop_schema(&res).await.unwrap();
    assert_eq!(db.count(&res, &ListOptions::default()).await.unwrap(), 0);
    // The reservations went with the records.
    db.create(&res, &book("0", "0")).await.unwrap();
    teardown(&db, &res).await;
}
