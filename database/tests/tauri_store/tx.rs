//! Transactions and bulk writes: all or nothing.

use std::sync::Arc;

use database::Error;

use super::{db, on_disk, provider, support};

#[tokio::test]
async fn a_transaction_commits_whole_or_not_at_all() {
    let (_app, p, path) = provider();
    let (db, res) = (db(&p).await, Arc::new(support::books()));
    let r = res.clone();
    db.tx
        .run(Box::new(move |tx| {
            Box::pin(async move {
                tx.create(&r, &support::book("dune", "Dune", 412)).await?;
                tx.create(&r, &support::book("emma", "Emma", 300))
                    .await
                    .map(|_| ())
            })
        }))
        .await
        .unwrap();
    assert!(on_disk(&path).contains_key("books:rec:books/emma"));
    let r = res.clone();
    let failed = db
        .tx
        .run(Box::new(move |tx| {
            Box::pin(async move {
                tx.delete(&r, "books/dune").await?;
                tx.create(&r, &support::book("copy", "Emma", 1))
                    .await
                    .map(|_| ())
            })
        }))
        .await;
    assert!(matches!(failed, Err(Error::AlreadyExists)), "{failed:?}");
    assert!(
        db.exists(&res, "books/dune").await.unwrap(),
        "the delete was rolled back"
    );
    let nested = db
        .tx
        .run(Box::new(|tx| {
            Box::pin(async move { tx.tx.run(Box::new(|_| Box::pin(async { Ok(()) }))).await })
        }))
        .await;
    assert!(
        matches!(nested, Err(Error::Unimplemented { .. })),
        "{nested:?}"
    );
}

#[tokio::test]
async fn a_bulk_create_is_all_or_nothing() {
    let (_app, p, _) = provider();
    let (db, res) = (db(&p).await, support::books());
    let batch = [support::book("a", "A", 1), support::book("b", "A", 2)];
    assert!(matches!(
        db.batch.create_many(&res, &batch).await,
        Err(Error::AlreadyExists)
    ));
    assert!(
        !db.exists(&res, "books/a").await.unwrap(),
        "the first was undone"
    );
    db.batch
        .create_many(
            &res,
            &[support::book("a", "A", 1), support::book("b", "B", 2)],
        )
        .await
        .unwrap();
    let got = db
        .batch
        .get_many(&res, &["books/b".into(), "books/x".into()])
        .await
        .unwrap();
    assert!(got[0].is_some() && got[1].is_none());
}
