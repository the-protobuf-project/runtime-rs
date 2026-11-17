//! Transactions on every engine: commit, rollback with the caller's error, a nested
//! transaction on a savepoint, and a transaction's database used after it ended. Ported from
//! runtime-go's `orm/dialect_test.go` and `orm/provider_test.go`.

#![allow(dead_code)]

use database::Error;
use database::records::{DB, ListOptions};
use futures::FutureExt;

use super::orm::{new_book, work};
use super::orm_scenarios::{drop, table};

/// Go's `TestEveryDialectTransactions`, `TestTransactionCommits` and
/// `TestTransactionRollsBack`, plus a nested transaction on a savepoint and a transaction's
/// database used after it ended.
pub async fn transactions(db: &DB) {
    let res = table(db).await;
    let r = res.clone();
    db.tx
        .run(work(move |tx| {
            async move {
                tx.create(&r, &new_book("books/a", "A", 2000, 1)).await?;
                tx.create(&r, &new_book("books/b", "B", 2001, 1)).await?;
                Ok(())
            }
            .boxed()
        }))
        .await
        .expect("commit");
    assert_eq!(db.count(&res, &ListOptions::default()).await.unwrap(), 2);

    let r = res.clone();
    let err = db
        .tx
        .run(work(move |tx| {
            async move {
                tx.create(&r, &new_book("books/c", "C", 2002, 1)).await?;
                Err(Error::Invalid("rolled back".into()))
            }
            .boxed()
        }))
        .await;
    assert!(
        matches!(&err, Err(Error::Invalid(m)) if m == "rolled back"),
        "the caller's error: {err:?}"
    );
    assert_eq!(
        db.count(&res, &ListOptions::default()).await.unwrap(),
        2,
        "one rollback"
    );

    // A failing inner transaction undoes only its own writes, back to its savepoint.
    let (r, leaked) = (
        res.clone(),
        std::sync::Arc::new(std::sync::Mutex::new(None)),
    );
    let keep = leaked.clone();
    db.tx
        .run(work(move |tx| {
            async move {
                tx.create(&r, &new_book("books/d", "D", 2003, 1)).await?;
                let inner = r.clone();
                let undone = tx
                    .tx
                    .run(work(move |tx| {
                        async move {
                            tx.create(&inner, &new_book("books/e", "E", 2004, 1))
                                .await?;
                            Err(Error::Invalid("inner".into()))
                        }
                        .boxed()
                    }))
                    .await;
                assert!(undone.is_err());
                *keep.lock().unwrap() = Some(tx.clone());
                Ok(())
            }
            .boxed()
        }))
        .await
        .expect("outer commit");
    assert!(db.exists(&res, "books/d").await.unwrap());
    assert!(
        !db.exists(&res, "books/e").await.unwrap(),
        "the savepoint rolled back"
    );
    let stale = leaked.lock().unwrap().take().unwrap();
    let after = stale.get(&res, "books/d").await;
    assert!(
        matches!(after, Err(Error::Invalid(_))),
        "a finished transaction: {after:?}"
    );
    drop(db, &res).await;
}
